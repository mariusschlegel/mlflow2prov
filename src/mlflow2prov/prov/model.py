import logging
from dataclasses import dataclass, field
from typing import Any, Iterable, Type

import prov.model

from mlflow2prov.adapters.repository import InMemoryRepository
from mlflow2prov.domain.constants import ChangeType, ProvRole
from mlflow2prov.domain.model import (
    Commit,
    Experiment,
    FileRevision,
    LifecycleStage,
    RegisteredModel,
    RegisteredModelVersion,
    RegisteredModelVersionStage,
    Run,
)

log = logging.getLogger(__name__)

DEFAULT_NAMESPACE = prov.model.Namespace("ex", "example.org")


@dataclass
class ProvContext:
    document: prov.model.ProvDocument
    namespace: str | None = None

    def add_element(
        self,
        dataclass_instance,
    ) -> prov.model.ProvRecord:
        element = self.convert_to_prov_element(dataclass_instance)

        # Add the namespace to the element if it is provided, but
        # namespace can only be set for ProvBundles and ProvDocuments
        # if self.namespace:
        #     element.add_namespace(self.namespace)

        return self.document.add_record(element)

    def convert_to_prov_element(
        self,
        dataclass_instance,
    ) -> prov.model.ProvElement:
        element = dataclass_instance.to_prov()

        return self.document.new_record(
            element._prov_type, element.identifier, element.attributes
        )

    def add_relation(
        self,
        source_dataclass_instance,
        target_dataclass_instance,
        relationship_type: Type[prov.model.ProvRelation],
        attributes: dict[str, Any] | None = None,
    ) -> None:
        if not attributes:
            attributes = dict()

        source = self.add_element(source_dataclass_instance)
        target = self.add_element(target_dataclass_instance)

        relationship = self.document.new_record(
            relationship_type._prov_type,
            prov.model.QualifiedName(
                DEFAULT_NAMESPACE, f"relation:{source.identifier}:{target.identifier}"
            ),
            {
                relationship_type.FORMAL_ATTRIBUTES[0]: source,  # type: ignore
                relationship_type.FORMAL_ATTRIBUTES[1]: target,  # type: ignore
            },
        )

        relationship.add_attributes(attributes)

        self.document.add_record(relationship)


@dataclass
class FileAdditionModel:
    commit: Commit
    parent: Commit | None
    revision: FileRevision
    context: ProvContext = field(init=False)

    def __post_init__(self):
        self.context = ProvContext(prov.model.ProvDocument())

    @staticmethod
    def query(
        git_repository: InMemoryRepository,
        mlflow_repository: InMemoryRepository,
    ) -> Iterable[tuple[Commit, Commit | None, FileRevision]]:
        for revision in git_repository.list_all(
            resource_type=FileRevision, status=ChangeType.ADDED
        ):
            commit = git_repository.get(resource_type=Commit, sha=revision.commit)
            if commit:
                for parent in [
                    git_repository.get(resource_type=Commit, sha=sha)
                    for sha in commit.parents
                ]:
                    yield commit, parent, revision

    def build_prov_model(self) -> prov.model.ProvDocument:
        self.context.add_element(self.commit)
        self.context.add_element(self.commit.author)
        self.context.add_element(self.commit.committer)
        self.context.add_element(self.revision)
        self.context.add_element(self.revision.file)

        if self.parent:
            self.context.add_element(self.parent)

            self.context.add_relation(
                source_dataclass_instance=self.commit,
                target_dataclass_instance=self.parent,
                relationship_type=prov.model.ProvCommunication,
            )

        self.context.add_relation(
            self.commit,
            self.commit.author,
            prov.model.ProvAssociation,
            {str(prov.model.PROV_ROLE): ProvRole.COMMIT_AUTHOR},
        )
        self.context.add_relation(
            self.commit,
            self.commit.committer,
            prov.model.ProvAssociation,
            {str(prov.model.PROV_ROLE): ProvRole.COMMITTER},
        )
        self.context.add_relation(
            self.revision,
            self.commit,
            prov.model.ProvGeneration,
            {
                str(prov.model.PROV_ATTR_STARTTIME): self.commit.authored_at,
                str(prov.model.PROV_ROLE): ProvRole.FILE,
            },
        )
        self.context.add_relation(
            self.revision.file,
            self.commit,
            prov.model.ProvGeneration,
            {
                str(prov.model.PROV_ATTR_STARTTIME): self.commit.authored_at,
                str(prov.model.PROV_ROLE): ProvRole.ADDED_REVISION,
            },
        )
        self.context.add_relation(
            self.revision.file, self.commit.author, prov.model.ProvAttribution
        )
        self.context.add_relation(
            self.revision, self.revision.file, prov.model.ProvSpecialization
        )

        return self.context.document


@dataclass
class FileModificationModel:
    commit: Commit
    parent: Commit | None
    revision: FileRevision
    previous: FileRevision | None
    context: ProvContext = field(init=False)

    def __post_init__(self):
        self.context = ProvContext(prov.model.ProvDocument())

    @staticmethod
    def query(
        git_repository: InMemoryRepository,
        mlflow_repository: InMemoryRepository,
    ) -> Iterable[tuple[Commit, Commit | None, FileRevision, FileRevision | None]]:
        for revision in git_repository.list_all(
            resource_type=FileRevision, status=ChangeType.MODIFIED
        ):
            commit = git_repository.get(resource_type=Commit, sha=revision.commit)
            if commit:
                for parent in [
                    git_repository.get(resource_type=Commit, sha=sha)
                    for sha in commit.parents
                ]:
                    yield commit, parent, revision, revision.previous

    def build_prov_model(self) -> prov.model.ProvDocument:
        self.context.add_element(self.commit)
        self.context.add_element(self.revision)
        self.context.add_element(self.revision.file)
        self.context.add_element(self.previous)
        self.context.add_element(self.commit.author)
        self.context.add_element(self.commit.committer)

        if self.parent:
            self.context.add_element(self.parent)

            self.context.add_relation(
                self.commit, self.parent, prov.model.ProvCommunication
            )

        self.context.add_relation(
            self.commit,
            self.commit.author,
            prov.model.ProvAssociation,
            {str(prov.model.PROV_ROLE): ProvRole.COMMIT_AUTHOR},
        )
        self.context.add_relation(
            self.commit,
            self.commit.committer,
            prov.model.ProvAssociation,
            {str(prov.model.PROV_ROLE): ProvRole.COMMITTER},
        )
        self.context.add_relation(
            self.revision, self.revision.file, prov.model.ProvSpecialization
        )
        self.context.add_relation(
            self.revision,
            self.commit,
            prov.model.ProvGeneration,
            {
                str(prov.model.PROV_ATTR_STARTTIME): self.commit.authored_at,
                str(prov.model.PROV_ROLE): ProvRole.MODIFIED_REVISION,
            },
        )
        self.context.add_relation(
            self.revision, self.commit.author, prov.model.ProvAttribution
        )
        self.context.add_relation(
            self.revision,
            self.previous,
            prov.model.ProvDerivation,
            {str(prov.model.PROV_TYPE): "prov:Revision"},
        )
        self.context.add_relation(
            self.commit,
            self.previous,
            prov.model.ProvUsage,
            {
                str(prov.model.PROV_ATTR_STARTTIME): self.commit.authored_at,
                str(prov.model.PROV_ROLE): ProvRole.PREVIOUS_REVISION,
            },
        )

        return self.context.document


@dataclass
class FileDeletionModel:
    commit: Commit
    parent: Commit | None
    revision: FileRevision
    context: ProvContext = field(init=False)

    def __post_init__(self):
        self.context = ProvContext(prov.model.ProvDocument())

    @staticmethod
    def query(
        git_repository: InMemoryRepository,
        mlflow_repository: InMemoryRepository,
    ) -> Iterable[tuple[Commit, Commit | None, FileRevision]]:
        for revision in git_repository.list_all(
            resource_type=FileRevision, status=ChangeType.DELETED
        ):
            commit = git_repository.get(resource_type=Commit, sha=revision.commit)
            if commit:
                for parent in [
                    git_repository.get(resource_type=Commit, sha=sha)
                    for sha in commit.parents
                ]:
                    yield commit, parent, revision

    def build_prov_model(self) -> prov.model.ProvDocument:
        self.context.add_element(self.commit)
        self.context.add_element(self.revision)
        self.context.add_element(self.revision.file)
        self.context.add_element(self.commit.author)
        self.context.add_element(self.commit.committer)

        if self.parent:
            self.context.add_element(self.parent)

            self.context.add_relation(
                self.commit, self.parent, prov.model.ProvCommunication
            )

        self.context.add_relation(
            self.commit,
            self.commit.committer,
            prov.model.ProvAssociation,
            {str(prov.model.PROV_ROLE): ProvRole.COMMITTER},
        )
        self.context.add_relation(
            self.commit,
            self.commit.author,
            prov.model.ProvAssociation,
            {str(prov.model.PROV_ROLE): ProvRole.COMMIT_AUTHOR},
        )
        self.context.add_relation(
            self.revision, self.revision.file, prov.model.ProvSpecialization
        )
        self.context.add_relation(
            self.revision,
            self.commit,
            prov.model.ProvInvalidation,
            {
                str(prov.model.PROV_ATTR_STARTTIME): self.commit.authored_at,
                str(prov.model.PROV_ROLE): ProvRole.DELETED_REVISION,
            },
        )

        return self.context.document


@dataclass
class ExperimentAdditionModel:
    experiment: Experiment
    context: ProvContext = field(init=False)

    def __post_init__(self):
        self.context = ProvContext(document=prov.model.ProvDocument())

    @staticmethod
    def query(
        git_repository: InMemoryRepository,
        mlflow_repository: InMemoryRepository,
    ) -> Iterable[tuple[Experiment]]:
        for experiment in mlflow_repository.list_all(resource_type=Experiment):
            yield (experiment,)

    def build_prov_model(self) -> prov.model.ProvDocument:
        self.context.add_element(self.experiment)
        self.context.add_element(self.experiment.creation)

        self.context.add_relation(
            self.experiment,
            self.experiment.creation,
            prov.model.ProvGeneration,
            {
                str(
                    prov.model.PROV_ATTR_STARTTIME
                ): self.experiment.creation.start_time,
                str(prov.model.PROV_ROLE): ProvRole.ADDED_EXPERIMENT,
            },
        )

        if self.experiment.tags:
            for tag in self.experiment.tags:
                self.context.add_element(tag)

                self.context.add_relation(
                    tag,
                    self.experiment.creation,
                    prov.model.ProvGeneration,
                    {
                        str(
                            prov.model.PROV_ATTR_STARTTIME
                        ): self.experiment.creation.start_time,
                        str(prov.model.PROV_ROLE): ProvRole.ADDED_EXPERIMENT_TAG,
                    },
                )
                if self.experiment.user:
                    self.context.add_relation(
                        tag, self.experiment.user, prov.model.ProvAttribution
                    )
                self.context.add_relation(
                    self.experiment, tag, prov.model.ProvMembership
                )

        if self.experiment.user:
            self.context.add_element(self.experiment.user)

            self.context.add_relation(
                self.experiment,
                self.experiment.user,
                prov.model.ProvAttribution,
            )
            self.context.add_relation(
                self.experiment.creation,
                self.experiment.user,
                prov.model.ProvAssociation,
                {str(prov.model.PROV_ROLE): ProvRole.EXPERIMENT_AUTHOR},
            )

        return self.context.document


@dataclass
class ExperimentDeletionModel:
    experiment: Experiment
    run: Run | None
    context: ProvContext = field(init=False)

    def __post_init__(self):
        self.context = ProvContext(prov.model.ProvDocument())

    @staticmethod
    def query(
        git_repository: InMemoryRepository,
        mlflow_repository: InMemoryRepository,
    ) -> Iterable[tuple[Experiment, Run | None]]:
        for experiment in mlflow_repository.list_all(
            resource_type=Experiment, lifecycle_stage=LifecycleStage.DELETED
        ):
            runs = mlflow_repository.list_all(
                resource_type=Run, experiment_id=experiment.experiment_id
            )
            for run in [*runs, None]:
                yield experiment, run

    def build_prov_model(self) -> prov.model.ProvDocument:
        self.context.add_element(self.experiment)
        if self.experiment.tags:
            for experiment_tag in self.experiment.tags:
                self.context.add_element(experiment_tag)
        if self.run:
            self.context.add_element(self.run)
            if self.run.metrics:
                for metric in self.run.metrics:
                    self.context.add_element(metric)
            if self.run.params:
                for param in self.run.params:
                    self.context.add_element(param)
            if self.run.tags:
                for run_tag in self.run.tags:
                    self.context.add_element(run_tag)
            if self.run.artifacts:
                for artifact in self.run.artifacts:
                    self.context.add_element(artifact)
            if self.run.model_artifacts:
                for model_artifact in self.run.model_artifacts:
                    self.context.add_element(model_artifact)
        self.context.add_element(self.experiment.user)
        self.context.add_element(self.experiment.deletion)

        self.context.add_relation(
            self.experiment.deletion, self.experiment.user, prov.model.ProvAssociation
        )
        self.context.add_relation(
            self.experiment,
            self.experiment.deletion,
            prov.model.ProvInvalidation,
            {
                str(
                    prov.model.PROV_ATTR_STARTTIME
                ): self.experiment.deletion.start_time,
                str(prov.model.PROV_ROLE): ProvRole.DELETED_EXPERIMENT,
            },
        )
        if self.experiment.tags:
            for experiment_tag in self.experiment.tags:
                self.context.add_relation(
                    self.experiment, experiment_tag, prov.model.ProvMembership
                )
                self.context.add_relation(
                    experiment_tag,
                    self.experiment.deletion,
                    prov.model.ProvInvalidation,
                    {
                        str(
                            prov.model.PROV_ATTR_STARTTIME
                        ): self.experiment.deletion.start_time,
                        str(prov.model.PROV_ROLE): ProvRole.DELETED_EXPERIMENT,
                    },
                )
        if self.run:
            self.context.add_relation(
                self.run,
                self.run.deletion,
                prov.model.ProvInvalidation,
                {
                    str(
                        prov.model.PROV_ATTR_STARTTIME
                    ): self.experiment.deletion.start_time,
                    str(prov.model.PROV_ROLE): ProvRole.DELETED_RUN,
                },
            )
            if self.run.metrics:
                for metric in self.run.metrics:
                    self.context.add_relation(
                        self.run, metric, prov.model.ProvMembership
                    )
                    self.context.add_relation(
                        metric,
                        self.run.deletion,
                        prov.model.ProvInvalidation,
                        {
                            str(
                                prov.model.PROV_ATTR_STARTTIME
                            ): self.experiment.deletion.start_time,
                            str(prov.model.PROV_ROLE): ProvRole.DELETED_RUN,
                        },
                    )
            if self.run.params:
                for param in self.run.params:
                    self.context.add_relation(
                        self.run, param, prov.model.ProvMembership
                    )
                    self.context.add_relation(
                        param,
                        self.run.deletion,
                        prov.model.ProvInvalidation,
                        {
                            str(
                                prov.model.PROV_ATTR_STARTTIME
                            ): self.experiment.deletion.start_time,
                            str(prov.model.PROV_ROLE): ProvRole.DELETED_RUN,
                        },
                    )
            if self.run.tags:
                for tag in self.run.tags:
                    self.context.add_relation(self.run, tag, prov.model.ProvMembership)
                    self.context.add_relation(
                        tag,
                        self.run.deletion,
                        prov.model.ProvInvalidation,
                        {
                            str(
                                prov.model.PROV_ATTR_STARTTIME
                            ): self.experiment.deletion.start_time,
                            str(prov.model.PROV_ROLE): ProvRole.DELETED_RUN,
                        },
                    )
            if self.run.artifacts:
                for artifact in self.run.artifacts:
                    self.context.add_relation(
                        self.run, artifact, prov.model.ProvMembership
                    )
                    self.context.add_relation(
                        artifact,
                        self.run.deletion,
                        prov.model.ProvInvalidation,
                        {
                            str(
                                prov.model.PROV_ATTR_STARTTIME
                            ): self.experiment.deletion.start_time,
                            str(prov.model.PROV_ROLE): ProvRole.DELETED_RUN,
                        },
                    )
            if self.run.model_artifacts:
                for model_artifact in self.run.model_artifacts:
                    self.context.add_relation(
                        self.run, model_artifact, prov.model.ProvMembership
                    )
                    self.context.add_relation(
                        model_artifact,
                        self.run.deletion,
                        prov.model.ProvInvalidation,
                        {
                            str(
                                prov.model.PROV_ATTR_STARTTIME
                            ): self.experiment.deletion.start_time,
                            str(prov.model.PROV_ROLE): ProvRole.DELETED_RUN,
                        },
                    )
                    if model_artifact.artifact:
                        self.context.add_relation(
                            model_artifact,
                            model_artifact.artifact,
                            prov.model.ProvSpecialization,
                        )

        return self.context.document


@dataclass
class RunAdditionModel:
    run: Run
    experiment: Experiment | None
    commit: Commit | None
    file_revision: FileRevision | None
    context: ProvContext = field(init=False)

    def __post_init__(self):
        self.context = ProvContext(prov.model.ProvDocument())

    @staticmethod
    def query(
        git_repository: InMemoryRepository,
        mlflow_repository: InMemoryRepository,
    ) -> Iterable[tuple[Run, Experiment | None, Commit | None, FileRevision | None]]:
        for run in mlflow_repository.list_all(resource_type=Run):
            experiment = mlflow_repository.get(
                resource_type=Experiment, experiment_id=run.experiment_id
            )
            commit = git_repository.get(resource_type=Commit, sha=run.source_git_commit)
            file_revision = git_repository.get(
                resource_type=FileRevision, name=run.source_name
            )
            yield run, experiment, commit, file_revision

    def build_prov_model(self) -> prov.model.ProvDocument:
        self.context.add_element(self.run)
        self.context.add_element(self.experiment)
        if self.run.metrics:
            for metric in self.run.metrics:
                self.context.add_element(metric)
        if self.run.params:
            for param in self.run.params:
                self.context.add_element(param)
        if self.run.tags:
            for tag in self.run.tags:
                self.context.add_element(tag)
        if self.run.artifacts:
            for artifact in self.run.artifacts:
                self.context.add_element(artifact)
        if self.run.model_artifacts:
            for model_artifact in self.run.model_artifacts:
                self.context.add_element(model_artifact)
        if self.run.user:
            self.context.add_element(self.run.user)
        self.context.add_element(self.run.creation)
        if self.commit:
            self.context.add_element(self.commit)
        if self.file_revision:
            self.context.add_element(self.file_revision)

        self.context.add_relation(self.experiment, self.run, prov.model.ProvMembership)
        self.context.add_relation(
            self.run,
            self.run.creation,
            prov.model.ProvGeneration,
            {
                str(prov.model.PROV_ATTR_STARTTIME): self.run.creation.start_time,
                str(prov.model.PROV_ROLE): ProvRole.ADDED_RUN,
            },
        )
        self.context.add_relation(self.run, self.run.user, prov.model.ProvAttribution)
        if self.run.metrics:
            for metric in self.run.metrics:
                self.context.add_relation(self.run, metric, prov.model.ProvMembership)
                self.context.add_relation(
                    metric,
                    self.run.creation,
                    prov.model.ProvGeneration,
                    {
                        str(
                            prov.model.PROV_ATTR_STARTTIME
                        ): self.run.creation.start_time,
                        str(prov.model.PROV_ROLE): ProvRole.ADDED_RUN,
                    },
                )
                self.context.add_relation(
                    metric, self.run.user, prov.model.ProvAttribution
                )
        if self.run.params:
            for param in self.run.params:
                self.context.add_relation(self.run, param, prov.model.ProvMembership)
                self.context.add_relation(
                    param,
                    self.run.creation,
                    prov.model.ProvGeneration,
                    {
                        str(
                            prov.model.PROV_ATTR_STARTTIME
                        ): self.run.creation.start_time,
                        str(prov.model.PROV_ROLE): ProvRole.ADDED_RUN,
                    },
                )
                self.context.add_relation(
                    param, self.run.user, prov.model.ProvAttribution
                )
        if self.run.tags:
            for tag in self.run.tags:
                self.context.add_relation(self.run, tag, prov.model.ProvMembership)
                self.context.add_relation(
                    tag,
                    self.run.creation,
                    prov.model.ProvGeneration,
                    {
                        str(
                            prov.model.PROV_ATTR_STARTTIME
                        ): self.run.creation.start_time,
                        str(prov.model.PROV_ROLE): ProvRole.ADDED_RUN,
                    },
                )
                self.context.add_relation(
                    tag, self.run.user, prov.model.ProvAttribution
                )
        if self.run.artifacts:
            for artifact in self.run.artifacts:
                self.context.add_relation(self.run, artifact, prov.model.ProvMembership)
                self.context.add_relation(
                    artifact,
                    self.run.creation,
                    prov.model.ProvGeneration,
                    {
                        str(
                            prov.model.PROV_ATTR_STARTTIME
                        ): self.run.creation.start_time,
                        str(prov.model.PROV_ROLE): ProvRole.ADDED_RUN,
                    },
                )
                self.context.add_relation(
                    artifact, self.run.user, prov.model.ProvAttribution
                )
        if self.run.model_artifacts:
            for model_artifact in self.run.model_artifacts:
                self.context.add_relation(
                    self.run, model_artifact, prov.model.ProvMembership
                )
                self.context.add_relation(
                    model_artifact,
                    self.run.creation,
                    prov.model.ProvGeneration,
                    {
                        str(
                            prov.model.PROV_ATTR_STARTTIME
                        ): self.run.creation.start_time,
                        str(prov.model.PROV_ROLE): ProvRole.ADDED_RUN,
                    },
                )
                self.context.add_relation(
                    model_artifact, self.run.user, prov.model.ProvAttribution
                )
                if model_artifact.artifact != None:
                    self.context.add_relation(
                        model_artifact,
                        model_artifact.artifact,
                        prov.model.ProvSpecialization,
                    )
        if self.commit:
            self.context.add_relation(
                self.run.creation, self.commit, prov.model.ProvCommunication
            )
        if self.file_revision:
            self.context.add_relation(
                self.run.creation,
                self.file_revision,
                prov.model.ProvUsage,
                {
                    str(prov.model.PROV_ATTR_STARTTIME): self.run.creation.start_time,
                    str(prov.model.PROV_ROLE): ProvRole.PREVIOUS_REVISION,
                },
            )

        return self.context.document


@dataclass
class RunDeletionModel:
    run: Run
    context: ProvContext = field(init=False)

    def __post_init__(self):
        self.context = ProvContext(prov.model.ProvDocument())

    @staticmethod
    def query(
        git_repository: InMemoryRepository,
        mlflow_repository: InMemoryRepository,
    ) -> Iterable[tuple[Run]]:
        for run in mlflow_repository.list_all(
            resource_type=Run, lifecycle_stage=LifecycleStage.DELETED
        ):
            yield (run,)

    def build_prov_model(self) -> prov.model.ProvDocument:
        if self.run.deletion:
            self.context.add_element(self.run)
            if self.run.metrics:
                for metric in self.run.metrics:
                    self.context.add_element(metric)
            if self.run.params:
                for param in self.run.params:
                    self.context.add_element(param)
            if self.run.tags:
                for tag in self.run.tags:
                    self.context.add_element(tag)
            if self.run.artifacts:
                for artifact in self.run.artifacts:
                    self.context.add_element(artifact)
            if self.run.model_artifacts:
                for model_artifact in self.run.model_artifacts:
                    self.context.add_element(model_artifact)
            self.context.add_element(self.run.user)
            self.context.add_element(self.run.deletion)

            self.context.add_relation(
                self.run.deletion, self.run.user, prov.model.ProvAssociation
            )
            self.context.add_relation(
                self.run,
                self.run.deletion,
                prov.model.ProvInvalidation,
                {
                    str(prov.model.PROV_ATTR_STARTTIME): self.run.deletion.start_time,
                    str(prov.model.PROV_ROLE): ProvRole.DELETED_RUN,
                },
            )
            if self.run.metrics:
                for metric in self.run.metrics:
                    self.context.add_relation(
                        self.run, metric, prov.model.ProvMembership
                    )
                    self.context.add_relation(
                        metric,
                        self.run.deletion,
                        prov.model.ProvInvalidation,
                        {
                            str(
                                prov.model.PROV_ATTR_STARTTIME
                            ): self.run.deletion.start_time,
                            str(prov.model.PROV_ROLE): ProvRole.DELETED_RUN,
                        },
                    )
            if self.run.params:
                for param in self.run.params:
                    self.context.add_relation(
                        self.run, param, prov.model.ProvMembership
                    )
                    self.context.add_relation(
                        param,
                        self.run.deletion,
                        prov.model.ProvInvalidation,
                        {
                            str(
                                prov.model.PROV_ATTR_STARTTIME
                            ): self.run.deletion.start_time,
                            str(prov.model.PROV_ROLE): ProvRole.DELETED_RUN,
                        },
                    )
            if self.run.tags:
                for tag in self.run.tags:
                    self.context.add_relation(self.run, tag, prov.model.ProvMembership)
                    self.context.add_relation(
                        tag,
                        self.run.deletion,
                        prov.model.ProvInvalidation,
                        {
                            str(
                                prov.model.PROV_ATTR_STARTTIME
                            ): self.run.deletion.start_time,
                            str(prov.model.PROV_ROLE): ProvRole.DELETED_RUN,
                        },
                    )
            if self.run.artifacts:
                for artifact in self.run.artifacts:
                    self.context.add_relation(
                        self.run, artifact, prov.model.ProvMembership
                    )
                    self.context.add_relation(
                        artifact,
                        self.run.deletion,
                        prov.model.ProvInvalidation,
                        {
                            str(
                                prov.model.PROV_ATTR_STARTTIME
                            ): self.run.deletion.start_time,
                            str(prov.model.PROV_ROLE): ProvRole.DELETED_RUN,
                        },
                    )
            if self.run.model_artifacts:
                for model_artifact in self.run.model_artifacts:
                    self.context.add_relation(
                        self.run, model_artifact, prov.model.ProvMembership
                    )
                    self.context.add_relation(
                        model_artifact,
                        self.run.deletion,
                        prov.model.ProvInvalidation,
                        {
                            str(
                                prov.model.PROV_ATTR_STARTTIME
                            ): self.run.deletion.start_time,
                            str(prov.model.PROV_ROLE): ProvRole.DELETED_RUN,
                        },
                    )
                    self.context.add_relation(
                        model_artifact,
                        model_artifact.artifact,
                        prov.model.ProvSpecialization,
                    )

        return self.context.document


@dataclass
class RegisteredModelAdditionModel:
    registered_model: RegisteredModel
    context: ProvContext = field(init=False)

    def __post_init__(self):
        self.context = ProvContext(prov.model.ProvDocument())

    @staticmethod
    def query(
        git_repository: InMemoryRepository,
        mlflow_repository: InMemoryRepository,
    ) -> Iterable[tuple[RegisteredModel]]:
        for registered_model in mlflow_repository.list_all(
            resource_type=RegisteredModel
        ):
            yield (registered_model,)

    def build_prov_model(self) -> prov.model.ProvDocument:
        self.context.add_element(self.registered_model)
        if self.registered_model.tags:
            for registered_model_tag in self.registered_model.tags:
                self.context.add_element(registered_model_tag)
        self.context.add_element(self.registered_model.creation)
        if self.registered_model.user:
            self.context.add_element(self.registered_model.user)

        self.context.add_relation(
            self.registered_model,
            self.registered_model.creation,
            prov.model.ProvGeneration,
            {
                str(
                    prov.model.PROV_ATTR_STARTTIME
                ): self.registered_model.creation.start_time,
                str(prov.model.PROV_ROLE): ProvRole.ADDED_REGISTERED_MODEL,
            },
        )
        if self.registered_model.tags:
            for registered_model_tag in self.registered_model.tags:
                self.context.add_relation(
                    self.registered_model,
                    registered_model_tag,
                    prov.model.ProvMembership,
                )
                self.context.add_relation(
                    registered_model_tag,
                    self.registered_model.creation,
                    prov.model.ProvGeneration,
                    {
                        str(
                            prov.model.PROV_ATTR_STARTTIME
                        ): self.registered_model.creation.start_time,
                        str(prov.model.PROV_ROLE): ProvRole.ADDED_REGISTERED_MODEL,
                    },
                )
                if self.registered_model.user:
                    self.context.add_relation(
                        registered_model_tag,
                        self.registered_model.user,
                        prov.model.ProvAttribution,
                    )
        if self.registered_model.user:
            self.context.add_relation(
                self.registered_model,
                self.registered_model.user,
                prov.model.ProvAttribution,
            )
            self.context.add_relation(
                self.registered_model.creation,
                self.registered_model.user,
                prov.model.ProvAssociation,
                {str(prov.model.PROV_ROLE): ProvRole.REGISTERED_MODEL_AUTHOR},
            )

        return self.context.document


@dataclass
class RegisteredModelVersionAdditionModel:
    registered_model: RegisteredModel
    registered_model_version: RegisteredModelVersion
    run: Run | None
    context: ProvContext = field(init=False)

    def __post_init__(self):
        self.context = ProvContext(prov.model.ProvDocument())

    @staticmethod
    def query(
        git_repository: InMemoryRepository,
        mlflow_repository: InMemoryRepository,
    ) -> Iterable[tuple[RegisteredModel, RegisteredModelVersion, Run | None]]:
        for registered_model in mlflow_repository.list_all(
            resource_type=RegisteredModel
        ):
            for registered_model_version in registered_model.versions:
                run = mlflow_repository.get(
                    resource_type=Run, run_id=registered_model_version.run_id
                )
                yield registered_model, registered_model_version, run

    def build_prov_model(self) -> prov.model.ProvDocument:
        self.context.add_element(self.registered_model)
        if self.registered_model.user:
            self.context.add_element(self.registered_model.user)
        self.context.add_element(self.registered_model_version)
        self.context.add_element(self.registered_model_version.creation)
        if self.registered_model_version.tags:
            for registered_model_version_tag in self.registered_model_version.tags:
                self.context.add_element(registered_model_version_tag)
        if self.run:
            self.context.add_element(self.run.creation)
            if self.run.model_artifacts:
                for model_artifact in self.run.model_artifacts:
                    self.context.add_element(model_artifact)

        self.context.add_relation(
            self.registered_model_version,
            self.registered_model,
            prov.model.ProvSpecialization,
        )
        self.context.add_relation(
            self.registered_model_version,
            self.registered_model_version.creation,
            prov.model.ProvGeneration,
            {
                str(
                    prov.model.PROV_ATTR_STARTTIME
                ): self.registered_model_version.creation.start_time,
                str(prov.model.PROV_ROLE): ProvRole.ADDED_REGISTERED_MODEL_VERSION,
            },
        )
        if self.run and self.run.model_artifacts != None:
            for model_artifact in self.run.model_artifacts:
                self.context.add_relation(
                    self.registered_model_version,
                    model_artifact,
                    prov.model.ProvDerivation,
                )
                self.context.add_relation(
                    self.registered_model_version.creation,
                    model_artifact,
                    prov.model.ProvUsage,
                    {
                        str(
                            prov.model.PROV_ATTR_STARTTIME
                        ): self.registered_model_version.creation.start_time,
                        str(
                            prov.model.PROV_ROLE
                        ): ProvRole.ADDED_REGISTERED_MODEL_VERSION,
                    },
                )
            self.context.add_relation(
                self.registered_model_version.creation,
                self.run.creation,
                prov.model.ProvCommunication,
            )
        if self.registered_model_version.user:
            self.context.add_relation(
                self.registered_model_version,
                self.registered_model_version.user,
                prov.model.ProvAttribution,
            )
            self.context.add_relation(
                self.registered_model_version.creation,
                self.registered_model_version.user,
                prov.model.ProvAssociation,
                {str(prov.model.PROV_ROLE): ProvRole.REGISTERED_MODEL_VERSION_AUTHOR},
            )
        if self.registered_model_version.tags:
            for registered_model_version_tag in self.registered_model_version.tags:
                self.context.add_relation(
                    self.registered_model_version,
                    registered_model_version_tag,
                    prov.model.ProvMembership,
                )
                self.context.add_relation(
                    registered_model_version_tag,
                    self.registered_model_version.creation,
                    prov.model.ProvGeneration,
                    {
                        str(
                            prov.model.PROV_ATTR_STARTTIME
                        ): self.registered_model_version.creation.start_time,
                        str(
                            prov.model.PROV_ROLE
                        ): ProvRole.ADDED_REGISTERED_MODEL_VERSION,
                    },
                )
                if self.registered_model_version.user:
                    self.context.add_relation(
                        registered_model_version_tag,
                        self.registered_model_version.user,
                        prov.model.ProvAttribution,
                    )

        return self.context.document


@dataclass
class RegisteredModelVersionDeletionModel:
    registered_model_version: RegisteredModelVersion
    context: ProvContext = field(init=False)

    def __post_init__(self):
        self.context = ProvContext(prov.model.ProvDocument())

    @staticmethod
    def query(
        git_repository: InMemoryRepository,
        mlflow_repository: InMemoryRepository,
    ) -> Iterable[tuple[RegisteredModelVersion]]:
        for registered_model in mlflow_repository.list_all(
            resource_type=RegisteredModel,
        ):
            for registered_model_version in registered_model.versions:
                if (
                    registered_model_version.registered_model_version_stage
                    == RegisteredModelVersionStage.DELETED_INTERNAL
                ):
                    yield (registered_model_version,)

    def build_prov_model(self) -> prov.model.ProvDocument:
        self.context.add_element(self.registered_model_version)
        if self.registered_model_version.tags:
            for registered_model_version_tag in self.registered_model_version.tags:
                self.context.add_element(registered_model_version_tag)
        self.context.add_element(self.registered_model_version.deletion)
        if self.registered_model_version.user:
            self.context.add_element(self.registered_model_version.user)

        if self.registered_model_version.user:
            self.context.add_relation(
                self.registered_model_version.deletion,
                self.registered_model_version.user,
                prov.model.ProvAssociation,
            )
        self.context.add_relation(
            self.registered_model_version,
            self.registered_model_version.deletion,
            prov.model.ProvInvalidation,
            {
                str(
                    prov.model.PROV_ATTR_STARTTIME
                ): self.registered_model_version.deletion.start_time,
                str(prov.model.PROV_ROLE): ProvRole.DELETED_REGISTERED_MODEL_VERSION,
            },
        )
        if self.registered_model_version.tags:
            for registered_model_version_tag in self.registered_model_version.tags:
                self.context.add_relation(
                    self.registered_model_version,
                    registered_model_version_tag,
                    prov.model.ProvMembership,
                )
                self.context.add_relation(
                    registered_model_version_tag,
                    self.registered_model_version.deletion,
                    prov.model.ProvInvalidation,
                    {
                        str(
                            prov.model.PROV_ATTR_STARTTIME
                        ): self.registered_model_version.deletion.start_time,
                        str(
                            prov.model.PROV_ROLE
                        ): ProvRole.DELETED_REGISTERED_MODEL_VERSION,
                    },
                )

        return self.context.document


@dataclass
class CallableModel:
    model: Type[
        FileAdditionModel
        | FileModificationModel
        | FileDeletionModel
        | ExperimentAdditionModel
        | ExperimentDeletionModel
        | RunAdditionModel
        | RunDeletionModel
        | RegisteredModelAdditionModel
        | RegisteredModelVersionAdditionModel
        | RegisteredModelVersionDeletionModel
    ]
    document: prov.model.ProvDocument = field(init=False)

    def __post_init__(self):
        self.document = prov.model.ProvDocument()

    def __call__(
        self,
        repositories: list[InMemoryRepository],
    ):
        query_result = self.model.query(
            git_repository=repositories[0],  # type: ignore
            mlflow_repository=repositories[1],  # type: ignore
        )

        for args in query_result:
            m = self.model(*args)  # type: ignore
            self.document.update(m.build_prov_model())

        return self.document


MODELS = [
    CallableModel(FileAdditionModel),
    CallableModel(FileModificationModel),
    CallableModel(FileDeletionModel),
    CallableModel(ExperimentAdditionModel),
    CallableModel(ExperimentDeletionModel),
    CallableModel(RunAdditionModel),
    CallableModel(RunDeletionModel),
    CallableModel(RegisteredModelAdditionModel),
    CallableModel(RegisteredModelVersionAdditionModel),
    CallableModel(RegisteredModelVersionDeletionModel),
]
