import datetime
import unittest
from typing import Type

import prov.model

from mlflow2prov.adapters.git.fetcher import GitFetcher
from mlflow2prov.adapters.mlflow.fetcher import MLflowFetcher
from mlflow2prov.adapters.repository import InMemoryRepository
from mlflow2prov.domain.constants import ProvRole
from mlflow2prov.domain.model import (
    Commit,
    Experiment,
    LifecycleStage,
    RegisteredModel,
    RegisteredModelVersion,
    RegisteredModelVersionStage,
    RegisteredModelVersionTag,
    Run,
    RunStatus,
    User,
)
from mlflow2prov.prov import operations
from mlflow2prov.prov.model import (
    DEFAULT_NAMESPACE,
    CallableModel,
    ExperimentAdditionModel,
    ExperimentDeletionModel,
    FileAdditionModel,
    FileDeletionModel,
    FileModificationModel,
    ProvContext,
    RegisteredModelAdditionModel,
    RegisteredModelVersionAdditionModel,
    RegisteredModelVersionDeletionModel,
    RunAdditionModel,
    RunDeletionModel,
)
from mlflow2prov.service_layer.unit_of_work import InMemoryUnitOfWork
from tests.test_git_fetcher import path_testproject_git_repo
from tests.utils import random_suffix

today = datetime.datetime.now()
yesterday = today - datetime.timedelta(days=1)
tomorrow = today + datetime.timedelta(days=1)


def create_commit(
    parents: list[str], authored_at: datetime.datetime, committed_at: datetime.datetime
) -> Commit:
    return Commit(
        sha=f"commit-hash-{random_suffix()}",
        message=f"commit-message-{random_suffix()}",
        title=f"commit-title-{random_suffix()}",
        author=None,
        committer=None,
        parents=parents,
        authored_at=authored_at,
        committed_at=committed_at,
    )


class TestProvContext:
    def test_add_element(self):
        case = unittest.TestCase()

        commit = create_commit(parents=[], authored_at=today, committed_at=today)

        context = ProvContext(prov.model.ProvDocument())
        el: prov.model.ProvRecord = context.add_element(commit)

        el_expected: prov.model.ProvElement = commit.to_prov()

        assert el._prov_type == el_expected._prov_type
        assert el.identifier == el_expected.identifier
        case.assertCountEqual(el.attributes, el_expected.attributes)

    def test_convert_to_prov_element(self):
        commit = create_commit(parents=[], authored_at=today, committed_at=today)

        context = ProvContext(prov.model.ProvDocument())
        record = context.convert_to_prov_element(commit)

        el_expected: prov.model.ProvElement = commit.to_prov()
        rec_expcected: prov.model.ProvRecord = prov.model.ProvDocument().new_record(
            el_expected._prov_type, el_expected.identifier, el_expected.attributes
        )

        assert record == rec_expcected

    def test_add_relation(self):
        parent = create_commit(
            parents=[], authored_at=yesterday, committed_at=yesterday
        )
        commit = create_commit(
            parents=[parent.sha], authored_at=today, committed_at=today
        )

        context: ProvContext = ProvContext(prov.model.ProvDocument())
        source = context.add_element(dataclass_instance=commit)
        target = context.add_element(dataclass_instance=parent)
        relationship_type: Type[prov.model.ProvRelation] = prov.model.ProvCommunication
        attributes = dict()
        relationship = context.document.new_record(
            relationship_type._prov_type,
            prov.model.QualifiedName(
                DEFAULT_NAMESPACE, f"relation:{source.identifier}:{target.identifier}"
            ),
            {
                relationship_type.FORMAL_ATTRIBUTES[0]: source,  # type: ignore
                relationship_type.FORMAL_ATTRIBUTES[1]: target,  # type: ignore
            },
        )
        relationship.add_attributes(attributes=attributes)
        context.document.add_record(relationship)

        context_expected: ProvContext = ProvContext(prov.model.ProvDocument())
        context_expected.add_relation(
            source_dataclass_instance=commit,
            target_dataclass_instance=parent,
            relationship_type=prov.model.ProvCommunication,
            attributes={},
        )

        assert context == context_expected


class TestExperimentAdditionModel:
    def test_post_init(self):
        experiment_id = f"experiment-id-{random_suffix()}"
        name = f"experiment-name-{random_suffix()}"
        user = User.from_username_str(f"user-name-{random_suffix()}")
        artifact_location = f"experiment-artifact-location-{random_suffix()}"
        lifecycle_stage = LifecycleStage.ACTIVE
        tags = []
        created_at = today
        last_updated = today

        experiment = Experiment(
            experiment_id=experiment_id,
            name=name,
            user=user,
            artifact_location=artifact_location,
            lifecycle_stage=lifecycle_stage,
            tags=tags,
            created_at=created_at,
            last_updated=last_updated,
        )

        model = ExperimentAdditionModel(experiment)

        assert model.context == ProvContext(document=prov.model.ProvDocument())


class TestExperimentDeletionModel:
    def test_post_init(self):
        experiment_id = f"experiment-id-{random_suffix()}"
        name = f"experiment-name-{random_suffix()}"
        user = User.from_username_str(f"user-name-{random_suffix()}")
        artifact_location = f"experiment-artifact-location-{random_suffix()}"
        lifecycle_stage = LifecycleStage.ACTIVE
        tags = []
        created_at = today
        last_updated = today

        experiment = Experiment(
            experiment_id=experiment_id,
            name=name,
            user=user,
            artifact_location=artifact_location,
            lifecycle_stage=lifecycle_stage,
            tags=tags,
            created_at=created_at,
            last_updated=last_updated,
        )

        run_id = f"run-id-{random_suffix()}"
        name = f"run-name-{random_suffix()}"
        experiment_id = f"experiment-id-{random_suffix()}"
        user = User.from_username_str(f"user-name-{random_suffix()}")

        status = RunStatus.FINISHED
        start_time = today
        end_time = today

        lifecycle_stage = LifecycleStage.ACTIVE
        artifact_uri = f"run-artifact-uri-{random_suffix()}"

        metrics = []
        params = []
        tags = []
        artifacts = []
        model_artifacts = []

        note = None
        source_type = None
        source_name = None
        source_git_commit = None
        source_git_branch = None
        source_git_repo_url = None

        run = Run(
            run_id=run_id,
            name=name,
            experiment_id=experiment_id,
            user=user,
            status=status,
            start_time=start_time,
            end_time=end_time,
            lifecycle_stage=lifecycle_stage,
            artifact_uri=artifact_uri,
            metrics=metrics,
            params=params,
            tags=tags,
            artifacts=artifacts,
            model_artifacts=model_artifacts,
            note=note,
            source_type=source_type,
            source_name=source_name,
            source_git_commit=source_git_commit,
            source_git_branch=source_git_branch,
            source_git_repo_url=source_git_repo_url,
        )

        model = ExperimentDeletionModel(experiment, run)

        assert model.context == ProvContext(document=prov.model.ProvDocument())


class TestRegisteredModelVersionDeletionModel:
    def test_post_init(self):
        name = f"registered-model-name-{random_suffix()}"
        version = f"registered-model-version-{random_suffix()}"
        created_at = today
        last_updated_at = today
        description = f"registered-model-version-description-{random_suffix()}"
        user = User.from_username_str(f"user-name-{random_suffix()}")
        registered_model_version_stage = RegisteredModelVersionStage.NONE
        source_path = f"source-path-{random_suffix()}"
        run_id = f"run-id-{random_suffix()}"
        status = f"status-{random_suffix()}"
        status_message = f"status-message-{random_suffix()}"
        tags = []
        run_link = f"run-link-{random_suffix()}"

        registered_model_version = RegisteredModelVersion(
            name=name,
            version=version,
            created_at=created_at,
            last_updated_at=last_updated_at,
            description=description,
            user=user,
            registered_model_version_stage=registered_model_version_stage,
            source_path=source_path,
            run_id=run_id,
            status=status,
            status_message=status_message,
            tags=tags,
            run_link=run_link,
        )

        model = RegisteredModelVersionDeletionModel(registered_model_version)

        assert model.context == ProvContext(document=prov.model.ProvDocument())

    def test_query(self):
        name = f"registered-model-name-{random_suffix()}"
        version = f"registered-model-version-{random_suffix()}"
        created_at = today
        last_updated_at = today
        description = f"registered-model-version-description-{random_suffix()}"
        user = User.from_username_str(f"user-name-{random_suffix()}")
        registered_model_version_stage = RegisteredModelVersionStage.DELETED_INTERNAL
        source_path = f"source-path-{random_suffix()}"
        run_id = f"run-id-{random_suffix()}"
        status = f"status-{random_suffix()}"
        status_message = f"status-message-{random_suffix()}"
        tags = []
        run_link = f"run-link-{random_suffix()}"

        registered_model_version = RegisteredModelVersion(
            name=name,
            version=version,
            created_at=created_at,
            last_updated_at=last_updated_at,
            description=description,
            user=user,
            registered_model_version_stage=registered_model_version_stage,
            source_path=source_path,
            run_id=run_id,
            status=status,
            status_message=status_message,
            tags=tags,
            run_link=run_link,
        )

        name = f"registered-model-name-{random_suffix()}"
        created_at = today
        last_updated_at = today
        description = f"registered-model-description-{random_suffix()}"
        user = User.from_username_str(f"user-name-{random_suffix()}")
        versions = [registered_model_version]
        tags = []

        registered_model = RegisteredModel(
            name=name,
            created_at=created_at,
            last_updated_at=last_updated_at,
            description=description,
            user=user,
            versions=versions,
            tags=tags,
        )

        model = RegisteredModelVersionDeletionModel(registered_model_version)
        mlflow_repository = InMemoryRepository()
        mlflow_repository.add(registered_model)
        query_result = model.query(InMemoryRepository(), mlflow_repository)

        for el in query_result:
            assert el[0] == registered_model_version

    def build_prov_model(
        self, registered_model_version: RegisteredModelVersion
    ) -> prov.model.ProvDocument:
        context = ProvContext(prov.model.ProvDocument())

        context.add_element(registered_model_version)
        if registered_model_version.tags:
            for registered_model_version_tag in registered_model_version.tags:
                context.add_element(registered_model_version_tag)
        context.add_element(registered_model_version.deletion)
        if registered_model_version.user:
            context.add_element(registered_model_version.user)

        if registered_model_version.user:
            context.add_relation(
                registered_model_version.deletion,
                registered_model_version.user,
                prov.model.ProvAssociation,
            )
        context.add_relation(
            registered_model_version,
            registered_model_version.deletion,
            prov.model.ProvInvalidation,
            {
                str(
                    prov.model.PROV_ATTR_STARTTIME
                ): registered_model_version.deletion.start_time,
                str(prov.model.PROV_ROLE): ProvRole.DELETED_REGISTERED_MODEL_VERSION,
            },
        )
        if registered_model_version.tags:
            for registered_model_version_tag in registered_model_version.tags:
                context.add_relation(
                    registered_model_version,
                    registered_model_version_tag,
                    prov.model.ProvMembership,
                )
                context.add_relation(
                    registered_model_version_tag,
                    registered_model_version.deletion,
                    prov.model.ProvInvalidation,
                    {
                        str(
                            prov.model.PROV_ATTR_STARTTIME
                        ): registered_model_version.deletion.start_time,
                        str(
                            prov.model.PROV_ROLE
                        ): ProvRole.DELETED_REGISTERED_MODEL_VERSION,
                    },
                )

        return context.document

    def test_build_prov_model(self):
        registered_model_name = f"registered-model-name-{random_suffix()}"
        registered_model_version = f"registered-model-version-{random_suffix()}"
        name = f"registered-model-version-tag-name-{random_suffix()}"
        value = f"registered-model-version-tag-value-{random_suffix()}"

        registered_model_version_tag = RegisteredModelVersionTag(
            registered_model_name=registered_model_name,
            registered_model_version=registered_model_version,
            name=name,
            value=value,
        )

        name = f"registered-model-name-{random_suffix()}"
        version = f"registered-model-version-{random_suffix()}"
        created_at = today
        last_updated_at = today
        description = f"registered-model-version-description-{random_suffix()}"
        user = User.from_username_str(f"user-name-{random_suffix()}")
        registered_model_version_stage = RegisteredModelVersionStage.DELETED_INTERNAL
        source_path = f"source-path-{random_suffix()}"
        run_id = f"run-id-{random_suffix()}"
        status = f"status-{random_suffix()}"
        status_message = f"status-message-{random_suffix()}"
        tags = [registered_model_version_tag]
        run_link = f"run-link-{random_suffix()}"

        registered_model_version = RegisteredModelVersion(
            name=name,
            version=version,
            created_at=created_at,
            last_updated_at=last_updated_at,
            description=description,
            user=user,
            registered_model_version_stage=registered_model_version_stage,
            source_path=source_path,
            run_id=run_id,
            status=status,
            status_message=status_message,
            tags=tags,
            run_link=run_link,
        )

        model = RegisteredModelVersionDeletionModel(registered_model_version)
        doc = model.build_prov_model()

        assert doc != False
        assert doc == self.build_prov_model(registered_model_version)


class TestCallableModel:
    def test_call(self):
        uow: InMemoryUnitOfWork = InMemoryUnitOfWork()

        git_fetcher: GitFetcher = GitFetcher()
        git_fetcher.get_from_local_path(path_testproject_git_repo)

        url = "http://localhost:5000"
        mlflow_fetcher = MLflowFetcher(tracking_uri=url)

        with uow:
            for resource in git_fetcher.fetch_all():
                uow.resources[str(path_testproject_git_repo)].add(resource)
        uow.commit()

        with uow:
            for resource in mlflow_fetcher.fetch_all():
                uow.resources[url].add(resource)
        uow.commit()

        merged = prov.model.ProvDocument()
        merged_expected = prov.model.ProvDocument()

        # location == str(path_testproject_git_repo)

        for callable_model in [
            CallableModel(FileAdditionModel),
            CallableModel(FileModificationModel),
            CallableModel(FileDeletionModel),
        ]:
            expected_document: prov.model.ProvDocument = callable_model(
                repositories=[
                    uow.resources[str(path_testproject_git_repo)],
                    None,  # type:ignore
                ]
            )

            repository: InMemoryRepository = uow.resources[
                str(path_testproject_git_repo)
            ]
            document: prov.model.ProvDocument = prov.model.ProvDocument()

            for args in callable_model.model.query(
                git_repository=repository,
                mlflow_repository=None,  # type:ignore
            ):
                m = callable_model.model(*args)  # type: ignore

                new_model = m.build_prov_model()
                document.update(new_model)

            merged = operations.merge([merged, document])
            merged_expected = operations.merge([merged_expected, expected_document])

        assert len(list(merged.get_records())) == len(
            list(merged_expected.get_records())
        )

        # location == url

        for callable_model in [
            CallableModel(RunAdditionModel),
        ]:
            expected_document: prov.model.ProvDocument = callable_model(
                repositories=[
                    uow.resources[str(path_testproject_git_repo)],
                    uow.resources[url],
                ]
            )

            git_repository: InMemoryRepository = uow.resources[
                str(path_testproject_git_repo)
            ]
            mlflow_repository: InMemoryRepository = uow.resources[url]
            document: prov.model.ProvDocument = prov.model.ProvDocument()

            for args in callable_model.model.query(
                git_repository=git_repository,
                mlflow_repository=mlflow_repository,
            ):
                m = callable_model.model(*args)  # type: ignore

                new_model = m.build_prov_model()
                document.update(new_model)

            merged = operations.merge([merged, document])
            merged_expected = operations.merge([merged_expected, expected_document])

        assert len(list(merged.get_records())) == len(
            list(merged_expected.get_records())
        )

        # location == url

        for callable_model in [
            CallableModel(ExperimentAdditionModel),
            CallableModel(ExperimentDeletionModel),
            CallableModel(RunDeletionModel),
            CallableModel(RegisteredModelAdditionModel),
            CallableModel(RegisteredModelVersionAdditionModel),
            CallableModel(RegisteredModelVersionDeletionModel),
        ]:
            expected_document: prov.model.ProvDocument = callable_model(
                repositories=[None, uow.resources[url]]  # type:ignore
            )

            repository: InMemoryRepository = uow.resources[url]
            document: prov.model.ProvDocument = prov.model.ProvDocument()

            for args in callable_model.model.query(
                git_repository=None,  # type:ignore
                mlflow_repository=repository,
            ):
                m = callable_model.model(*args)  # type: ignore

                new_model = m.build_prov_model()
                document.update(new_model)

            merged = operations.merge([merged, document])
            merged_expected = operations.merge([merged_expected, expected_document])

        assert len(list(merged.get_records())) == len(
            list(merged_expected.get_records())
        )
