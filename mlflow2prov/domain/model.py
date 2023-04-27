from __future__ import annotations

import datetime
import logging
import re
import urllib.parse
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

import prov.model

from mlflow2prov.domain.constants import ProvType
from mlflow2prov.utils.prov_utils import document_factory, qualified_name

log = logging.getLogger(__name__)


@dataclass
class User:
    name: str
    email: str
    username: str = ""
    prov_role: str | None = None

    def __post_init__(self):
        self.email = self.email.lower()

    @classmethod
    def from_username_str(cls, username: str, name: str = "", email: str = "") -> User:
        return cls(name=name, email=email, username=username)

    @classmethod
    def from_splitable_user_string(cls, user_str: str) -> User:
        regex = re.compile("[^>]<([^>]+)>")
        strlist = list(filter(None, regex.split(user_str)))

        return cls(name=strlist[0], email=strlist[1])

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        name = urllib.parse.quote_plus(self.name, safe="", encoding=None, errors=None)
        email = urllib.parse.quote_plus(self.email, safe="", encoding=None, errors=None)

        return qualified_name(f"User?name={name}&email={email}")

    def to_prov(self) -> prov.model.ProvAgent:
        prov_attributes = [
            ("name", self.name),
            ("email", self.email),
            ("username", self.username),
            (prov.model.PROV_ROLE, self.prov_role),
            (prov.model.PROV_TYPE, ProvType.USER),
        ]

        return prov.model.ProvAgent(
            document_factory(), self.prov_identifier, prov_attributes
        )


@dataclass
class File:
    name: str
    path: str
    commit: str

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        name = urllib.parse.quote_plus(self.name, safe="", encoding=None, errors=None)
        path = urllib.parse.quote_plus(self.path, safe="", encoding=None, errors=None)
        commit = urllib.parse.quote_plus(
            self.commit, safe="", encoding=None, errors=None
        )

        return qualified_name(f"File?name={name}&path={path}&commit={commit}")

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("name", self.name),
            ("path", self.path),
            ("commit", self.commit),
            (prov.model.PROV_TYPE, ProvType.FILE),
        ]

        return prov.model.ProvEntity(
            document_factory(), self.prov_identifier, prov_attributes
        )


@dataclass
class FileRevision(File):
    status: str
    file: File | None = None
    previous: FileRevision | None = None

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        name = urllib.parse.quote_plus(self.name, safe="", encoding=None, errors=None)
        path = urllib.parse.quote_plus(self.path, safe="", encoding=None, errors=None)
        commit = urllib.parse.quote_plus(
            self.commit, safe="", encoding=None, errors=None
        )
        status = urllib.parse.quote_plus(
            self.status, safe="", encoding=None, errors=None
        )

        return qualified_name(
            f"FileRevision?name={name}&path={path}&commit={commit}&status={status}"
        )

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("name", self.name),
            ("path", self.path),
            ("status", self.status),
            (prov.model.PROV_TYPE, ProvType.FILE_REVISION),
        ]

        return prov.model.ProvEntity(
            document_factory(),
            self.prov_identifier,
            prov_attributes,
        )


@dataclass
class Commit:
    sha: str
    title: str
    message: str
    author: User | None
    committer: User | None
    parents: list[str]
    authored_at: datetime
    committed_at: datetime

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        sha = urllib.parse.quote_plus(self.sha, safe="", encoding=None, errors=None)

        return qualified_name(f"Commit?sha={sha}")

    def to_prov(self) -> prov.model.ProvActivity:
        prov_attributes = [
            ("sha", self.sha),
            ("title", self.title),
            ("message", self.message),
            ("authored_at", self.authored_at),
            ("committed_at", self.committed_at),
            (prov.model.PROV_ATTR_STARTTIME, self.authored_at),
            (prov.model.PROV_ATTR_ENDTIME, self.committed_at),
            (prov.model.PROV_TYPE, ProvType.COMMIT),
        ]

        return prov.model.ProvActivity(
            document_factory(), self.prov_identifier, prov_attributes
        )


@dataclass
class Creation:
    uid: str
    resource_type: str
    start_time: datetime
    end_time: datetime | None = (
        None  # also allow None, because a run may be still running
    )

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        uid = urllib.parse.quote_plus(self.uid, safe="", encoding=None, errors=None)
        resource_type = urllib.parse.quote_plus(
            self.resource_type, safe="", encoding=None, errors=None
        )

        return qualified_name(f"Creation?uid={uid}&resource_type={resource_type}")

    def to_prov(self) -> prov.model.ProvActivity:
        prov_attributes = [
            ("uid", self.uid),
            (prov.model.PROV_ATTR_STARTTIME, self.start_time),
            (prov.model.PROV_ATTR_ENDTIME, self.end_time) if self.end_time else None,
            (prov.model.PROV_TYPE, ProvType.CREATION),
        ]

        return prov.model.ProvActivity(
            document_factory(), self.prov_identifier, prov_attributes
        )

    @classmethod
    def from_experiment(cls, experiment: Experiment) -> Creation:
        return cls(
            uid=experiment.experiment_id,
            resource_type=ProvType.EXPERIMENT,
            start_time=experiment.created_at,
            end_time=experiment.last_updated if experiment.last_updated else None,
        )

    @classmethod
    def from_run(cls, run: Run) -> Creation:
        return cls(
            uid=run.run_id,
            resource_type=ProvType.RUN,
            start_time=run.start_time,
            end_time=run.end_time if run.end_time else None,
        )

    @classmethod
    def from_registered_model(cls, registered_model: RegisteredModel) -> Creation:
        return cls(
            uid=registered_model.name,
            resource_type=ProvType.REGISTERED_MODEL,
            start_time=registered_model.created_at,
            end_time=registered_model.last_updated_at
            if registered_model.last_updated_at
            else None,
        )

    @classmethod
    def from_registered_model_version(
        cls, registered_model_version: RegisteredModelVersion
    ) -> Creation:
        return cls(
            uid=f"{registered_model_version.name}-version-{registered_model_version.version}",
            resource_type=ProvType.REGISTERED_MODEL_VERSION,
            start_time=registered_model_version.created_at,
            end_time=registered_model_version.last_updated_at
            if registered_model_version.last_updated_at
            else None,
        )


@dataclass
class Deletion:
    uid: str
    resource_type: str
    start_time: datetime
    end_time: datetime | None  # also allow None, because a run may be still running

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        uid = urllib.parse.quote_plus(self.uid, safe="", encoding=None, errors=None)
        resource_type = urllib.parse.quote_plus(
            self.resource_type, safe="", encoding=None, errors=None
        )

        return qualified_name(f"Deletion?uid={uid}&resource_type={resource_type}")

    def to_prov(self) -> prov.model.ProvActivity:
        prov_attributes = [
            ("uid", self.uid),
            (prov.model.PROV_ATTR_STARTTIME, self.start_time),
            (prov.model.PROV_ATTR_ENDTIME, self.end_time),
            (prov.model.PROV_TYPE, ProvType.DELETION),
        ]

        return prov.model.ProvActivity(
            document_factory(), self.prov_identifier, prov_attributes
        )

    @classmethod
    def from_experiment(cls, experiment: Experiment) -> Deletion:
        return cls(
            uid=experiment.experiment_id,
            resource_type=ProvType.EXPERIMENT,
            start_time=experiment.last_updated,
            end_time=experiment.last_updated,
        )

    @classmethod
    def from_run(cls, run: Run) -> Deletion:
        return cls(
            uid=run.run_id,
            resource_type=ProvType.RUN,
            start_time=run.end_time if run.end_time else run.start_time,
            end_time=run.end_time,
        )

    @classmethod
    def from_registered_model_version(
        cls, registered_model_version: RegisteredModelVersion
    ) -> Deletion:
        return cls(
            uid=f"{registered_model_version.name}-version-{registered_model_version.version}",
            resource_type=ProvType.REGISTERED_MODEL_VERSION,
            start_time=registered_model_version.last_updated_at,
            end_time=registered_model_version.last_updated_at,
        )


@dataclass
class Experiment:
    experiment_id: str
    name: str
    user: User | None
    artifact_location: str
    lifecycle_stage: LifecycleStage
    tags: list[ExperimentTag] | None
    created_at: datetime
    last_updated: datetime

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        experiment_id = urllib.parse.quote_plus(
            self.experiment_id, safe="", encoding=None, errors=None
        )
        name = urllib.parse.quote_plus(self.name, safe="", encoding=None, errors=None)

        return qualified_name(f"Experiment?experiment_id={experiment_id}&name={name}")

    @property
    def creation(self) -> Creation:
        return Creation.from_experiment(self)

    @property
    def deletion(self) -> Deletion:
        return Deletion.from_experiment(self)

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("experiment_id", self.experiment_id),
            ("name", self.name),
            ("artifact_location", self.artifact_location),
            ("lifecycle_stage", LifecycleStage.to_string(self.lifecycle_stage)),
            (prov.model.PROV_TYPE, ProvType.EXPERIMENT),
            (prov.model.PROV_TYPE, ProvType.COLLECTION),
        ]

        return prov.model.ProvEntity(
            document_factory(), self.prov_identifier, prov_attributes
        )


@dataclass
class Run:
    run_id: str
    name: str
    experiment_id: str
    user: User | None
    status: RunStatus
    start_time: datetime
    end_time: datetime | None
    lifecycle_stage: LifecycleStage | None
    artifact_uri: str | None
    metrics: list[Metric] | None
    params: list[Param] | None
    tags: list[RunTag] | None
    artifacts: list[Artifact] | None
    model_artifacts: list[ModelArtifact] | None
    note: str | None
    source_type: SourceType | None
    source_name: str | None
    source_git_commit: str | None
    source_git_branch: str | None
    source_git_repo_url: str | None

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        run_id = urllib.parse.quote_plus(
            self.run_id, safe="", encoding=None, errors=None
        )
        name = urllib.parse.quote_plus(self.name, safe="", encoding=None, errors=None)

        return qualified_name(f"Run?run_id={run_id}&name={name}")

    @property
    def creation(self) -> Creation:
        return Creation.from_run(self)

    @property
    def deletion(self) -> Deletion:
        return Deletion.from_run(self)

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("run_id", self.run_id),
            ("name", self.name),
            ("status", RunStatus.to_string(self.status)),
            ("lifecycle_stage", LifecycleStage.to_string(self.lifecycle_stage))
            if self.lifecycle_stage
            else None,
            ("artifact_uri", self.artifact_uri),
            ("note", self.note),
            (prov.model.PROV_TYPE, ProvType.RUN),
            (prov.model.PROV_TYPE, ProvType.COLLECTION),
        ]

        return prov.model.ProvEntity(
            document_factory(), self.prov_identifier, prov_attributes
        )


@dataclass
class Metric:
    run_id: str
    name: str
    value: float
    timestamp: datetime
    step: int

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        run_id = urllib.parse.quote_plus(
            self.run_id, safe="", encoding=None, errors=None
        )
        name = urllib.parse.quote_plus(self.name, safe="", encoding=None, errors=None)
        step = urllib.parse.quote_plus(
            str(self.step), safe="", encoding=None, errors=None
        )

        return qualified_name(f"Metric?run_id={run_id}&name={name}&step={step}")

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("name", self.name),
            ("value", self.value),
            ("timestamp", self.timestamp),
            ("step", self.step),
            (prov.model.PROV_TYPE, ProvType.METRIC),
        ]

        return prov.model.ProvEntity(
            document_factory(), self.prov_identifier, prov_attributes
        )


@dataclass
class Param:
    run_id: str
    name: str
    value: str

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        run_id = urllib.parse.quote_plus(
            self.run_id, safe="", encoding=None, errors=None
        )
        name = urllib.parse.quote_plus(self.name, safe="", encoding=None, errors=None)

        return qualified_name(f"Param?run_id={run_id}&name={name}")

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("name", self.name),
            ("value", self.value),
            (prov.model.PROV_TYPE, ProvType.PARAM),
        ]

        return prov.model.ProvEntity(
            document_factory(), self.prov_identifier, prov_attributes
        )


@dataclass
class Artifact:
    run_id: str
    path: str
    is_dir: bool
    file_size: int  # in bytes

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        run_id = urllib.parse.quote_plus(
            self.run_id, safe="", encoding=None, errors=None
        )
        path = urllib.parse.quote_plus(self.path, safe="", encoding=None, errors=None)

        return qualified_name(f"Artifact?run_id={run_id}&path={path}")

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("path", self.path),
            ("is_dir", self.is_dir),
            ("file_size", self.file_size),
            (prov.model.PROV_TYPE, ProvType.ARTIFACT),
        ]

        return prov.model.ProvEntity(
            document_factory(), self.prov_identifier, prov_attributes
        )


@dataclass
class ModelArtifact(Artifact):
    artifact: Artifact | None = None

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        run_id = urllib.parse.quote_plus(
            self.run_id, safe="", encoding=None, errors=None
        )
        path = urllib.parse.quote_plus(self.path, safe="", encoding=None, errors=None)

        return qualified_name(f"ModelArtifact?run_id={run_id}&path={path}")

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("path", self.path),
            ("is_dir", self.is_dir),
            ("file_size", self.file_size),
            (prov.model.PROV_TYPE, ProvType.MODEL_ARTIFACT),
        ]

        return prov.model.ProvEntity(
            document_factory(), self.prov_identifier, prov_attributes
        )


@dataclass
class RegisteredModel:
    name: str
    created_at: datetime
    last_updated_at: datetime
    description: str | None
    user: User | None
    versions: list[RegisteredModelVersion]
    tags: list[RegisteredModelTag]

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        name = urllib.parse.quote_plus(self.name, safe="", encoding=None, errors=None)

        return qualified_name(f"RegisteredModel?name={name}")

    @property
    def creation(self) -> Creation:
        return Creation.from_registered_model(self)

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("name", self.name),
            ("created_at", self.created_at),
            ("last_updated_at", self.last_updated_at),
            ("description", self.description),
            (prov.model.PROV_TYPE, ProvType.REGISTERED_MODEL),
            (prov.model.PROV_TYPE, ProvType.COLLECTION),
        ]

        return prov.model.ProvEntity(
            document_factory(), self.prov_identifier, prov_attributes
        )


@dataclass
class RegisteredModelVersion:
    name: str
    version: str
    created_at: datetime
    last_updated_at: datetime
    description: str | None
    user: User | None
    registered_model_version_stage: RegisteredModelVersionStage | None
    source_path: str | None
    run_id: str | None
    status: str
    status_message: str | None
    tags: list[RegisteredModelVersionTag] | None
    run_link: str | None

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        name = urllib.parse.quote_plus(self.name, safe="", encoding=None, errors=None)
        version = urllib.parse.quote_plus(
            self.version, safe="", encoding=None, errors=None
        )

        return qualified_name(f"RegisteredModelVersion?name={name}&version={version}")

    @property
    def creation(self) -> Creation:
        return Creation.from_registered_model_version(self)

    @property
    def deletion(self) -> Deletion:
        return Deletion.from_registered_model_version(self)

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("name", self.name),
            ("version", self.version),
            ("created_at", self.created_at),
            ("last_updated_at", self.last_updated_at),
            ("description", self.description),
            (
                "registered_model_version_stage",
                RegisteredModelVersionStage.to_string(
                    self.registered_model_version_stage
                ),
            )
            if self.registered_model_version_stage
            else None,
            ("source_path", self.source_path),
            ("status", self.status),
            ("status_message", self.status_message),
            ("run_link", self.run_link),
            (prov.model.PROV_TYPE, ProvType.REGISTERED_MODEL_VERSION),
            (prov.model.PROV_TYPE, ProvType.COLLECTION),
        ]

        return prov.model.ProvEntity(
            document_factory(), self.prov_identifier, prov_attributes
        )


@dataclass
class Tag:
    name: str
    value: str


@dataclass
class ExperimentTag(Tag):
    experiment_id: str

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        name = urllib.parse.quote_plus(self.name, safe="", encoding=None, errors=None)
        experiment_id = urllib.parse.quote_plus(
            self.experiment_id, safe="", encoding=None, errors=None
        )

        return qualified_name(
            f"ExperimentTag?experiment_id={experiment_id}&name={name}"
        )

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("name", self.name),
            ("value", self.value),
            (prov.model.PROV_TYPE, ProvType.EXPERIMENT_TAG),
        ]

        return prov.model.ProvEntity(
            document_factory(), self.prov_identifier, prov_attributes
        )


@dataclass
class RunTag(Tag):
    run_id: str

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        run_id = urllib.parse.quote_plus(
            self.run_id, safe="", encoding=None, errors=None
        )
        name = urllib.parse.quote_plus(self.name, safe="", encoding=None, errors=None)

        return qualified_name(f"RunTag?run_id={run_id}&name={name}")

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("name", self.name),
            ("value", self.value),
            (prov.model.PROV_TYPE, ProvType.RUN_TAG),
        ]

        return prov.model.ProvEntity(
            document_factory(), self.prov_identifier, prov_attributes
        )


@dataclass
class RegisteredModelTag(Tag):
    registered_model_name: str

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        registered_model_name = urllib.parse.quote_plus(
            self.registered_model_name, safe="", encoding=None, errors=None
        )
        name = urllib.parse.quote_plus(self.name, safe="", encoding=None, errors=None)

        return qualified_name(
            f"RegisteredModelTag?registered_model_name={registered_model_name}&name={name}"
        )

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("name", self.name),
            ("value", self.value),
            (prov.model.PROV_TYPE, ProvType.REGISTERED_MODEL_TAG),
        ]

        return prov.model.ProvEntity(
            document_factory(), self.prov_identifier, prov_attributes
        )


@dataclass
class RegisteredModelVersionTag(Tag):
    registered_model_name: str
    registered_model_version: str

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        registered_model_name = urllib.parse.quote_plus(
            self.registered_model_name, safe="", encoding=None, errors=None
        )
        registered_model_version = urllib.parse.quote_plus(
            self.registered_model_version, safe="", encoding=None, errors=None
        )
        name = urllib.parse.quote_plus(self.name, safe="", encoding=None, errors=None)

        return qualified_name(
            f"RegisteredModelVersionTag?registered_model_name={registered_model_name}&registered_model_version={registered_model_version}&name={name}"
        )

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("name", self.name),
            ("value", self.value),
            (prov.model.PROV_TYPE, ProvType.REGISTERED_MODEL_VERSION_TAG),
        ]

        return prov.model.ProvEntity(
            document_factory(), self.prov_identifier, prov_attributes
        )


class LifecycleStage(Enum):
    ACTIVE = "active"
    DELETED = "deleted"

    def __str__(self) -> str:
        return self.value

    @staticmethod
    def from_string(stage_str: str) -> LifecycleStage:
        if stage_str in ("active", "Active", "ACTIVE"):
            return LifecycleStage.ACTIVE
        elif stage_str in ("deleted", "Deleted", "DELETED"):
            return LifecycleStage.DELETED
        else:
            raise NotImplementedError

    @staticmethod
    def to_string(stage: LifecycleStage) -> str:
        return str(stage)


class RunStatus(Enum):
    RUNNING = "running"
    SCHEDULED = "scheduled"
    FINISHED = "finished"
    FAILED = "failed"
    KILLED = "killed"

    def __str__(self) -> str:
        return self.value

    @staticmethod
    def from_string(status_str: str) -> RunStatus:
        if status_str in ("running", "Running", "RUNNING"):
            return RunStatus.RUNNING
        elif status_str in ("scheduled", "Scheduled", "SCHEDULED"):
            return RunStatus.SCHEDULED
        elif status_str in ("finished", "Finished", "FINISHED"):
            return RunStatus.FINISHED
        elif status_str in ("failed", "Failed", "FAILED"):
            return RunStatus.FAILED
        elif status_str in ("killed", "Killed", "KILLED"):
            return RunStatus.KILLED
        else:
            raise NotImplementedError

    @staticmethod
    def to_string(status: RunStatus) -> str:
        return str(status)


class SourceType(Enum):
    NOTEBOOK = "Notebook"
    JOB = "Job"
    PROJECT = "Project"
    LOCAL = "Local"
    RECIPE = "Recipe"
    UNKNOWN = "Unknown"

    def __str__(self) -> str:
        return self.value

    @staticmethod
    def from_string(source_str: str) -> SourceType:
        if source_str in ("notebook", "Notebook", "NOTEBOOK"):
            return SourceType.NOTEBOOK
        elif source_str in ("job", "Job", "JOB"):
            return SourceType.JOB
        elif source_str in ("project", "Project", "PROJECT"):
            return SourceType.PROJECT
        elif source_str in ("local", "Local", "LOCAL"):
            return SourceType.LOCAL
        elif source_str in ("recipe", "Recipe", "RECIPE"):
            return SourceType.RECIPE
        elif source_str in ("unknown", "Unknown", "UNKNOWN"):
            return SourceType.UNKNOWN
        else:
            raise NotImplementedError

    @staticmethod
    def to_string(source: SourceType) -> str:
        return str(source)


class RegisteredModelVersionStage(Enum):
    NONE = "None"
    STAGING = "Staging"
    PRODUCTION = "Production"
    ARCHIVED = "Archived"
    DELETED_INTERNAL = "Deleted_Internal"

    def __str__(self) -> str:
        return self.value

    @staticmethod
    def from_string(stage_str: str) -> RegisteredModelVersionStage:
        if stage_str in ("none", "None", "NONE"):
            return RegisteredModelVersionStage.NONE
        elif stage_str in ("staging", "Staging", "STAGING"):
            return RegisteredModelVersionStage.STAGING
        elif stage_str in ("production", "Production", "PRODUCTION"):
            return RegisteredModelVersionStage.PRODUCTION
        elif stage_str in ("archived", "Archived", "ARCHIVED"):
            return RegisteredModelVersionStage.ARCHIVED
        elif stage_str in ("deleted_internal", "Deleted_Internal", "DELETED_INTERNAL"):
            return RegisteredModelVersionStage.DELETED_INTERNAL
        else:
            raise NotImplementedError

    @staticmethod
    def to_string(stage: RegisteredModelVersionStage) -> str:
        return str(stage)
