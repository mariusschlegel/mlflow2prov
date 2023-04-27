import datetime
import random
import urllib.parse

import prov.model

from mlflow2prov.domain.constants import ProvRole, ProvType
from mlflow2prov.domain.model import (
    Artifact,
    Commit,
    Creation,
    Deletion,
    Experiment,
    ExperimentTag,
    File,
    FileRevision,
    LifecycleStage,
    Metric,
    ModelArtifact,
    Param,
    RegisteredModel,
    RegisteredModelTag,
    RegisteredModelVersion,
    RegisteredModelVersionStage,
    RegisteredModelVersionTag,
    Run,
    RunStatus,
    RunTag,
    SourceType,
    User,
)
from mlflow2prov.utils.prov_utils import document_factory, qualified_name
from tests.utils import random_suffix

today = datetime.datetime.now()
yesterday = today - datetime.timedelta(days=1)
next_week = today + datetime.timedelta(days=7)
tomorrow = today + datetime.timedelta(days=1)


class TestUser:
    def test_email_normalization(self):
        name = f"user-name-{random_suffix()}"
        role = f"user-prov-role-{random_suffix()}"
        uppercase = f"user-email-{random_suffix()}".upper()

        user = User(name=name, email=uppercase, prov_role=role)

        assert user.email.islower()

    def test_from_splitable_username(self):
        name = f"user-name-{random_suffix()}"
        email = f"{name}@domain.org"

        assert User.from_splitable_user_string(f"{name} <{email}>") == User(
            name=name, email=email, username=""
        )

        name = f"User Name"
        email = f"user.name@domain.org"

        assert User.from_splitable_user_string(f"{name} <{email}>") == User(
            name=name, email=email, username=""
        )

    def test_prov_identifier(self):
        name: str = f"user-name-{random_suffix()}"
        email: str = f"user-email-{random_suffix()}"

        user = User(
            name=name,
            email=email,
        )

        expected_prov_identifier = qualified_name(
            f"User?{urllib.parse.urlencode([('name', name), ('email', email)])}"
        )

        assert qualified_name(f"User?{name=!s}&{email=!s}") == expected_prov_identifier
        assert user.prov_identifier == expected_prov_identifier

    def test_to_prov(self):
        name = f"user-name-{random_suffix()}"
        email = f"user-email-{random_suffix()}"
        prov_role = ProvRole.AUTHOR

        user = User(
            name=name,
            email=email,
            prov_role=prov_role,
        )

        expected_prov_attributes = [
            ("name", name),
            ("email", email),
            ("username", ""),
            (prov.model.PROV_ROLE, prov_role),
            (prov.model.PROV_TYPE, ProvType.USER),
        ]

        expected_prov_agent = prov.model.ProvAgent(
            document_factory(),
            user.prov_identifier,
            expected_prov_attributes,
        )

        assert user.to_prov() == expected_prov_agent


class TestFile:
    def test_prov_identifier(self):
        name = f"file-name-{random_suffix()}"
        path = f"file-path-{random_suffix()}"
        commit = f"commit-hash-{random_suffix()}"

        file = File(
            name=name,
            path=path,
            commit=commit,
        )

        expected_prov_identifier = qualified_name(
            f"File?{urllib.parse.urlencode([('name', name), ('path', path), ('commit', commit)])}"
        )

        assert (
            qualified_name(f"File?{name=!s}&{path=!s}&{commit=!s}")
            == expected_prov_identifier
        )
        assert file.prov_identifier == expected_prov_identifier

    def test_to_prov(self):
        name = f"file-name-{random_suffix()}"
        path = f"file-path-{random_suffix()}"
        commit = f"commit-hash-{random_suffix()}"

        file = File(name=name, path=path, commit=commit)

        expected_prov_attributes = [
            ("name", name),
            ("path", path),
            ("commit", commit),
            (prov.model.PROV_TYPE, ProvType.FILE),
        ]

        expected_prov_entity = prov.model.ProvEntity(
            document_factory(),
            file.prov_identifier,
            expected_prov_attributes,
        )

        assert file.to_prov() == expected_prov_entity


class TestFileRevision:
    def test_prov_identifier(self):
        name = f"file-name-{random_suffix()}"
        path = f"file-path-{random_suffix()}"
        commit = f"commit-hash-{random_suffix()}"

        status = f"status-{random_suffix()}"

        file_revision = FileRevision(
            name=name,
            path=path,
            commit=commit,
            status=status,
            file=None,
            previous=None,
        )

        expected_prov_identifier = qualified_name(
            f"FileRevision?{urllib.parse.urlencode([('name', name),('path', path), ('commit', commit), ('status', status)])}"
        )

        assert (
            qualified_name(f"FileRevision?{name=!s}&{path=!s}&{commit=!s}&{status=!s}")
            == expected_prov_identifier
        )
        assert file_revision.prov_identifier == expected_prov_identifier

    def test_to_prov(self):
        name = f"file-name-{random_suffix()}"
        path = f"file-path-{random_suffix()}"
        commit = f"commit-hash-{random_suffix()}"

        status = f"status-{random_suffix()}"

        file_revision = FileRevision(
            name=name,
            path=path,
            commit=commit,
            status=status,
            file=None,
            previous=None,
        )

        expected_prov_attributes = [
            ("name", name),
            ("path", path),
            ("status", status),
            (prov.model.PROV_TYPE, ProvType.FILE_REVISION),
        ]

        expected_prov_entity = prov.model.ProvEntity(
            document_factory(),
            file_revision.prov_identifier,
            expected_prov_attributes,
        )

        assert file_revision.to_prov() == expected_prov_entity


class TestCommit:
    def test_prov_identifier(self):
        sha = f"commit-hash-{random_suffix()}"
        message = f"commit-message-{random_suffix()}"
        title = f"commit-title-{random_suffix()}"
        author = None
        committer = None
        parents = []
        authored_at = today
        committed_at = tomorrow

        commit = Commit(
            sha=sha,
            message=message,
            title=title,
            author=author,
            committer=committer,
            parents=parents,
            authored_at=authored_at,
            committed_at=committed_at,
        )

        expected_prov_identifier = qualified_name(
            f"Commit?{urllib.parse.urlencode([('sha', sha)])}"
        )

        assert commit.prov_identifier == expected_prov_identifier

    def test_to_prov(self):
        sha = f"commit-hash-{random_suffix()}"
        message = f"commit-message-{random_suffix()}"
        title = f"commit-title-{random_suffix()}"
        author = None
        committer = None
        parents = []
        authored_at = today
        committed_at = tomorrow

        commit = Commit(
            sha=sha,
            message=message,
            title=title,
            author=author,
            committer=committer,
            parents=parents,
            authored_at=authored_at,
            committed_at=committed_at,
        )

        expected_prov_attributes = [
            ("sha", sha),
            ("title", title),
            ("message", message),
            ("authored_at", authored_at),
            ("committed_at", committed_at),
            (prov.model.PROV_ATTR_STARTTIME, authored_at),
            (prov.model.PROV_ATTR_ENDTIME, committed_at),
            (prov.model.PROV_TYPE, ProvType.COMMIT),
        ]

        expected_prov_activity = prov.model.ProvActivity(
            document_factory(),
            commit.prov_identifier,
            expected_prov_attributes,
        )

        assert commit.to_prov() == expected_prov_activity


class TestExperiment:
    def test_prov_identifier(self):
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

        expected_prov_identifier = qualified_name(
            f"Experiment?{urllib.parse.urlencode([('experiment_id', experiment_id), ('name', name)])}"
        )

        assert experiment.prov_identifier == expected_prov_identifier

    def test_to_prov(self):
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

        expected_prov_attributes = [
            ("experiment_id", experiment_id),
            ("name", name),
            ("artifact_location", artifact_location),
            ("lifecycle_stage", LifecycleStage.to_string(lifecycle_stage)),
            (prov.model.PROV_TYPE, ProvType.EXPERIMENT),
            (prov.model.PROV_TYPE, ProvType.COLLECTION),
        ]

        expected_prov_entity = prov.model.ProvEntity(
            document_factory(),
            experiment.prov_identifier,
            expected_prov_attributes,
        )

        assert experiment.to_prov() == expected_prov_entity


class TestRun:
    def test_prov_identifier(self):
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

        expected_prov_identifier = qualified_name(
            f"Run?{urllib.parse.urlencode([('run_id', run_id), ('name', name)])}"
        )

        assert run.prov_identifier == expected_prov_identifier

    def test_to_prov(self):
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

        expected_prov_attributes = [
            ("run_id", run_id),
            ("name", name),
            ("status", RunStatus.to_string(status)),
            ("lifecycle_stage", LifecycleStage.to_string(lifecycle_stage)),
            ("artifact_uri", artifact_uri),
            ("note", note),
            (prov.model.PROV_TYPE, ProvType.RUN),
            (prov.model.PROV_TYPE, ProvType.COLLECTION),
        ]

        expected_prov_entity = prov.model.ProvEntity(
            document_factory(),
            run.prov_identifier,
            expected_prov_attributes,
        )

        assert run.to_prov() == expected_prov_entity


class TestCreation:
    def test_prov_identifier(self):
        uid = f"creation-uid-{random_suffix()}"
        resource_type = f"creation-resource-type-{random_suffix()}"

        creation = Creation(
            uid=uid, resource_type=resource_type, start_time=today, end_time=today
        )

        expected_prov_identifier = qualified_name(
            f"Creation?{urllib.parse.urlencode([('uid', uid), ('resource_type', resource_type)])}"
        )

        assert creation.prov_identifier == expected_prov_identifier

    def test_to_prov_element(self):
        uid = f"creation-uid-{random_suffix()}"
        resource_type = f"creation-resource-type-{random_suffix()}"
        start_time = today
        end_time = today

        creation = Creation(
            uid=uid,
            resource_type=resource_type,
            start_time=start_time,
            end_time=end_time,
        )

        expected_prov_attributes = [
            ("uid", uid),
            (prov.model.PROV_ATTR_STARTTIME, start_time),
            (prov.model.PROV_ATTR_ENDTIME, end_time),
            (prov.model.PROV_TYPE, ProvType.CREATION),
        ]

        expected_prov_activity = prov.model.ProvActivity(
            document_factory(),
            creation.prov_identifier,
            expected_prov_attributes,
        )

        assert creation.to_prov() == expected_prov_activity

    def test_from_experiment(self):
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

        expected_creation = Creation(
            uid=experiment_id,
            resource_type=ProvType.EXPERIMENT,
            start_time=created_at,
            end_time=last_updated,
        )

        assert Creation.from_experiment(experiment) == expected_creation
        assert experiment.creation == expected_creation

    def test_from_run(self):
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

        expected_creation = Creation(
            uid=run_id,
            resource_type=ProvType.RUN,
            start_time=start_time,
            end_time=end_time,
        )

        assert Creation.from_run(run) == expected_creation
        assert run.creation == expected_creation

    def test_from_registered_model(self):
        name = f"registered-model-name-{random_suffix()}"
        created_at = today
        last_updated_at = today
        description = f"registered-model-description-{random_suffix()}"
        user = User.from_username_str(f"user-name-{random_suffix()}")
        versions = []
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

        expected_creation = Creation(
            uid=registered_model.name,
            resource_type=ProvType.REGISTERED_MODEL,
            start_time=created_at,
            end_time=last_updated_at,
        )

        assert Creation.from_registered_model(registered_model) == expected_creation
        assert registered_model.creation == expected_creation

    def test_from_registered_model_version(self):
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

        expected_creation = Creation(
            uid=f"{registered_model_version.name}-version-{registered_model_version.version}",
            resource_type=ProvType.REGISTERED_MODEL_VERSION,
            start_time=created_at,
            end_time=last_updated_at,
        )

        assert (
            Creation.from_registered_model_version(registered_model_version)
            == expected_creation
        )
        assert registered_model_version.creation == expected_creation


class TestDeletion:
    def test_prov_identifier(self):
        uid = f"deletion-uid-{random_suffix()}"
        resource_type = f"deletion-resource-type-{random_suffix()}"

        deletion = Deletion(
            uid=uid, resource_type=resource_type, start_time=today, end_time=today
        )

        expected_prov_identifier = qualified_name(
            f"Deletion?{urllib.parse.urlencode([('uid', uid), ('resource_type', resource_type)])}"
        )

        assert deletion.prov_identifier == expected_prov_identifier

    def test_to_prov_element(self):
        uid = f"deletion-uid-{random_suffix()}"
        resource_type = f"deletion-resource-type-{random_suffix()}"
        start_time = today
        end_time = today

        deletion = Deletion(
            uid=uid,
            resource_type=resource_type,
            start_time=start_time,
            end_time=end_time,
        )

        expected_prov_attributes = [
            ("uid", uid),
            (prov.model.PROV_ATTR_STARTTIME, start_time),
            (prov.model.PROV_ATTR_ENDTIME, end_time),
            (prov.model.PROV_TYPE, ProvType.DELETION),
        ]

        expected_prov_activity = prov.model.ProvActivity(
            document_factory(),
            deletion.prov_identifier,
            expected_prov_attributes,
        )

        assert deletion.to_prov() == expected_prov_activity

    def test_from_experiment(self):
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

        expected_deletion = Deletion(
            uid=experiment_id,
            resource_type=ProvType.EXPERIMENT,
            start_time=today,
            end_time=today,
        )

        assert Deletion.from_experiment(experiment) == expected_deletion
        assert experiment.deletion == expected_deletion

    def test_from_run(self):
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

        expected_deletion = Deletion(
            uid=run_id, resource_type=ProvType.RUN, start_time=today, end_time=today
        )

        assert Deletion.from_run(run) == expected_deletion
        assert run.deletion == expected_deletion

    def test_from_registered_model_version(self):
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

        expected_deletion = Deletion(
            uid=f"{registered_model_version.name}-version-{registered_model_version.version}",
            resource_type=ProvType.REGISTERED_MODEL_VERSION,
            start_time=created_at,
            end_time=last_updated_at,
        )

        assert (
            Deletion.from_registered_model_version(registered_model_version)
            == expected_deletion
        )

        assert registered_model_version.deletion == expected_deletion


class TestMetric:
    def test_prov_identifier(self):
        run_id = f"run-id-{random_suffix()}"
        name = f"run-name-{random_suffix()}"
        value = random.uniform(0, 10)
        timestamp = today
        step = random.randint(1, 10)

        metric = Metric(
            run_id=run_id, name=name, value=value, timestamp=timestamp, step=step
        )

        expected_prov_identifier = qualified_name(
            f"Metric?{urllib.parse.urlencode([('run_id', run_id), ('name', name), ('step', step)])}"
        )

        assert metric.prov_identifier == expected_prov_identifier

    def test_to_prov(self):
        run_id = f"run-id-{random_suffix()}"
        name = f"run-name-{random_suffix()}"
        value = random.uniform(0, 10)
        timestamp = today
        step = random.randint(1, 10)

        metric = Metric(
            run_id=run_id, name=name, value=value, timestamp=timestamp, step=step
        )

        expected_prov_attributes = [
            ("name", name),
            ("value", value),
            ("timestamp", timestamp),
            ("step", step),
            (prov.model.PROV_TYPE, ProvType.METRIC),
        ]

        expected_prov_entity = prov.model.ProvEntity(
            document_factory(),
            metric.prov_identifier,
            expected_prov_attributes,
        )

        assert metric.to_prov() == expected_prov_entity


class TestParam:
    def test_prov_identifier(self):
        run_id = f"run-id-{random_suffix()}"
        name = f"param-name-{random_suffix()}"
        value = f"param-value-{random_suffix()}"

        param = Param(run_id=run_id, name=name, value=value)

        expected_prov_identifier = qualified_name(
            f"Param?{urllib.parse.urlencode([('run_id', run_id), ('name', name)])}"
        )

        assert param.prov_identifier == expected_prov_identifier

    def test_to_prov(self):
        run_id = f"run-id-{random_suffix()}"
        name = f"param-name-{random_suffix()}"
        value = f"param-value-{random_suffix()}"

        param = Param(run_id=run_id, name=name, value=value)

        expected_prov_attributes = [
            ("name", name),
            ("value", value),
            (prov.model.PROV_TYPE, ProvType.PARAM),
        ]

        expected_prov_entity = prov.model.ProvEntity(
            document_factory(),
            param.prov_identifier,
            expected_prov_attributes,
        )

        assert param.to_prov() == expected_prov_entity


class TestArtifact:
    def test_prov_identifier(self):
        run_id = f"run-id-{random_suffix()}"
        path = f"artifact-path-{random_suffix()}"
        is_dir = bool(random.getrandbits(1))
        file_size = random.randint(1, 1000000)

        artifact = Artifact(
            run_id=run_id, path=path, is_dir=is_dir, file_size=file_size
        )

        expected_prov_identifier = qualified_name(
            f"Artifact?{urllib.parse.urlencode([('run_id', run_id), ('path', path)])}"
        )

        assert artifact.prov_identifier == expected_prov_identifier

    def test_to_prov(self):
        run_id = f"run-id-{random_suffix()}"
        path = f"artifact-path-{random_suffix()}"
        is_dir = bool(random.getrandbits(1))
        file_size = random.randint(1, 1000000)

        artifact = Artifact(
            run_id=run_id, path=path, is_dir=is_dir, file_size=file_size
        )

        expected_prov_attributes = [
            ("path", path),
            ("is_dir", is_dir),
            ("file_size", file_size),
            (prov.model.PROV_TYPE, ProvType.ARTIFACT),
        ]

        expected_prov_entity = prov.model.ProvEntity(
            document_factory(),
            artifact.prov_identifier,
            expected_prov_attributes,
        )

        assert artifact.to_prov() == expected_prov_entity


class TestModelArtifact:
    def test_prov_identifier(self):
        run_id = f"run-id-{random_suffix()}"
        path = f"model-artifact-path-{random_suffix()}"
        is_dir = bool(random.getrandbits(1))
        file_size = random.randint(1, 1000000)

        model_artifact = ModelArtifact(
            run_id=run_id, path=path, is_dir=is_dir, file_size=file_size
        )

        expected_prov_identifier = qualified_name(
            f"ModelArtifact?{urllib.parse.urlencode([('run_id', run_id), ('path', path)])}"
        )

        assert model_artifact.prov_identifier == expected_prov_identifier

    def test_to_prov(self):
        run_id = f"run-id-{random_suffix()}"
        path = f"model-artifact-path-{random_suffix()}"
        is_dir = bool(random.getrandbits(1))
        file_size = random.randint(1, 1000000)

        model_artifact = ModelArtifact(
            run_id=run_id, path=path, is_dir=is_dir, file_size=file_size
        )

        expected_prov_attributes = [
            ("path", path),
            ("is_dir", is_dir),
            ("file_size", file_size),
            (prov.model.PROV_TYPE, ProvType.MODEL_ARTIFACT),
        ]

        expected_prov_entity = prov.model.ProvEntity(
            document_factory(),
            model_artifact.prov_identifier,
            expected_prov_attributes,
        )

        assert model_artifact.to_prov() == expected_prov_entity


class TestRegisteredModel:
    def test_prov_identifier(self):
        name = f"registered-model-name-{random_suffix()}"
        created_at = today
        last_updated_at = today
        description = f"registered-model-description-{random_suffix()}"
        user = User.from_username_str(f"user-name-{random_suffix()}")
        versions = []
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

        expected_prov_identifier = qualified_name(
            f"RegisteredModel?{urllib.parse.urlencode([('name', name)])}"
        )

        assert registered_model.prov_identifier == expected_prov_identifier

    def test_to_prov(self):
        name = f"registered-model-name-{random_suffix()}"
        created_at = today
        last_updated_at = today
        description = f"registered-model-description-{random_suffix()}"
        user = User.from_username_str(f"user-name-{random_suffix()}")
        versions = []
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

        expected_prov_attributes = [
            ("name", name),
            ("created_at", created_at),
            ("last_updated_at", last_updated_at),
            ("description", description),
            (prov.model.PROV_TYPE, ProvType.REGISTERED_MODEL),
            (prov.model.PROV_TYPE, ProvType.COLLECTION),
        ]

        expected_prov_entity = prov.model.ProvEntity(
            document_factory(),
            registered_model.prov_identifier,
            expected_prov_attributes,
        )

        assert registered_model.to_prov() == expected_prov_entity


class TestRegisteredModelVersion:
    def test_prov_identifier(self):
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

        expected_prov_identifier = qualified_name(
            f"RegisteredModelVersion?{urllib.parse.urlencode([('name', name), ('version', version)])}"
        )

        assert registered_model_version.prov_identifier == expected_prov_identifier

    def test_to_prov(self):
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

        expected_prov_attributes = [
            ("name", name),
            ("version", version),
            ("created_at", created_at),
            ("last_updated_at", last_updated_at),
            ("description", description),
            (
                "registered_model_version_stage",
                RegisteredModelVersionStage.to_string(registered_model_version_stage),
            ),
            ("source_path", source_path),
            ("status", status),
            ("status_message", status_message),
            ("run_link", run_link),
            (prov.model.PROV_TYPE, ProvType.REGISTERED_MODEL_VERSION),
            (prov.model.PROV_TYPE, ProvType.COLLECTION),
        ]

        expected_prov_entity = prov.model.ProvEntity(
            document_factory(),
            registered_model_version.prov_identifier,
            expected_prov_attributes,
        )

        assert registered_model_version.to_prov() == expected_prov_entity


class TestExperimentTag:
    def test_prov_identifier(self):
        experiment_id = f"experiment-id-{random_suffix()}"
        name = f"experiment-tag-name-{random_suffix()}"
        value = f"experiment-tag-value-{random_suffix()}"

        experiment_tag = ExperimentTag(
            experiment_id=experiment_id, name=name, value=value
        )

        expected_prov_identifier = qualified_name(
            f"ExperimentTag?{urllib.parse.urlencode([('experiment_id', experiment_id), ('name', name)])}"
        )

        assert experiment_tag.prov_identifier == expected_prov_identifier

    def test_to_prov(self):
        experiment_id = f"experiment-id-{random_suffix()}"
        name = f"experiment-tag-name-{random_suffix()}"
        value = f"experiment-tag-value-{random_suffix()}"

        experiment_tag = ExperimentTag(
            experiment_id=experiment_id, name=name, value=value
        )

        expected_prov_attributes = [
            ("name", name),
            ("value", value),
            (prov.model.PROV_TYPE, ProvType.EXPERIMENT_TAG),
        ]

        expected_prov_entity = prov.model.ProvEntity(
            document_factory(),
            experiment_tag.prov_identifier,
            expected_prov_attributes,
        )

        assert experiment_tag.to_prov() == expected_prov_entity


class TestRunTag:
    def test_prov_identifier(self):
        run_id = f"run-id-{random_suffix()}"
        name = f"run-tag-name-{random_suffix()}"
        value = f"run-tag-value-{random_suffix()}"

        run_tag = RunTag(run_id=run_id, name=name, value=value)

        expected_prov_identifier = qualified_name(
            f"RunTag?{urllib.parse.urlencode([('run_id', run_id), ('name', name)])}"
        )

        assert run_tag.prov_identifier == expected_prov_identifier

    def test_to_prov(self):
        run_id = f"run-id-{random_suffix()}"
        name = f"run-tag-name-{random_suffix()}"
        value = f"run-tag-value-{random_suffix()}"

        run_tag = RunTag(run_id=run_id, name=name, value=value)

        expected_prov_attributes = [
            ("name", name),
            ("value", value),
            (prov.model.PROV_TYPE, ProvType.RUN_TAG),
        ]

        expected_prov_entity = prov.model.ProvEntity(
            document_factory(),
            run_tag.prov_identifier,
            expected_prov_attributes,
        )

        assert run_tag.to_prov() == expected_prov_entity


class TestRegisteredModelTag:
    def test_prov_identifier(self):
        registered_model_name = f"registered-model-name-{random_suffix()}"
        name = f"registered-model-tag-name-{random_suffix()}"
        value = f"registered-model-tag-value-{random_suffix()}"

        registered_model_tag = RegisteredModelTag(
            registered_model_name=registered_model_name, name=name, value=value
        )

        expected_prov_identifier = qualified_name(
            f"RegisteredModelTag?{urllib.parse.urlencode([('registered_model_name', registered_model_name), ('name', name)])}"
        )

        assert registered_model_tag.prov_identifier == expected_prov_identifier

    def test_to_prov(self):
        registered_model_name = f"registered-model-name-{random_suffix()}"
        name = f"registered-model-tag-name-{random_suffix()}"
        value = f"registered-model-tag-value-{random_suffix()}"

        registered_model_tag = RegisteredModelTag(
            registered_model_name=registered_model_name, name=name, value=value
        )

        expected_prov_attributes = [
            ("name", name),
            ("value", value),
            (prov.model.PROV_TYPE, ProvType.REGISTERED_MODEL_TAG),
        ]

        expected_prov_entity = prov.model.ProvEntity(
            document_factory(),
            registered_model_tag.prov_identifier,
            expected_prov_attributes,
        )

        assert registered_model_tag.to_prov() == expected_prov_entity


class TestRegisteredModelVersionTag:
    def test_prov_identifier(self):
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

        expected_prov_identifier = qualified_name(
            f"RegisteredModelVersionTag?{urllib.parse.urlencode([('registered_model_name', registered_model_name), ('registered_model_version', registered_model_version), ('name', name)])}"
        )

        assert registered_model_version_tag.prov_identifier == expected_prov_identifier

    def test_to_prov(self):
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

        expected_prov_attributes = [
            ("name", name),
            ("value", value),
            (prov.model.PROV_TYPE, ProvType.REGISTERED_MODEL_VERSION_TAG),
        ]

        expected_prov_entity = prov.model.ProvEntity(
            document_factory(),
            registered_model_version_tag.prov_identifier,
            expected_prov_attributes,
        )

        assert registered_model_version_tag.to_prov() == expected_prov_entity


class TestLifecycleStage:
    def test_str(self):
        str_stage_active = LifecycleStage.ACTIVE

        assert str(str_stage_active) == "active"
        assert str_stage_active.value == "active"

        str_stage_deleted = LifecycleStage.DELETED

        assert str(str_stage_deleted) == "deleted"
        assert str_stage_deleted.value == "deleted"

    def test_from_string(self):
        str_stage_active_1 = "active"
        str_stage_active_2 = "Active"
        str_stage_active_3 = "ACTIVE"

        assert LifecycleStage.from_string(str_stage_active_1) == LifecycleStage.ACTIVE
        assert LifecycleStage.from_string(str_stage_active_2) == LifecycleStage.ACTIVE
        assert LifecycleStage.from_string(str_stage_active_3) == LifecycleStage.ACTIVE

        str_stage_deleted_1 = "deleted"
        str_stage_deleted_2 = "Deleted"
        str_stage_deleted_3 = "DELETED"

        assert LifecycleStage.from_string(str_stage_deleted_1) == LifecycleStage.DELETED
        assert LifecycleStage.from_string(str_stage_deleted_2) == LifecycleStage.DELETED
        assert LifecycleStage.from_string(str_stage_deleted_3) == LifecycleStage.DELETED

    def test_to_string(self):
        assert LifecycleStage.to_string(LifecycleStage.ACTIVE) == str(
            LifecycleStage.ACTIVE
        )
        assert LifecycleStage.to_string(LifecycleStage.DELETED) == str(
            LifecycleStage.DELETED
        )


class TestRunStatus:
    def test_str(self):
        str_status_running = RunStatus.RUNNING

        assert str(str_status_running) == "running"
        assert str_status_running.value == "running"

        str_status_scheduled = RunStatus.SCHEDULED

        assert str(str_status_scheduled) == "scheduled"
        assert str_status_scheduled.value == "scheduled"

        str_status_finished = RunStatus.FINISHED

        assert str(str_status_finished) == "finished"
        assert str_status_finished.value == "finished"

        str_status_failed = RunStatus.FAILED

        assert str(str_status_failed) == "failed"
        assert str_status_failed.value == "failed"

        str_status_killed = RunStatus.KILLED

        assert str(str_status_killed) == "killed"
        assert str_status_killed.value == "killed"

    def test_from_string(self):
        str_status_running_1 = "running"
        str_status_running_2 = "Running"
        str_status_running_3 = "RUNNING"

        assert RunStatus.from_string(str_status_running_1) == RunStatus.RUNNING
        assert RunStatus.from_string(str_status_running_2) == RunStatus.RUNNING
        assert RunStatus.from_string(str_status_running_3) == RunStatus.RUNNING

        str_status_scheduled_1 = "scheduled"
        str_status_scheduled_2 = "Scheduled"
        str_status_scheduled_3 = "SCHEDULED"

        assert RunStatus.from_string(str_status_scheduled_1) == RunStatus.SCHEDULED
        assert RunStatus.from_string(str_status_scheduled_2) == RunStatus.SCHEDULED
        assert RunStatus.from_string(str_status_scheduled_3) == RunStatus.SCHEDULED

        str_status_finished_1 = "finished"
        str_status_finished_2 = "Finished"
        str_status_finished_3 = "FINISHED"

        assert RunStatus.from_string(str_status_finished_1) == RunStatus.FINISHED
        assert RunStatus.from_string(str_status_finished_2) == RunStatus.FINISHED
        assert RunStatus.from_string(str_status_finished_3) == RunStatus.FINISHED

        str_status_failed_1 = "failed"
        str_status_failed_2 = "Failed"
        str_status_failed_3 = "FAILED"

        assert RunStatus.from_string(str_status_failed_1) == RunStatus.FAILED
        assert RunStatus.from_string(str_status_failed_2) == RunStatus.FAILED
        assert RunStatus.from_string(str_status_failed_3) == RunStatus.FAILED

        str_status_killed_1 = "killed"
        str_status_killed_2 = "Killed"
        str_status_killed_3 = "KILLED"

        assert RunStatus.from_string(str_status_killed_1) == RunStatus.KILLED
        assert RunStatus.from_string(str_status_killed_2) == RunStatus.KILLED
        assert RunStatus.from_string(str_status_killed_3) == RunStatus.KILLED

    def test_to_string(self):
        assert RunStatus.to_string(RunStatus.RUNNING) == str(RunStatus.RUNNING)
        assert RunStatus.to_string(RunStatus.SCHEDULED) == str(RunStatus.SCHEDULED)
        assert RunStatus.to_string(RunStatus.FINISHED) == str(RunStatus.FINISHED)
        assert RunStatus.to_string(RunStatus.FAILED) == str(RunStatus.FAILED)
        assert RunStatus.to_string(RunStatus.KILLED) == str(RunStatus.KILLED)


class TestSourceType:
    def test_str(self):
        str_source_notebook = SourceType.NOTEBOOK

        assert str(str_source_notebook) == "Notebook"
        assert str_source_notebook.value == "Notebook"

        str_source_job = SourceType.JOB

        assert str(str_source_job) == "Job"
        assert str_source_job.value == "Job"

        str_source_project = SourceType.PROJECT

        assert str(str_source_project) == "Project"
        assert str_source_project.value == "Project"

        str_source_local = SourceType.LOCAL

        assert str(str_source_local) == "Local"
        assert str_source_local.value == "Local"

        str_source_recipe = SourceType.RECIPE

        assert str(str_source_recipe) == "Recipe"
        assert str_source_recipe.value == "Recipe"

        str_source_unknown = SourceType.UNKNOWN

        assert str(str_source_unknown) == "Unknown"
        assert str_source_unknown.value == "Unknown"

    def test_from_string(self):
        str_source_notebook_1 = "notebook"
        str_source_notebook_2 = "Notebook"
        str_source_notebook_3 = "NOTEBOOK"

        assert SourceType.from_string(str_source_notebook_1) == SourceType.NOTEBOOK
        assert SourceType.from_string(str_source_notebook_2) == SourceType.NOTEBOOK
        assert SourceType.from_string(str_source_notebook_3) == SourceType.NOTEBOOK

        str_source_job_1 = "job"
        str_source_job_2 = "Job"
        str_source_job_3 = "JOB"

        assert SourceType.from_string(str_source_job_1) == SourceType.JOB
        assert SourceType.from_string(str_source_job_2) == SourceType.JOB
        assert SourceType.from_string(str_source_job_3) == SourceType.JOB

        str_source_project_1 = "project"
        str_source_project_2 = "Project"
        str_source_project_3 = "PROJECT"

        assert SourceType.from_string(str_source_project_1) == SourceType.PROJECT
        assert SourceType.from_string(str_source_project_2) == SourceType.PROJECT
        assert SourceType.from_string(str_source_project_3) == SourceType.PROJECT

        str_source_local_1 = "local"
        str_source_local_2 = "Local"
        str_source_local_3 = "LOCAL"

        assert SourceType.from_string(str_source_local_1) == SourceType.LOCAL
        assert SourceType.from_string(str_source_local_2) == SourceType.LOCAL
        assert SourceType.from_string(str_source_local_3) == SourceType.LOCAL

        str_source_recipe_1 = "recipe"
        str_source_recipe_2 = "Recipe"
        str_source_recipe_3 = "RECIPE"

        assert SourceType.from_string(str_source_recipe_1) == SourceType.RECIPE
        assert SourceType.from_string(str_source_recipe_2) == SourceType.RECIPE
        assert SourceType.from_string(str_source_recipe_3) == SourceType.RECIPE

        str_source_unknown_1 = "unknown"
        str_source_unknown_2 = "Unknown"
        str_source_unknown_3 = "UNKNOWN"

        assert SourceType.from_string(str_source_unknown_1) == SourceType.UNKNOWN
        assert SourceType.from_string(str_source_unknown_2) == SourceType.UNKNOWN
        assert SourceType.from_string(str_source_unknown_3) == SourceType.UNKNOWN

    def test_to_string(self):
        assert SourceType.to_string(SourceType.NOTEBOOK) == str(SourceType.NOTEBOOK)
        assert SourceType.to_string(SourceType.JOB) == str(SourceType.JOB)
        assert SourceType.to_string(SourceType.PROJECT) == str(SourceType.PROJECT)
        assert SourceType.to_string(SourceType.LOCAL) == str(SourceType.LOCAL)
        assert SourceType.to_string(SourceType.RECIPE) == str(SourceType.RECIPE)
        assert SourceType.to_string(SourceType.UNKNOWN) == str(SourceType.UNKNOWN)


class TestRegisteredModelVersionStage:
    def test_str(self):
        str_stage_none = RegisteredModelVersionStage.NONE

        assert str(str_stage_none) == "None"
        assert str_stage_none.value == "None"

        str_stage_staging = RegisteredModelVersionStage.STAGING

        assert str(str_stage_staging) == "Staging"
        assert str_stage_staging.value == "Staging"

        str_stage_production = RegisteredModelVersionStage.PRODUCTION

        assert str(str_stage_production) == "Production"
        assert str_stage_production.value == "Production"

        str_stage_archived = RegisteredModelVersionStage.ARCHIVED

        assert str(str_stage_archived) == "Archived"
        assert str_stage_archived.value == "Archived"

        str_stage_deleted_internal = RegisteredModelVersionStage.DELETED_INTERNAL

        assert str(str_stage_deleted_internal) == "Deleted_Internal"
        assert str_stage_deleted_internal.value == "Deleted_Internal"

    def test_from_string(self):
        str_stage_none_1 = "none"
        str_stage_none_2 = "None"
        str_stage_none_3 = "NONE"

        assert (
            RegisteredModelVersionStage.from_string(str_stage_none_1)
            == RegisteredModelVersionStage.NONE
        )
        assert (
            RegisteredModelVersionStage.from_string(str_stage_none_2)
            == RegisteredModelVersionStage.NONE
        )
        assert (
            RegisteredModelVersionStage.from_string(str_stage_none_3)
            == RegisteredModelVersionStage.NONE
        )

        str_stage_staging_1 = "staging"
        str_stage_staging_2 = "Staging"
        str_stage_staging_3 = "STAGING"

        assert (
            RegisteredModelVersionStage.from_string(str_stage_staging_1)
            == RegisteredModelVersionStage.STAGING
        )
        assert (
            RegisteredModelVersionStage.from_string(str_stage_staging_2)
            == RegisteredModelVersionStage.STAGING
        )
        assert (
            RegisteredModelVersionStage.from_string(str_stage_staging_3)
            == RegisteredModelVersionStage.STAGING
        )

        str_stage_production_1 = "production"
        str_stage_production_2 = "Production"
        str_stage_production_3 = "PRODUCTION"

        assert (
            RegisteredModelVersionStage.from_string(str_stage_production_1)
            == RegisteredModelVersionStage.PRODUCTION
        )
        assert (
            RegisteredModelVersionStage.from_string(str_stage_production_2)
            == RegisteredModelVersionStage.PRODUCTION
        )
        assert (
            RegisteredModelVersionStage.from_string(str_stage_production_3)
            == RegisteredModelVersionStage.PRODUCTION
        )

        str_stage_archived_1 = "archived"
        str_stage_archived_2 = "Archived"
        str_stage_archived_3 = "ARCHIVED"

        assert (
            RegisteredModelVersionStage.from_string(str_stage_archived_1)
            == RegisteredModelVersionStage.ARCHIVED
        )
        assert (
            RegisteredModelVersionStage.from_string(str_stage_archived_2)
            == RegisteredModelVersionStage.ARCHIVED
        )
        assert (
            RegisteredModelVersionStage.from_string(str_stage_archived_3)
            == RegisteredModelVersionStage.ARCHIVED
        )

        str_stage_deleted_internal_1 = "deleted_internal"
        str_stage_deleted_internal_2 = "Deleted_Internal"
        str_stage_deleted_internal_3 = "DELETED_INTERNAL"

        assert (
            RegisteredModelVersionStage.from_string(str_stage_deleted_internal_1)
            == RegisteredModelVersionStage.DELETED_INTERNAL
        )
        assert (
            RegisteredModelVersionStage.from_string(str_stage_deleted_internal_2)
            == RegisteredModelVersionStage.DELETED_INTERNAL
        )
        assert (
            RegisteredModelVersionStage.from_string(str_stage_deleted_internal_3)
            == RegisteredModelVersionStage.DELETED_INTERNAL
        )

    def test_to_string(self):
        assert RegisteredModelVersionStage.to_string(
            RegisteredModelVersionStage.NONE
        ) == str(RegisteredModelVersionStage.NONE)
        assert RegisteredModelVersionStage.to_string(
            RegisteredModelVersionStage.STAGING
        ) == str(RegisteredModelVersionStage.STAGING)
        assert RegisteredModelVersionStage.to_string(
            RegisteredModelVersionStage.PRODUCTION
        ) == str(RegisteredModelVersionStage.PRODUCTION)
        assert RegisteredModelVersionStage.to_string(
            RegisteredModelVersionStage.ARCHIVED
        ) == str(RegisteredModelVersionStage.ARCHIVED)
        assert RegisteredModelVersionStage.to_string(
            RegisteredModelVersionStage.DELETED_INTERNAL
        ) == str(RegisteredModelVersionStage.DELETED_INTERNAL)
