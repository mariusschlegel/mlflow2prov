import itertools
import logging
import os
import urllib.parse
from dataclasses import dataclass, field
from typing import Iterator

import mlflow
import mlflow.entities.model_registry
import mlflow.store
import mlflow.store.model_registry
import mlflow.utils.mlflow_tags
import requests

from mlflow2prov.domain.model import (
    Artifact,
    Experiment,
    ExperimentTag,
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
    User,
)
from mlflow2prov.utils.time_utils import unix_timestamp_to_datetime

log = logging.getLogger(__name__)


@dataclass
class MLflowFetcher:
    tracking_uri: str | None = None
    mlflow_client: mlflow.MlflowClient = field(init=False)

    def __post_init__(self) -> None:
        if os.environ.get("MLFLOW_TRACKING_URI") is None:
            if self.tracking_uri is None:
                self.tracking_uri = "http://localhost:5000"
                log.warning(
                    f"warning: tracking URI not set, defaulting to http://localhost:5000"
                )
                mlflow.set_tracking_uri(self.tracking_uri)
            else:
                mlflow.set_tracking_uri(self.tracking_uri)
        else:
            self.tracking_uri = str(os.environ.get("MLFLOW_TRACKING_URI"))

        self.mlflow_client = mlflow.MlflowClient(tracking_uri=self.tracking_uri)

        if os.environ.get("MLFLOW_TRACKING_USERNAME") is None:
            if os.environ.get("MLFLOW_TRACKING_PASSWORD") is None:
                if os.environ.get("MLFLOW_TRACKING_TOKEN") is None:
                    log.debug(f"warning: credentials may be missing")

        try:
            requests.get(self.tracking_uri)
        except requests.exceptions.RequestException as e:
            log.warning(
                f"warning: cannot reach tracking server, tracking server may be not available"
            )

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return (
                self.mlflow_client._tracking_client.tracking_uri
                == other.mlflow_client._tracking_client.tracking_uri
                and self.mlflow_client._registry_uri
                == other.mlflow_client._registry_uri
            )
        return False

    def log_error(self, log: logging.Logger, error: Exception, fetch_name: str) -> None:
        log.error(f"failed to fetch {fetch_name} from {self.tracking_uri}")
        log.error(f"error: {error}")

    def fetch_all(
        self,
    ) -> Iterator[Experiment | Run | RegisteredModel]:
        yield from itertools.chain(
            self.fetch_experiments(),
            self.fetch_runs(),
            self.fetch_models(),
        )

    def fetch_experiments(self) -> Iterator[Experiment]:
        for experiment in mlflow.search_experiments(
            view_type=mlflow.entities.ViewType.ALL,
            order_by=["experiment_id ASC"],
        ):
            yield Experiment(
                experiment_id=experiment.experiment_id,
                name=experiment.name,
                user=User.from_username_str(str(experiment.tags.get("mlflow.user")))
                if experiment.tags.get("mlflow.user")
                else None,
                artifact_location=experiment.artifact_location,
                lifecycle_stage=LifecycleStage.from_string(experiment.lifecycle_stage),
                tags=[
                    ExperimentTag(
                        experiment_id=experiment.experiment_id,
                        name=tag_key,
                        value=tag_value,
                    )
                    for tag_key, tag_value in experiment.tags.items()
                ],
                created_at=unix_timestamp_to_datetime(experiment.creation_time),
                last_updated=unix_timestamp_to_datetime(experiment.last_update_time),
            )

    def fetch_runs(self, exps: list[str] | None = None) -> Iterator[Run]:
        run: mlflow.entities.Run
        for run in mlflow.search_runs(  # type: ignore
            [
                experiment.experiment_id
                for experiment in mlflow.search_experiments(
                    view_type=mlflow.entities.ViewType.ALL,
                    order_by=["experiment_id ASC"],
                )
            ]
            if exps is None
            else exps,
            run_view_type=mlflow.entities.ViewType.ALL,
            output_format="list",
            order_by=["start_time ASC"],
        ):
            if run != None:
                yield Run(
                    run_id=run.info.run_id,
                    name=run.info.run_name,
                    experiment_id=run.info.experiment_id,
                    user=User.from_username_str(run.info.user_id)
                    if run.info.user_id
                    else None,
                    status=RunStatus.from_string(run.info.status),
                    start_time=unix_timestamp_to_datetime(run.info.start_time),
                    end_time=unix_timestamp_to_datetime(run.info.end_time),
                    lifecycle_stage=LifecycleStage.from_string(
                        run.info.lifecycle_stage
                    ),
                    artifact_uri=run.info.artifact_uri,
                    metrics=[
                        Metric(
                            run_id=run.info.run_id,
                            name=metric.key,
                            value=metric.value,
                            timestamp=unix_timestamp_to_datetime(metric.timestamp),
                            step=metric.step,
                        )
                        for metric in run.data._metric_objs
                    ],
                    params=[
                        Param(run_id=run.info.run_id, name=param_key, value=param_value)
                        for param_key, param_value in run.data.params.items()
                    ],
                    tags=[
                        RunTag(
                            run_id=run.info.run_id,
                            name=tag_key.strip(),
                            value=tag_value.strip(),
                        )
                        for tag_key, tag_value in run.data.tags.items()
                    ],
                    artifacts=[
                        Artifact(
                            run_id=run.info.run_id,
                            path=artifact.path,
                            is_dir=artifact.is_dir,
                            file_size=artifact.file_size,
                        )
                        for artifact in self.mlflow_client.list_artifacts(
                            run.info.run_id
                        )
                    ],
                    model_artifacts=[
                        ModelArtifact(
                            run_id=run.info.run_id,
                            path=artifact.path,
                            is_dir=artifact.is_dir,
                            file_size=artifact.file_size,
                            artifact=Artifact(
                                run_id=run.info.run_id,
                                path=artifact.path,
                                is_dir=artifact.is_dir,
                                file_size=artifact.file_size,
                            ),
                        )
                        for artifact in self.mlflow_client.list_artifacts(
                            run.info.run_id
                        )
                        for file in os.listdir(
                            os.path.join(
                                urllib.parse.unquote(
                                    urllib.parse.urlparse(
                                        str(run.info.artifact_uri)
                                    ).path
                                ),
                                artifact.path,
                            )
                        )
                        if file.endswith("MLmodel")
                    ],
                    note=run.data.tags.get(
                        mlflow.utils.mlflow_tags.MLFLOW_RUN_NOTE, None
                    ),
                    source_type=run.data.tags.get(
                        mlflow.utils.mlflow_tags.MLFLOW_SOURCE_TYPE, None
                    ),
                    source_name=run.data.tags.get(
                        mlflow.utils.mlflow_tags.MLFLOW_SOURCE_NAME, None
                    ).strip(),
                    source_git_commit=run.data.tags.get(
                        mlflow.utils.mlflow_tags.MLFLOW_GIT_COMMIT, None
                    ),
                    source_git_branch=run.data.tags.get(
                        mlflow.utils.mlflow_tags.MLFLOW_GIT_BRANCH, None
                    ),
                    source_git_repo_url=run.data.tags.get(
                        mlflow.utils.mlflow_tags.MLFLOW_GIT_REPO_URL, None
                    ),
                )

    def fetch_models(
        self,
    ) -> Iterator[RegisteredModel]:
        for registered_model in mlflow.search_registered_models():
            yield RegisteredModel(
                name=registered_model.name,
                created_at=unix_timestamp_to_datetime(
                    registered_model.creation_timestamp
                ),
                last_updated_at=unix_timestamp_to_datetime(
                    registered_model.last_updated_timestamp
                ),
                user=User.from_username_str(
                    str(registered_model.tags.get("mlflow.user"))
                )
                if registered_model.tags.get("mlflow.user")
                else None,
                description=registered_model.description,
                versions=[
                    RegisteredModelVersion(
                        name=registered_model_version.name,
                        version=registered_model_version.version,
                        created_at=unix_timestamp_to_datetime(
                            registered_model_version.creation_timestamp
                        ),
                        last_updated_at=unix_timestamp_to_datetime(
                            registered_model_version.last_updated_timestamp
                        ),
                        description=registered_model_version.description,
                        user=User.from_username_str(
                            str(registered_model_version.user_id)
                        ),
                        registered_model_version_stage=RegisteredModelVersionStage.from_string(
                            str(registered_model_version.current_stage)
                        ),
                        source_path=registered_model_version.source,
                        run_id=registered_model_version.run_id,
                        status=registered_model_version.status,
                        status_message=registered_model_version.status_message,
                        tags=[
                            RegisteredModelVersionTag(
                                registered_model_name=registered_model.name,
                                registered_model_version=registered_model_version.version,
                                name=tag_key,
                                value=tag_value,
                            )
                            for tag_key, tag_value in registered_model_version.tags.items()
                        ],
                        run_link=registered_model_version.run_link,
                    )
                    for registered_model_version in mlflow.search_model_versions()
                ],
                tags=[
                    RegisteredModelTag(
                        registered_model_name=registered_model.name,
                        name=tag_key,
                        value=tag_value,
                    )
                    for tag_key, tag_value in registered_model.tags.items()
                ],
            )
