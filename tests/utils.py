import itertools
import pathlib
import uuid

import yaml

from mlflow2prov.domain import model
from mlflow2prov.domain.constants import ProvRole


def random_suffix():
    return uuid.uuid4().hex[:6]


def random_user():
    return model.User(
        name=f"user-name-{random_suffix()}",
        email=f"user-email-{random_suffix()}",
        prov_role=ProvRole.AUTHOR,
    )


def rewrite_artifact_path(
    metadata_file: pathlib.Path,
    artifact_path_key: str,
    artifact_path: pathlib.Path,
):
    with open(metadata_file, "r") as f:
        y = yaml.safe_load(f)
        y[artifact_path_key] = f"file://{artifact_path}"

    with open(metadata_file, "w") as f:
        yaml.dump(y, f, default_flow_style=False, sort_keys=False)


def fix_artifact_paths(path: str):
    absolute_path = pathlib.Path(path).resolve()
    absolute_path_trash = absolute_path / ".trash"

    for experiment_folder in itertools.chain(
        # experiments in "mlruns/"
        filter(
            lambda el: not el.is_file() and el.name != "trash" and el.name != "models",
            absolute_path.iterdir(),
        ),
        # experiments in "mlruns/.trash/"
        filter(lambda el: not el.is_file(), absolute_path_trash.iterdir()),
    ):
        metadata_file = experiment_folder / "meta.yaml"

        # fix experiment metadata
        if metadata_file.exists():
            rewrite_artifact_path(
                metadata_file=metadata_file,
                artifact_path_key="artifact_location",
                artifact_path=experiment_folder,
            )

            for run_folder in filter(
                lambda el: not el.is_file() and el.name != "tags",
                experiment_folder.iterdir(),
            ):

                metadata_file = run_folder / "meta.yaml"

                # fix run metadata
                if metadata_file.exists():
                    rewrite_artifact_path(
                        metadata_file=metadata_file,
                        artifact_path_key="artifact_uri",
                        artifact_path=run_folder / "artifacts",
                    )
