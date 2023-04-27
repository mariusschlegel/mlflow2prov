import os
import pathlib
import shlex
import subprocess
import zipfile

import pytest

from tests.utils import fix_artifact_paths


@pytest.fixture(scope="session", autouse=True)
def mlflow_context():
    path_str = "tests/resources"
    path_zipfile_str = f"{path_str}/testproject.zip"
    path_extracted_str = f"{path_str}/testproject/mlflow"

    if not os.path.exists(path_extracted_str):
        with zipfile.ZipFile(file=path_zipfile_str, mode="r") as archive:
            archive.extractall(path=path_str)

        path_mlruns_str = str(pathlib.Path(path_extracted_str) / "mlruns")
        fix_artifact_paths(path=path_mlruns_str)

    process = subprocess.Popen(
        args=shlex.split("mlflow server"),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
        cwd=os.path.realpath(path_extracted_str),
        start_new_session=True,
    )

    yield

    process.terminate()

    subprocess.Popen(
        args=shlex.split("pkill -f gunicorn"),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
        cwd=os.path.realpath(path_extracted_str),
        start_new_session=False,
    )
