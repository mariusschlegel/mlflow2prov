import os
from os.path import dirname
from pathlib import Path

from mlflow2prov.root import get_project_root


class TestRoot:
    def test_get_project_root(self):
        project_root_path = os.path.split(dirname(__file__))[0]
        assert get_project_root() == Path(project_root_path)
