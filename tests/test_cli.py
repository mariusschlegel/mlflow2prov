import logging
import tempfile

import pytest
from click.testing import CliRunner

from mlflow2prov.entrypoints.cli import cli
from mlflow2prov.log import LOG_FORMAT, LOG_LEVEL
from tests.test_config import expected_config_data, invalid_config_data
from tests.test_git_fetcher import path_testproject_git_repo


class TestCli:
    def test_enable_logging(self):
        log = logging.getLogger()

        assert not log.level == LOG_LEVEL
        assert not log.handlers[0].formatter._fmt == LOG_FORMAT  # type:ignore

        runner = CliRunner()
        result = runner.invoke(cli, ["--verbose"])

        assert log.level == LOG_LEVEL
        assert log.handlers[0].formatter._fmt == LOG_FORMAT  # type:ignore

    def test_load_config(self):
        test_config_data = f"""
            # yaml-language-server: $schema=../mlflow2prov/config/schema.json
            - extract:
                    repository_path: "{path_testproject_git_repo}"
                    mlflow_url: "http://localhost:5000"
            """

        runner = CliRunner()

        with tempfile.NamedTemporaryFile(mode="r+", encoding="utf-8") as tmpfile:
            tmpfile.write(test_config_data)
            tmpfile.seek(0)

            result = runner.invoke(cli, ["--config", tmpfile.name])

            assert result.exit_code == 0

    def test_load_config_if_validation_failed(self):
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(mode="r+", encoding="utf-8") as tmpfile:
            tmpfile.write(invalid_config_data)
            tmpfile.seek(0)

            result = runner.invoke(cli, ["--config", tmpfile.name])

            assert result.exit_code == 2
            assert "Validation failed" in result.output

    def test_validate_config(self):
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(mode="r+", encoding="utf-8") as tmpfile:
            tmpfile.write(expected_config_data)
            tmpfile.seek(0)

            result = runner.invoke(cli, ["--validate", tmpfile.name])

            assert result.output.splitlines()[0] == "Validation successful"

    def test_validate_config_if_config_invalid(self):
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(mode="r+", encoding="utf-8") as tmpfile:
            tmpfile.write(invalid_config_data)
            tmpfile.seek(0)

            result = runner.invoke(cli, ["--validate", tmpfile.name])

            assert "Validation failed" in result.output

    @pytest.mark.xdist_group(name="group1")
    def test_extract(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
                "--repository_path",
                f"{path_testproject_git_repo}",
                "--mlflow_url",
                "http://localhost:5000",
            ],
        )

        assert result.exit_code == 0

    @pytest.mark.xdist_group(name="group1")
    def test_load(self):
        content_xml = '<?xml version="1.0" encoding="ASCII"?>\n<prov:document xmlns:prov="http://www.w3.org/ns/prov#" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"/>'

        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".xml",
        ) as tmpfile:
            tmpfile.write(content_xml)
            tmpfile.seek(0)

            runner = CliRunner()
            result = runner.invoke(
                cli,
                ["load", "--input", tmpfile.name],
            )

            assert result.exit_code == 0

    @pytest.mark.xdist_group(name="group1")
    def test_save(self):
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".json",
        ) as tmpfile:
            runner = CliRunner()
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "--repository_path",
                    f"{path_testproject_git_repo}",
                    "--mlflow_url",
                    "http://localhost:5000",
                    "save",
                    "--format",
                    "json",
                    "--output",
                    tmpfile.name,
                ],
            )

            assert result.exit_code == 0

    @pytest.mark.xdist_group(name="group1")
    def test_merge(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
                "--repository_path",
                f"{path_testproject_git_repo}",
                "--mlflow_url",
                "http://localhost:5000",
                "extract",
                "--repository_path",
                f"{path_testproject_git_repo}",
                "--mlflow_url",
                "http://localhost:5000",
                "merge",
                "transform",
                "--eliminate_duplicates",
            ],
        )

        assert result.exit_code == 0

    @pytest.mark.xdist_group(name="group1")
    def test_transform(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
                "--repository_path",
                f"{path_testproject_git_repo}",
                "--mlflow_url",
                "http://localhost:5000",
                "transform",
                "--use_pseudonyms",
                "--eliminate_duplicates",
            ],
        )

        assert result.exit_code == 0

    @pytest.mark.xdist_group(name="group2")
    def test_statistics(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
                "--repository_path",
                f"{path_testproject_git_repo}",
                "--mlflow_url",
                "http://localhost:5000",
                "statistics",
                "--resolution",
                "coarse",
                "--format",
                "table",
            ],
        )

        assert result.exit_code == 0
