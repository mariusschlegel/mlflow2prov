import json
import pathlib
import tempfile

import pytest

from mlflow2prov.config.config import Config
from mlflow2prov.root import get_project_root

expected_schema_data = """
{
    "$schema": "http://json-schema.org/draft-07/schema",
    "type": "array",
    "items": {
        "oneOf": [
            {
                "$ref": "#/definitions/extract"
            },
            {
                "$ref": "#/definitions/load"
            },
            {
                "$ref": "#/definitions/save"
            },
            {
                "$ref": "#/definitions/merge"
            },
            {
                "$ref": "#/definitions/transform"
            },
            {
                "$ref": "#/definitions/statistics"
            }
        ]
    },
    "definitions": {
        "extract": {
            "type": "object",
            "properties": {
                "extract": {
                    "type": "object",
                    "properties": {
                        "repository_path": {
                            "type": "string"
                        },
                        "mlflow_url": {
                            "type": "string"
                        }
                    },
                    "additionalProperties": false,
                    "required": [
                        "repository_path",
                        "mlflow_url"
                    ]
                }
            },
            "additionalProperties": false,
            "required": [
                "extract"
            ]
        },
        "load": {
            "type": "object",
            "properties": {
                "load": {
                    "type": "object",
                    "properties": {
                        "input": {
                            "type": "array",
                            "items": {
                                "type": "string"
                            }
                        }
                    },
                    "additionalProperties": false,
                    "required": [
                        "input"
                    ]
                }
            },
            "additionalProperties": false,
            "required": [
                "load"
            ]
        },
        "save": {
            "type": "object",
            "properties": {
                "save": {
                    "type": "object",
                    "properties": {
                        "output": {
                            "type": "string"
                        },
                        "format": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "enum": [
                                    "json",
                                    "xml",
                                    "rdf",
                                    "provn",
                                    "dot"
                                ]
                            }
                        }
                    },
                    "additionalProperties": false,
                    "required": [
                        "output",
                        "format"
                    ]
                }
            },
            "additionalProperties": false,
            "required": [
                "save"
            ]
        },
        "merge": {
            "type": "object",
            "properties": {
                "merge": {
                    "type": "null"
                }
            },
            "additionalProperties": false,
            "required": [
                "merge"
            ]
        },
        "transform": {
            "type": "object",
            "properties": {
                "transform": {
                    "type": "object",
                    "properties": {
                        "use_pseudonyms": {
                            "type": "boolean"
                        },
                        "eliminate_duplicates": {
                            "type": "boolean"
                        },
                        "merge_aliased_agents": {
                            "type": "boolean"
                        }
                    },
                    "additionalProperties": false
                }
            },
            "additionalProperties": false,
            "required": [
                "transform"
            ]
        },
        "statistics": {
            "type": "object",
            "properties": {
                "statistics": {
                    "type": "object",
                    "properties": {
                        "resolution": {
                            "type": "string",
                            "enum": [
                                "coarse",
                                "fine"
                            ]
                        },
                        "formatter": {
                            "type": "string",
                            "enum": [
                                "table",
                                "csv"
                            ]
                        }
                    },
                    "additionalProperties": false
                }
            },
            "additionalProperties": false,
            "required": [
                "statistics"
            ]
        }
    }
}
"""

expected_config_data = """
- extract:
        repository_path: "/home/user/dev/project_foo/project_foo.git"
        mlflow_url: "http://localhost-foo:5000"
- extract:
        repository_path: "/home/user/dev/project_bar/project_bar.git"
        mlflow_url: "http://localhost-bar:5000"
- load:
        input: [example.rdf]
- save:
        output: result
        format: [json, xml, rdf, provn, dot]
- merge:
- transform:
        use_pseudonyms: true
- statistics:
        resolution: fine
        formatter: table
"""

invalid_config_data = """
- extract:
        mlflow_url: "http://localhost-foo:5000"
- extract:
        repository_path: "/home/user/dev/project_bar/project_bar.git"
- save:
        output: result
"""

expected_parsed_config = [
    "extract",
    "--repository_path",
    "/home/user/dev/project_foo/project_foo.git",
    "--mlflow_url",
    "http://localhost-foo:5000",
    "extract",
    "--repository_path",
    "/home/user/dev/project_bar/project_bar.git",
    "--mlflow_url",
    "http://localhost-bar:5000",
    "load",
    "--input",
    "example.rdf",
    "save",
    "--output",
    "result",
    "--format",
    "json",
    "--format",
    "xml",
    "--format",
    "rdf",
    "--format",
    "provn",
    "--format",
    "dot",
    "merge",
    "transform",
    "--use_pseudonyms",
    "statistics",
    "--resolution",
    "fine",
    "--formatter",
    "table",
]


class TestConfig:
    def test_get_schema(self):
        schema = Config.get_schema()
        expected_schema = json.loads(expected_schema_data)

        assert schema == expected_schema

    def test_read(self):
        path: pathlib.Path = (
            get_project_root() / "tests" / "resources" / "testconfig.yaml"
        )
        config = Config.read(str(path))

        with tempfile.NamedTemporaryFile(mode="r+", encoding="utf-8") as tmpfile:
            tmpfile.write(expected_config_data)
            tmpfile.seek(0)
            expected_config = Config.read(tmpfile.name)

            assert config == expected_config

    def test_validate(self):
        with tempfile.NamedTemporaryFile(mode="r+", encoding="utf-8") as tmpfile:
            tmpfile.write(expected_config_data)
            tmpfile.seek(0)
            config = Config.read(tmpfile.name)

            result = config.validate()

            assert result[0] == True

    def test_validate_if_config_invalid(self):
        with tempfile.NamedTemporaryFile(mode="r+", encoding="utf-8") as tmpfile:
            tmpfile.write(invalid_config_data)
            tmpfile.seek(0)
            config = Config.read(tmpfile.name)

            result = config.validate()

            assert result[0] == False

    def test_parse(self):
        with tempfile.NamedTemporaryFile(mode="r+", encoding="utf-8") as tmpfile:
            tmpfile.write(expected_config_data)
            tmpfile.seek(0)
            config = Config.read(tmpfile.name)

            parsed_config = config.parse()

            assert parsed_config == expected_parsed_config

    def test_parse_if_literal_type_unknown(self):
        test_config = """
        - check:
                date: 2023-03-02
        """

        with tempfile.NamedTemporaryFile(mode="r+", encoding="utf-8") as tmpfile:
            tmpfile.write(test_config)
            tmpfile.seek(0)
            config = Config.read(tmpfile.name)

            with pytest.raises(ValueError):
                config.parse()
