import json
from dataclasses import dataclass, field
from typing import Any

import jsonschema
import jsonschema.exceptions
import ruamel.yaml
import ruamel.yaml.constructor

from mlflow2prov.root import get_project_root


@dataclass
class Config:
    schema: dict[str, Any] = field(init=False)
    content: Any

    def __post_init__(self):
        self.schema = self.get_schema()

    @classmethod
    def read(cls, filepath: str):
        with open(filepath, "rt") as f:
            yaml = ruamel.yaml.YAML(typ="safe")
            return cls(content=yaml.load(f.read()))

    @staticmethod
    def get_schema() -> dict[str, Any]:
        path = get_project_root() / "mlflow2prov" / "config" / "schema.json"

        with open(path, "rt", encoding="utf-8") as f:
            return json.loads(f.read())

    def validate(self) -> tuple[bool, str]:
        try:
            jsonschema.validate(self.content, self.schema)
        except jsonschema.exceptions.ValidationError as err:
            return False, err.message

        return True, "Validation successful, no errors"

    def parse(self) -> list[str]:
        args = []

        for obj in self.content:
            command = list(obj.keys())[0]
            args.append(command)

            options = obj.get(command)
            if not options:
                continue

            for name, literal in options.items():
                if isinstance(literal, bool):
                    args.append(f"--{name}")
                elif isinstance(literal, str):
                    args.append(f"--{name}")
                    args.append(literal)
                elif isinstance(literal, list):
                    for lit in literal:
                        args.append(f"--{name}")
                        args.append(lit)
                else:
                    raise ValueError(f"Unknown literal type: {type(literal)}")

        return args
