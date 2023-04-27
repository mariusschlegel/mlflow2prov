# MLflow2PROV

[![Made-with-Python](https://img.shields.io/badge/Made%20with-Python-1f425f.svg)](https://www.python.org) [![W3C-PROV](https://img.shields.io/static/v1?logo=w3c&label=&message=PROV&labelColor=2c2c32&color=007acc&logoColor=007acc?logoWidth=200)](https://www.w3.org/TR/prov-overview/) [![License](https://img.shields.io/badge/license-Apache_2.0-green.svg)](https://opensource.org/licenses/Apache-2.0) [![Coverage](assets/coverage-badge.svg)](README.md) [![Black](https://img.shields.io/badge/code%20style-black-black)](https://github.com/psf/black)

MLflow2PROV is a Python library and command line tool for extracting provenance graphs from ML experiment projects that use Git repositories and MLflow tracking. The underlying data model is compliant with the [W3C PROV](https://www.w3.org/TR/prov-overview/) specification.

## Installation

MLflow2PROV can currently be installed via [Poetry](https://python-poetry.org) (soon also available on [PyPI](https://pypi.org)). For instructions on installing Poetry, please see [here](https://python-poetry.org/docs/#installation).

Some dependencies still require Python 3.10 (for example, see <https://github.com/numba/numba/issues/8304> and <https://github.com/numba/llvmlite/issues/885>). You may be required to install Python 3.10 (using [pyenv](https://github.com/pyenv/pyenv)) and tell Poetry to use this version:

```bash
sudo dnf install -y openssl-devel libffi-devel bzip2-devel readline-devel sqlite-devel xz-devel tk-devel  # examplary installation of Python dependencies in Fedora 38
pyenv install 3.10.11
poetry env use 3.10.11
```

MLflow2PROV uses Graphviz for exporting provenance graphs in the `dot` format. Since Graphviz is not available as a Python package, the installation with the distribution's package manager may be required as follows:

```bash
sudo dnf install graphviz  # exemplary installation in Fedora 38
```

Then, install MLflow2PROV and its dependencies with Poetry:

```bash
poetry install
```

To currently use all features of MLflow2PROV, the application of two minor patches to the MLflow installation is required. You can apply the patches locally as follows:

```bash
patch .venv/lib/python3.10/site-packages/mlflow/utils/search_utils.py < patches/mlflow-2.3.0-search_utils.patch
patch .venv/lib/python3.10/site-packages/mlflow/store/model_registry/sqlalchemy_store.py < patches/mlflow-2.3.0-sqlalchemy_store.patch
```

Specifically, these patches adjust the `FileStore` and `SQLAlchemyStore` Model Registry backend implementations to also enable reading deleted `ModelVersion` objects. This is especially required to create instances of the `RegisteredModelVersionDeletion` provenance model. The issue has been already reported to the MLflow project (see <https://github.com/mlflow/mlflow/issues/8225>).

The dependencies for development can be installed via Poetry's `--with` option:

```bash
poetry install --with dev
```

## Getting Started

The repository [`mlflow2prov-quickstart-example`](https://github.com/mariusschlegel/mlflow2prov-quickstart-example) provides a ready-to-run ML project including a prepared MLflow instance that can be used to try out MLflow2PROV. Please read [`mlflow2prov-quickstart-example/README.md`](https://github.com/mariusschlegel/mlflow2prov-quickstart-example/blob/main/README.md) for detailed instructions.

## Usage

MLflow2PROV can be currently run from within the virtual environment created by Poetry inside the project's root directory via

```bash
poetry run mlflow2prov [OPTIONS] COMMAND1 [ARGS]... [COMMAND2 [ARGS]...]...
```

or

```bash
poetry shell
mflow2prov [OPTIONS] COMMAND1 [ARGS]... [COMMAND2 [ARGS]...]...
```

If the project's MLflow Tracking Server uses HTTP authentication, then it is possible to set the credentials via environment variables as follows:

```bash
poetry shell
export MLFLOW_TRACKING_USERNAME="myusername"
export MLFLOW_TRACKING_PASSWORD="mypassword"
mflow2prov [OPTIONS] COMMAND1 [ARGS]... [COMMAND2 [ARGS]...]...
```

Alternatively, `poetry run` can be used together with a shell script containing the commands listed above.

Further MLflow environment variables can be set analogously (see [documentation](<https://mlflow.org/docs/latest/tracking.html#logging-to-a-tracking-server>)).

The command line interface of MLflow2PROV can be used either used with a chain of commands and options or, alternatively, by providing a configuration file in `.yaml` format.

### Command Line Usage

The command line interface provides commands that can be chained together like a Unix pipeline.

```
Usage: mlflow2prov [OPTIONS] COMMAND1 [ARGS]... [COMMAND2 [ARGS]...]...

  Extract provenance information from ML experiment projects that use Git
  repositories and MLflow tracking.

Options:
  --version        Show the version and exit.
  --verbose        Enable logging to stdout.
  --config FILE    Read configuration from file.
  --validate FILE  Validate configuration file and exit.
  --help           Show this message and exit.

Commands:
  extract     Extract a provenance document from an ML experiment project...
  load        Load provenance documents from one or more file(s).
  merge       Merge one or more given provenance documents into a single...
  save        Save one or more provenance documents to file(s).
  statistics  Print statistics for one or more provenance documents.
  transform   Apply a set of transformations to one or more given...
```

MLflow2PROV can be invoked as follows:

```bash
mlflow2prov extract --repository_path "/home/user/dev/mlproject-foo" --mlflow_url "http://localhost-foo:5000" \
            extract --repository_path "/home/user/dev/mlproject-bar" --mlflow_url "http://localhost-bar:5000" \
            load --input example.rdf                                                                          \
            transform --use_pseudonyms --eliminate_duplicates                                                 \
            merge                                                                                             \
            save --output result --format json --format rdf --format xml --format provn --format dot          \
            statistics --resolution fine --format table
```

### Configuration File Usage

MLflow2PROV supports configuration files in `.yaml` format that are functionally equivalent to command line invocations. To read configuration details from a file instead of specifying on the command line, use the `--config` option:

```bash
mlflow2prov --config config/example.yaml
```

You can validate your configuration file (e.g. to check for syntactical errors) before as follows:

```bash
mlflow2prov --validate config/example.yaml
```

A configuration file functionally equivalent to the [above command line invocation example](#command-line-usage) is specified as follows (see also `config/example.yaml`):

```yaml
- extract:
        repository_path: "/home/user/dev/mlproject-foo"
        mlflow_url: "http://localhost-foo:5000"
- extract:
        repository_path: "/home/user/dev/mlproject-bar"
        mlflow_url: "http://localhost-bar:5000"
- load:
        input: [example.rdf]
- transform:
        use_pseudonyms: true
        eliminate_duplicates: true
- merge:
- save:
        output: result
        format: [json, rdf, xml, provn, dot]
- statistics:
        fine: true
        format: table
```

### Provenance Output Formats

MLflow2PROV supports multiple output formats provided by the [`prov`](https://github.com/trungdong/prov) library:

* [PROV-N](http://www.w3.org/TR/prov-n/)
* [PROV-O](http://www.w3.org/TR/prov-o/) (RDF)
* [PROV-XML](http://www.w3.org/TR/prov-xml/)
* [PROV-JSON](http://www.w3.org/Submission/prov-json/)
* [Graphviz](https://graphviz.org/) (DOT)

## Contributing

Contributions and pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.

Further information on contributing can be found in the document [`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

This project is Apache 2.0 licensed. Copyright © 2023 by Marius Schlegel.
