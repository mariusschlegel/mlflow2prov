# Quickstart Example

This directory contains a ready-to-run ML experiment project that can be used to try out MLflow2PROV. The project is prepared such that all provenance models implemented in MLflow2PROV can be captured and observed in the extraction result.

## Installation

The dependencies can be installed with Poetry as follows:

```bash
poetry install
```

MLflow2PROV uses Graphviz for exporting provenance graphs in the `dot` format. Since Graphviz is not available as a Python package, the installation with the distribution's package manager may be required as follows:

```bash
sudo dnf install graphviz  # exemplary installation in Fedora 38
```

To currently use all features of MLflow2PROV, the application of two minor patches to the MLflow installation is required. You can apply the patches as follows:

```bash
patch .venv/lib/python3.10/site-packages/mlflow/utils/search_utils.py < ../../patches/mlflow-2.3.2-search_utils.patch
patch .venv/lib/python3.10/site-packages/mlflow/store/model_registry/sqlalchemy_store.py < ../../patches/mlflow-2.3.2-sqlalchemy_store.patch
```

Specifically, these patches adjust the `FileStore` and `SQLAlchemyStore` Model Registry backend implementations to also enable reading deleted `ModelVersion` objects. This is especially required to create instances of the `RegisteredModelVersionDeletion` provenance model. The issue has been already reported to the MLflow project (see <https://github.com/mlflow/mlflow/issues/8225>).

The code of the example project is located in a separate GitHub repository ([mariusschlegel/mlflow-example](https://github.com/mariusschlegel/mlflow-example) forked from [mlflow/mlflow-example ](https://github.com/mlflow/mlflow-example)), which is cloned as follows:

```bash
git clone https://github.com/mariusschlegel/mlflow-example project
```

MLflow uses absolute paths within metadata files by design (see <https://github.com/mlflow/mlflow/issues/3144>). This causes all artifact paths to be invalid when copying the `mlruns` directory (e.g., to other machines). To fix this problem and correct the paths to locally valid ones, it is necessary to run a script as follows:

```bash
poetry run python mlflow/fix_artifact_paths.py mlflow/mlruns
```

## Usage

The MLflow tracking server can be started locally with

```bash
mlflow/start_server.sh
```

Next, MLflow2PROV is executed based on the prepared configuration file (`config/config.yaml`):

```bash
poetry run mlflow2prov --config config/config.yaml
```

Finally, the MLflow tracking server is stopped:

```bash
mlflow/stop_server.sh
```

In `examples/integrations/` we show several integrations for further use, processing and querying of the files extracted by MLflow2PROV.
