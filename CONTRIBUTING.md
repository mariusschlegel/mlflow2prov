# Contributing to MLflow2PROV

We welcome community contributions to MLflow2PROV. This document provides useful information about contributing to MLflow2PROV.

## Development

We recommend using [VSCode](https://code.visualstudio.com) together with the [Python extension](https://marketplace.visualstudio.com/items?itemName=ms-python.python) and the provided settings (see `.vscode/settings.json`).

## Testing

The execution of the tests via including the coverage check is performed as follows:

```bash
poetry run pytest
```

## Release

### Coverage Badge

The coverage badge used in the `README.md` can be generated as follows:

```bash
poetry run genbadge coverage -i docs/assets/coverage.xml -o docs/assets/coverage-badge.svg
```

### Patch Creation

```bash
version="2.3.2"
sed 's/model_versions = \[mv for mv in model_versions if mv.current_stage != STAGE_DELETED_INTERNAL\]/model_versions = \[mv for mv in model_versions\]/g' .venv/lib/python3.10/site-packages/mlflow/utils/search_utils.py | diff -u .venv/lib/python3.10/site-packages/mlflow/utils/search_utils.py - > patches/mlflow-$version-search_utils.patch
sed '/.filter(SqlModelVersion.current_stage != STAGE_DELETED_INTERNAL)/d' .venv/lib/python3.10/site-packages/mlflow/store/model_registry/sqlalchemy_store.py | diff -u .venv/lib/python3.10/site-packages/mlflow/store/model_registry/sqlalchemy_store.py - > patches/mlflow-$version-sqlalchemy_store.patch
```
