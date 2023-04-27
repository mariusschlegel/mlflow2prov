# Contributing to MLflow2PROV

We welcome community contributions to MLflow2PROV. This document provides useful information about contributing to MLflow2PROV.

## Development

We recommend using [VSCode](https://code.visualstudio.com) together with the [Python extension](https://marketplace.visualstudio.com/items?itemName=ms-python.python) and the provided settings (see `.vscode/settings.json`).

## Testing

The execution of the tests via including the coverage check is performed as follows:

```bash
poetry run pytest
```

## Coverage Badge

The coverage badge used in the `README.md` can be generated as follows:

```bash
poetry run genbadge coverage -i assets/coverage.xml -o assets/coverage-badge.svg
```
