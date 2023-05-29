# Graphviz Integration

The `dot` output of MLflow2PROV can be further processed and visualized using `dot`-compatible tools.

For visualization, a `dot` viewer such as [xdot](https://github.com/jrfonseca/xdot.py) can be used. xdot can be installed via the distribution's package manager

```bash
sudo dnf install python-xdot  # exemplary installation in Fedora 38
```

or by following the instructions in the corresponding [GitHub repository](https://github.com/jrfonseca/xdot.py/blob/master/README.md).

An extracted provenance graph can be viewed as follows:

```bash
xdot input/mlflow2prov-output.dot
```

Alternatively, a `dot` file can be converted to other formats (such as `pdf`, `svg`, or `jpg`) with [Graphviz](https://graphviz.org). Graphviz can be installed via the distribution's package manager as follows:

```bash
sudo dnf install graphviz  # exemplary installation in Fedora 38
```

The conversion from `dot` to `svg`, `pdf`, or `png` can be done as follows:

```bash
dot -Tsvg input/mlflow2prov-output.dot > output/mlflow2prov-output.svg
dot -Tpdf input/mlflow2prov-output.dot > output/mlflow2prov-output.pdf
dot -Tpng input/mlflow2prov-output.dot > output/mlflow2prov-output.png
```
