# Neo4J Integration

## Installation

The Python virtual environment (venv) and the dependencies are installed as follows:

```bash
poetry env use 3.10.11
poetry install
```

For importing a provenance graph extracted by [MLflow2PROV](https://github.com/mariusschlegel/mlflow2prov) into [Neo4J](https://neo4j.com), we use a modified version of [PROV2Neo](https://github.com/DLR-SC/prov2neo)) based on the current `main` branch, in which we fixed an occurring type error regarding JSON parameters of type `DateTime`.

Since `prov2neo` uses the [`py2neo`](https://github.com/py2neo-org/py2neo) client library (now EOL, see <https://github.com/py2neo-org/py2neo/issues/958#issuecomment-1554088585>), Neo4J 4.4 is the latest version that can be used (see <https://github.com/py2neo-org/py2neo#installation--compatibility>). The corresponding Docker container can be pulled as follows:

```bash
docker pull neo4j:4.4
```

## Usage

The Neo4J Docker container is started as follows:

```bash
./start_neo4j.sh
```

The `json` input file can be imported with `prov2neo` as follows:

```bash
poetry run prov2neo connect -a localhost:7474 -u neo4j -p neo4jneo4j -s http import -i input/mlflow2prov-output.json
```

Now it is possible to make queries. The shell script `run_query.sh` allows to execute the predefined queries mounted in the container (see `/queries/` directory). For example, its usage for `q1.cypher` is as follows:

```bash
./run_query.sh q1
```

The Neo4J Docker container is stopped as follows:

```bash
./stop_neo4j.sh
```
