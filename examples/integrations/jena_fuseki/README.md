# Apache Jena Fuseki Integration

## Installation

Download and build the prepared Apache Jena Fuseki Docker container as follows:

```bash
git clone https://github.com/mariusschlegel/jena-docker
cd jena-docker
docker build -t jena-fuseki jena-fuseki
```

For the conversion from PROV-N to RDF/XML (required for import into Apache Jena Fuseki, see also [supported formats](https://jena.apache.org/documentation/io/#formats)), we use `provconvert` from the [ProvToolbox](https://lucmoreau.github.io/ProvToolbox/) project. It can be installed as follows:

```bash
wget https://repo1.maven.org/maven2/org/openprovenance/prov/provconvert/0.9.29/provconvert-0.9.29-release.zip
unzip provconvert-0.9.29-release.zip
rm provconvert-0.9.29-release.zip
```

## Usage

First, a `provn` file is converted to RDF/XML as follows:

```bash
ProvToolbox/bin/provconvert -infile /input/mlflow2prov-output.provn -outfile input/mlflow2prov-output.rdfxml
```

The Apache Jena Fuseki Docker container is started as follows:

```bash
./start_jena_fuseki.sh
```

Then, the Web UI can be accessed at [`http://localhost:3030`](http://localhost:3030) (user `admin` and password `admin`). The RDF/XML input file can be imported at [`http://localhost:3030/#/dataset/testdataset/upload`](http://localhost:3030/#/dataset/testdataset/upload). SPARQL queries are possible at [`http://localhost:3030/#/dataset/testdataset/query`](http://localhost:3030/#/dataset/testdataset/query).

The Apache Jena Fuseki Docker container is stopped as follows:

```bash
./stop_jena_fuseki.sh
```
