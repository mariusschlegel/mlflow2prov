#!/usr/bin/env bash

query=$1
docker exec -it neo4j \
    cypher-shell -u neo4j -p neo4jneo4j --file /queries/$query.cypher
