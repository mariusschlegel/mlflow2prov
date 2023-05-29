#!/usr/bin/env bash

cd "$(dirname "$0")"
docker run \
    --rm -d \
    -p 7474:7474/tcp \
    -p 7687:7687/tcp \
    -v $(pwd)/data:/data \
    -v $(pwd)/queries:/queries \
    -e NEO4J_AUTH=neo4j/neo4jneo4j \
    --name neo4j \
    neo4j:4.4
