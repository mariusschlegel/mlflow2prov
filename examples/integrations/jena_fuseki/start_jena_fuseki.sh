#!/usr/bin/env bash

cd "$(dirname "$0")"
docker run \
    --rm -d \
    -p 3030:3030 \
    -v $(pwd)/data:/fuseki \
    -v $(pwd)/input:/staging \
    -e ADMIN_PASSWORD=admin \
    -e TDB=2 \
    -e FUSEKI_DATASET_1=testdataset \
    --name fuseki \
    jena-fuseki
