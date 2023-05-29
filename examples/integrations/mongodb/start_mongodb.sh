#!/usr/bin/env bash

cd "$(dirname "$0")"
docker run \
    --rm -d \
    -p 27017:27017 \
    -v mongodb:/data/db \
    -v $(pwd)/input:/input \
    -v $(pwd)/queries:/queries \
    --name mongodb \
    mongo:latest
