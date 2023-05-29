#!/usr/bin/env bash

cd "$(dirname "$0")"
source ../.venv/bin/activate
mlflow server &> /dev/null &
deactivate
