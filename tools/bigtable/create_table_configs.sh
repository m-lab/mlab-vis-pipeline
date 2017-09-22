#!/bin/bash

USAGE="Optionally pass CONFIG_DIR= to create files in new directory"

set -e
set -x

source "${HOME}/google-cloud-sdk/path.bash.inc"

# Setup config files that dictate what tables to create
CONFIG_DIR=${CONFIG_DIR:-./dataflow/data/bigtable}
mkdir -p ${CONFIG_DIR}
mkdir -p ${CONFIG_DIR}/queries

python -m tools.bigtable.create_bigtable_time_configs --configs ${CONFIG_DIR}
python -m tools.bigtable.create_bigtable_search_configs --configs ${CONFIG_DIR}
python -m tools.bigtable.create_bigtable_premade_configs --configs ${CONFIG_DIR}