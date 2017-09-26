#!/bin/bash

USAGE="Pass CONFIG_DIR if desired. Also pass environment sandbox|staging|production"

set -e
set -x

source "${HOME}/google-cloud-sdk/path.bash.inc"

# Initialize correct environment variables based on type of server being run
if [[ "$1" == production ]]; then
  source ./environments/production.sh
elif [[ "$1" == staging ]]; then
  source ./environments/staging.sh
elif [[ "$1" == sandbox ]]; then
  source ./environments/sandbox.sh
else
  echo "BAD ARGUMENT TO $0"
  exit 1
fi

CONFIG_DIR=${CONFIG_DIR:-./dataflow/data/bigtable}

PROJECT=${PROJECT} \
BIGTABLE_INSTANCE=${BIGTABLE_INSTANCE} \
BIGTABLE_POOL_SIZE=${BIGTABLE_POOL_SIZE} \
python -m tools.bigtable.remove_unused_tables --configs ${CONFIG_DIR}