#!/bin/bash

USAGE="Optionally pass CONFIG_DIR= to create files in new directory"

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

# Setup config files that dictate what tables to create
CONFIG_DIR=${CONFIG_DIR:-./dataflow/data/bigtable}
CONFIG_FILES=${CONFIG_DIR}/*.json

for f in $CONFIG_FILES
do
    tablename=$(basename $f | cut -d"." -f1)
    echo "Processing $tablename"
    GOOGLE_APPLICATION_CREDENTIALS=${KEY_FILE} cbt \
    --project ${PROJECT} \
    --instance ${BIGTABLE_INSTANCE} \
    setgcpolicy ${tablename} data maxversions=1

    GOOGLE_APPLICATION_CREDENTIALS=${KEY_FILE} cbt \
    --project ${PROJECT} \
    --instance ${BIGTABLE_INSTANCE} \
    setgcpolicy ${tablename} meta maxversions=1
done



# GOOGLE_APPLICATION_CREDENTIALS=../mlab-keys/sandbox.json cbt \
# --project mlab-sandbox \
# --instance data-api-sandbox-iros-cluster \
# setgcpolicy client_asn_by_day_hour meta maxversions=1




# server_asn_search