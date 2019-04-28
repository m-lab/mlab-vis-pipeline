#!/bin/bash

# Run all parts of the pipeline with a provided end date.
# options:
#    -m staging|production|sandbox: environment to use

set -e
set -x

usage() {
  echo "Usage: KEY_FILE=<path> $0 -m staging|production|sandbox" $1 1>&2; exit 1;
}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

while getopts ":t:e:s:m:" opt; do
  case $opt in
    m)
      echo "${OPTARG} environment"
      if [[ "${OPTARG}" == production ]]; then
        source $DIR/environments/production.sh
      elif [[ "${OPTARG}" == staging ]]; then
        source $DIR/environments/staging.sh
      elif [[ "${OPTARG}" == sandbox ]]; then
        source $DIR/environments/sandbox.sh
      else
        echo "BAD ARGUMENT TO $0: ${OPTARG}"
        exit 1
      fi
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done

DATAFLOW_DIR="${DIR}/dataflow"

echo "Project: ${PROJECT}"

# echo "moving into dir: ${DATAFLOW_DIR}"
cd ${DATAFLOW_DIR}

echo "Starting server for bigquery metrics & bigtable pipeline"

ENVIRONMENT_PARAMETERS="--runner=DataflowRunner --project=$PROJECT --instance=${BIGTABLE_INSTANCE} --stagingLocation=${STAGING_LOCATION} --maxNumWorkers=150"

if [ -n "${KEY_FILE}" ]; then
  export GOOGLE_APPLICATION_CREDENTIALS=${KEY_FILE}
  mvn exec:java -Dexec.mainClass=mlab.dataviz.main.BTRunner -Dexec.args="$ENVIRONMENT_PARAMETERS"
else
  mvn exec:java -Dexec.mainClass=mlab.dataviz.main.BTRunner -Dexec.args="$ENVIRONMENT_PARAMETERS"
fi

