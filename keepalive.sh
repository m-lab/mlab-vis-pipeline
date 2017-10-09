#!/bin/bash

usage() {
  echo "Usage: KEY_FILE=<path> $0 -m staging|production|sandbox" $1 1>&2; exit 1;
}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

while getopts ":m:" opt; do
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

if [ -z "${PROMETHEUS}" ]; then
  usage
fi


DATAFLOW_DIR="${DIR}/dataflow"
JAR_BASEDIR="${DIR}/dataflow/target"
JAR_FILE="${JAR_BASEDIR}/mlab-vis-pipeline.jar"

if [ ! -f $JAR_FILE ]; then
  echo "JAR File not found at: ${JAR_FILE}"
  exit 1;
fi

echo "Project: ${PROJECT}"
echo "Keep alive notification to Prometheus"

cd ${DATAFLOW_DIR}

GOOGLE_APPLICATION_CREDENTIALS=${KEY_FILE} java \
    -cp target/mlab-vis-pipeline.jar mlab.dataviz.AlivePing \
    --prometheus=${PROMETHEUS} &

wait

echo "Done"