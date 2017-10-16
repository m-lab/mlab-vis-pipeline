#!/bin/bash

usage() {
  echo "Usage: KEY_FILE=<path> $0 -m staging|production|sandbox" $1 1>&2; exit 1;
}

source /etc/profile
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

DATAFLOW_DIR="${DIR}/dataflow"
JAR_BASEDIR="${DIR}/dataflow/target"
JAR_FILE="${JAR_BASEDIR}/mlab-vis-pipeline.jar"
mkdir -p ${JAR_BASEDIR}

# Check the latest version of the jars available
gcloud auth activate-service-account --key-file=${KEY_FILE}
REMOTE_PIPELINE_JAR=`gsutil ls gs://mlab-data-viz-sandbox/jars/mlab-vis-pipeline-*.jar | sort | tail -n1`
REMOTE_PIPELINE_JAR_FILE=`basename ${REMOTE_PIPELINE_JAR}`

# check the local version of the jar
cd ${JAR_BASEDIR}
LOCAL_PIPELINE_JAR_FILE=`ls | egrep -e 'mlab-vis-pipeline-[0-9]*.jar' | sort | tail -n1`

# Is it newer than ours?
FETCH=0
if [[ -z "${LOCAL_PIPELINE_JAR_FILE// }" ]]; then
    echo "JAR File not found. Fetching it."
    FETCH=1
else
    echo "Jar exists. Checking if its newer"
    REMOTE_VERSION=`echo ${REMOTE_PIPELINE_JAR} | sed -e s/[^0-9]//g`
    LOCAL_VERSION=`echo ${LOCAL_PIPELINE_JAR_FILE} | sed -e s/[^0-9]//g`
    echo "Remote: ${REMOTE_VERSION} Local: ${LOCAL_VERSION}"

    if [ "$LOCAL_VERSION" \< "$REMOTE_VERSION" ]; then
        FETCH=1
    else
        echo "Same version. Not fetching."
    fi
fi

# fetch the latest jar
if [[ $FETCH == 1 ]]; then
    echo "fetching jar ${REMOTE_PIPELINE_JAR_FILE}"
    gsutil cp ${REMOTE_PIPELINE_JAR} .
    cp ${REMOTE_PIPELINE_JAR_FILE} mlab-vis-pipeline.jar
    export PIPELINE_JAR=${PIPELINE_JAR}

    # remove the local file, since we fetched a new one.
    if [[ ! -z "${LOCAL_PIPELINE_JAR_FILE// }" ]]; then
        rm ${LOCAL_PIPELINE_JAR_FILE}
    fi
fi

echo "Done."