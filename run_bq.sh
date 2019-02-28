#!/bin/bash

# Run all parts of the pipeline with a provided end date.
# options:
#    -s <YYYY-MM-DD>: start date to run pipeline from.
#    -e <YYYY-MM-DD>: end date to run pipeline to.
#    -m staging|production|sandbox: environment to use
#    -t : to do a test run (doesn't start dataflow)

set -e
set -x
set -u

usage() {
  echo "Usage: KEY_FILE=<path> $0 -s <YYYY-MM-DD> -e <YYYY-MM-DD> -m staging|production|sandbox [-t]" $1 1>&2; exit 1;
}

ENDDATE=""
STARTDATE=""
TEST=0
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

while getopts ":t:e:s:m:" opt; do
  case $opt in
    e)
      ENDDATE=${OPTARG}
      ;;
    s)
      STARTDATE=${OPTARG}
      ;;
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
    t)
      echo "Setting Test to 1" >&2
      TEST=1
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

echo "Project: ${PROJECT}"
echo "Start date: ${STARTDATE}"
echo "End date: ${ENDDATE}"

# echo "moving into dir: ${DATAFLOW_DIR}"
cd ${DATAFLOW_DIR}

ENVIRONMENT_PARAMETERS="--runner=DataflowRunner --project=$PROJECT --stagingLocation=${STAGING_LOCATION} --maxNumWorkers=20 --skipNDTRead=0 --test=${TEST} --diskSizeGb=30"

echo "Starting server for metrics & bigquery pipeline (DAY and HOUR)"

if [ -z "${ENDDATE}" ] && [ -z "${STARTDATE}" ]; then
  # empty start and end dates, going to let auto detection happen
  echo "Empty start and end dates, going to let pipeline determine dates."

  if [ -n "${KEY_FILE}" ]; then
    export GOOGLE_APPLICATION_CREDENTIALS=${KEY_FILE}
    mvn exec:java -Dexec.mainClass=mlab.dataviz.main.BQRunner -Dexec.args="$ENVIRONMENT_PARAMETERS"
  else
    mvn exec:java -Dexec.mainClass=mlab.dataviz.main.BQRunner -Dexec.args="$ENVIRONMENT_PARAMETERS"
  fi

else
  echo "Running on dates ${STARTDATE} - ${ENDDATE}"
  if [ -n "${KEY_FILE}" ]; then
    export GOOGLE_APPLICATION_CREDENTIALS=${KEY_FILE} 
    mvn exec:java -Dexec.mainClass=mlab.dataviz.main.BQRunner -Dexec.args="$ENVIRONMENT_PARAMETERS --startDate=${STARTDATE} --endDate=${ENDDATE}"
  else
    mvn exec:java -Dexec.mainClass=mlab.dataviz.main.BQRunner -Dexec.args="$ENVIRONMENT_PARAMETERS --startDate=${STARTDATE} --endDate=${ENDDATE}"
  fi
fi
