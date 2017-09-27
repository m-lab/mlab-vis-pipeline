#!/bin/bash

# Run all parts of the pipeline with a provided end date.
# options:
#    -s <YYYY-MM-DD>: start date to run pipeline from.
#    -e <YYYY-MM-DD>: end date to run pipeline to.
#    -m staging|production|sandbox: environment to use
#    -t : to do a test run (doesn't start dataflow)

usage() {
  echo "Usage: KEY_FILE=<path> $0 -s <YYYY-MM-DD> -e <YYYY-MM-DD> -m staging|production|sandbox [-t]" $1 1>&2; exit 1;
}

ENDDATE=""
STARTDATE=""
TEST=0

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
        source ./environments/production.sh
      elif [[ "${OPTARG}" == staging ]]; then
        source ./environments/staging.sh
      elif [[ "${OPTARG}" == sandbox ]]; then
        source ./environments/sandbox.sh
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

if [ -z "${ENDDATE}" ]; then
  usage
fi
if [ -z "${STARTDATE}" ]; then
  usage
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DATAFLOW_DIR="${DIR}/dataflow"
JAR_BASEDIR="${DIR}/dataflow/target"
JAR_FILE="${JAR_BASEDIR}/mlab-vis-pipeline.jar"

if [ ! -f $JAR_FILE ]; then
  echo "JAR File not found at: ${JAR_FILE}"
  exit 1;
fi

echo "Project: ${PROJECT}"
echo "Start date: ${STARTDATE}"
echo "End date: ${ENDDATE}"
echo "STARTING PIPELINE"

echo 'Authenticate service account'
gcloud auth activate-service-account --key-file=${KEY_FILE}

echo "moving into dir: ${DATAFLOW_DIR}"
cd ${DATAFLOW_DIR}

echo "Running historic pipeline for DAY"
GOOGLE_APPLICATION_CREDENTIALS=${KEY_FILE} java -jar ${JAR_FILE} \
  --runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner \
  --timePeriod="day" --project=${PROJECT} --stagingLocation="${STAGING_LOCATION}" \
  --skipNDTRead=0 --startDate=${STARTDATE} --endDate=${ENDDATE} --test=${TEST} \
  --diskSizeGb=30 &


echo "Running historic pipeline for HOUR"
GOOGLE_APPLICATION_CREDENTIALS=${KEY_FILE} java -jar ${JAR_FILE} \
  --runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner \
  --timePeriod="hour" --project=${PROJECT} --stagingLocation="${STAGING_LOCATION}" \
  --skipNDTRead=0 --startDate=${STARTDATE} --endDate=${ENDDATE} --test=${TEST} \
  --diskSizeGb=30 &

# wait for these to complete.
wait

echo "Running Bigtable Transfer Pipeline"
GOOGLE_APPLICATION_CREDENTIALS=${KEY_FILE} java -cp ${JAR_FILE} mlab.dataviz.BigtableTransferPipeline \
  --runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner \
  --project=${PROJECT} --instance=${BIGTABLE_INSTANCE} \
  --stagingLocation="${STAGING_LOCATION}" --test=${TEST} &

wait
echo "Done."
