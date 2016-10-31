#!/bin/bash

# Run all parts of the pipeline with a provided end date.
# options: -e : end date to run pipeline to.

usage() { echo "Usage: $0 [-e <YYYY-MM-DD>] " 1>&2; exit 1; }
ENDDATE=""

while getopts ":e:" opt; do
  case $opt in
    e)
      ENDDATE=${OPTARG}
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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JAR_BASEDIR="${DIR}/dataflow/dist"
JAR_FILE="${JAR_BASEDIR}/MLabPipeline.jar"

if [ ! -f $JAR_FILE ]; then
  echo "JAR File not found at: ${JAR_FILE}"
  exit 1;
fi


echo "End date: ${ENDDATE}"
echo "STARTING PIPELINE"

echo "Running historic pipeline for DAY"
java -cp ${JAR_FILE} mlab.bocoup.HistoricPipeline \
  --runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner \
  --timePeriod="day" --project=mlab-oti --stagingLocation="gs://bocoup" \
  --skipNDTRead=0 --endDate=${ENDDATE}

echo "Running historic pipeline for HOUR"
java -cp ${JAR_FILE} mlab.bocoup.HistoricPipeline \
  --runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner \
  --timePeriod="hour" --project=mlab-oti --stagingLocation="gs://bocoup" \
  --skipNDTRead=0 --endDate=${ENDDATE}

echo "Running Bigtable Transfer Pipeline"
java -cp ${JAR_FILE} mlab.bocoup.BigtableTransferPipeline \
  --runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner \
  --project=mlab-oti --stagingLocation="gs://bocoup"
