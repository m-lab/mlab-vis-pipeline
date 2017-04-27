#!/bin/bash

# Run all parts of the pipeline with a provided end date.
# options:
#    -s <YYYY-MM-DD>: start date to run pipeline from.
#    -e <YYYY-MM-DD>: end date to run pipeline to.
#    -t: to do a test run (doesn't start dataflow)

usage() { echo "Usage: $0 -s <YYYY-MM-DD> -e <YYYY-MM-DD> [-t]" 1>&2; exit 1; }
STARTDATE=""
ENDDATE=""
TEST=0

while getopts "s:e:t" opt; do
   case $opt in
    s)
      STARTDATE=${OPTARG}
      ;;
    e)
      ENDDATE=${OPTARG}
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

if [ -z "${ENDDATE}" ] || [ -z "${STARTDATE}" ]; then
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

echo "Start date: ${STARTDATE}"
echo "End date: ${ENDDATE}"
echo "STARTING PIPELINE"

echo "moving into dir: ${DATAFLOW_DIR}"
cd ${DATAFLOW_DIR}

echo "Running historic pipeline for DAY"
java -jar ${JAR_FILE} \
  --runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner \
  --timePeriod="day" --project=mlab-staging --stagingLocation="gs://mlab-data-viz" \
  --skipNDTRead=0 --startDate=${STARTDATE} --endDate=${ENDDATE} --test=${TEST} &

echo "Running historic pipeline for HOUR"
java -jar ${JAR_FILE} \
  --runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner \
  --timePeriod="hour" --project=mlab-staging --stagingLocation="gs://mlab-data-viz" \
  --skipNDTRead=0 --startDate=${STARTDATE} --endDate=${ENDDATE} --test=${TEST} &

# wait for these to complete.
wait

echo "Running Bigtable Transfer Pipeline"
java -cp ${JAR_FILE} mlab.bocoup.BigtableTransferPipeline \
  --runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner \
  --project=mlab-staging --stagingLocation="gs://mlab-data-viz" --test=${TEST} \
  --diskSizeGb=15
