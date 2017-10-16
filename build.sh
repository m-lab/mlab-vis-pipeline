#!/bin/bash

usage() {
  echo "Usage: KEY_FILE=<path> $0 -m staging|production|sandbox -d 1|0" $1 1>&2; exit 1;
}
source /etc/profile
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PUSH=0

while getopts ":m:d:" opt; do
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
    d)
      echo "${OPTARG} deploy"
      if [[ "${OPTARG}" == "1" ]]; then
        PUSH=1
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

echo "PROJECT ${PROJECT}"

# -- JARs

# build jar (with tests)
cd dataflow && mvn package && cd ../

# Upload jar, it's too big to send via gcloud deploy
echo "Uploading jar to ${STAGING_LOCATION}"
TIMESTAMP=`date +"%Y%m%d%H%M"`
gsutil cp dataflow/target/mlab-vis-pipeline.jar ${STAGING_LOCATION}/jars/mlab-vis-pipeline-${TIMESTAMP}.jar

# Always latest at mlab-vis-pipeline.jar
gsutil cp ${STAGING_LOCATION}/jars/mlab-vis-pipeline-${TIMESTAMP}.jar ${STAGING_LOCATION}/jars/mlab-vis-pipeline.jar

# Keep only last 3 versions of the jar
MAXVERSIONS=3
JARFILES=`gsutil ls gs://mlab-data-viz-sandbox/jars/mlab-vis-pipeline-*.jar > list.txt`
JARFILECOUNT=`more list.txt | wc -l`
if [[ $((JARFILECOUNT)) > $MAXVERSIONS ]]; then
    echo "Too many jars found on server: $(($JARFILECOUNT + 1)). Deleting last."
    LASTFILE=`more list.txt | sort | head -n1`
    gsutil rm ${LASTFILE}
fi

# -- DOCKER

# Build container
echo "Building Docker image "
docker build -t ${PROJECT}/mlab-vis-pipeline -f PipelineDockerfile .
gcloud auth activate-service-account --key-file=${KEY_FILE}

# Push container to registry
if [[ $PUSH == 1 ]]; then
  echo "Pushing docker image to container registry"
  docker tag ${PROJECT}/mlab-vis-pipeline gcr.io/${PROJECT}/mlab-vis-pipeline:latest
  docker push gcr.io/${PROJECT}/mlab-vis-pipeline:latest
fi
