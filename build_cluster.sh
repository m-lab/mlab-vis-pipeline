#!/bin/bash

set -e
set -x

usage() {
  echo "Usage: KEY_FILE=<path> $0 -m staging|production|sandbox -d 1|0 -t 1|0" $1 1>&2; exit 1;
}
source /etc/profile
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PUSH=0
TRAVIS=0

GIT_BRANCH=$(git branch | tail -n 1 | awk '{print $2}')
GIT_COMMIT=$(git log -n 1 | head -n 1 | awk '{print $2}')

while getopts ":m:d:t:" opt; do
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
    t)
      if [[ "${OPTARG}" == 1 ]]; then
        echo "Travis"
        cd $TRAVIS_BUILD_DIR
        TRAVIS=1
        GIT_COMMIT=${TRAVIS_COMMIT}
        GIT_BRANCH=${TRAVIS_BRANCH}
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

CONTAINER_TAG=${GIT_BRANCH}-${GIT_COMMIT}
CONTAINER_NAME="gcr.io/${PROJECT}/vis-pipeline:${CONTAINER_TAG}"

# -- DOCKER

# Build container
echo "Building Docker image"
if [ $TRAVIS == 1 ]; then
  docker build -t pipeline -f Dockerfile .
else
  docker build -t ${PROJECT}/vis-pipeline -f Dockerfile .
fi


if [ $PUSH == 1 ] && [ -n "${KEY_FILE}" ]; then
  gcloud auth activate-service-account --key-file=${KEY_FILE}

  echo "Pushing docker image '${CONTAINER_NAME}' to container registry"

  # the _json_key is required per instructions here:
  # https://cloud.google.com/container-registry/docs/advanced-authentication
  docker login -u _json_key -p "$(cat $KEY_FILE)" https://gcr.io
  docker tag ${PROJECT}/vis-pipeline ${CONTAINER_NAME}
  docker push ${CONTAINER_NAME}

  # update the k8s files. most templates are updated at deploy time
  # except these, because we need to know the specific container name.
  mkdir -p deploy-build/k8s/
  cp templates/k8s/configmap.yaml deploy-build/k8s/
  cp templates/k8s/deployment_bq.yaml deploy-build/k8s/
  cp templates/k8s/deployment_bt.yaml deploy-build/k8s/

  ./travis/substitute_values.sh deploy-build/k8s/ \
    CONTAINER_NAME ${CONTAINER_NAME} \
    BIGTABLE_INSTANCE ${BIGTABLE_INSTANCE} \
    PROJECT ${PROJECT} \
    API_MODE ${API_MODE} \
    BIGTABLE_POOL_SIZE ${BIGTABLE_POOL_SIZE} \
    STAGING_LOCATION ${STAGING_LOCATION} \
    K8_CLUSTER ${K8_CLUSTER} \
    GIT_COMMIT ${GIT_COMMIT} \
    GIT_BRANCH ${GIT_BRANCH}
fi


