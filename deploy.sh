#!/bin/bash
# Options:
# -m environment
# -b to build or not to build (1|0)
# -t to trais or not to travis
USAGE="KEY_FILE=pathto.json $0 -m production|staging|sandbox -b 1|0 -t"

set -e
set -x

source "${HOME}/google-cloud-sdk/path.bash.inc"

# Adapted from the one from ezprompt.net
function git_is_dirty {
    status=`git status 2>&1 | tee`
    dirty=`echo -n "${status}" 2> /dev/null | grep "modified:" &> /dev/null; echo "$?"`
    newfile=`echo -n "${status}" 2> /dev/null | grep "new file:" &> /dev/null; echo "$?"`
    renamed=`echo -n "${status}" 2> /dev/null | grep "renamed:" &> /dev/null; echo "$?"`
    deleted=`echo -n "${status}" 2> /dev/null | grep "deleted:" &> /dev/null; echo "$?"`
    bits=''
    if [ "${renamed}" == "0" ]; then
        bits=">${bits}"
    fi
    if [ "${newfile}" == "0" ]; then
        bits="+${bits}"
    fi
    if [ "${deleted}" == "0" ]; then
        bits="x${bits}"
    fi
    if [ "${dirty}" == "0" ]; then
        bits="!${bits}"
    fi
    [[ -n "${bits}" ]]
}

BUILD=0
GIT_COMMIT=$(git log -n 1 | head -n 1 | awk '{print $2}')
# Initialize correct environment variables based on type of deployment
while getopts ":m:b:" opt; do
  case $opt in
    m)
      echo "${OPTARG} environment"
      if [[ "${OPTARG}" == production ]]; then
        source ./environments/production.sh
        if git_is_dirty ; then
          echo "We won't deploy to production with uncommitted changes"
          exit 1
        fi
      elif [[ "${OPTARG}" == staging ]]; then
        source ./environments/staging.sh
        if git_is_dirty ; then
          echo "We won't deploy to staging with uncommitted changes"
          exit 1
        fi
      elif [[ "${OPTARG}" == sandbox ]]; then
        source ./environments/sandbox.sh
      else
        echo "BAD ARGUMENT TO $0: ${OPTARG}"
        exit 1
      fi
      ;;
    t)
      echo "Travis"
      cd $TRAVIS_BUILD_DIR
      GIT_COMMIT=${TRAVIS_COMMIT}
      ;;
    b)
      echo "Also building jar and docker image"
      if [[ "${OPTARG}" == "1" ]]; then
        BUILD=1
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

if [[ $2 == travis ]]; then
  gcloud auth activate-service-account --key-file ${KEY_FILE}
fi

# remove built files so we do not upload them.
find job_scheduler/ -name '*.pyc' -delete

# Copy service key locally so that we can upload it as part of the deploy.
cp ${KEY_FILE} cred.json

# Copy templates folder for deploy
rm -rf deploy-build
mkdir deploy-build
cp templates/nginx.conf deploy-build/
cp templates/supervisord.conf deploy-build/
cp templates/scheduler_jobs.json deploy-build/
mkdir -p deploy-build/k8s/
cp templates/k8s/deployment.yaml deploy-build/k8s/
cp templates/k8s/service.yaml deploy-build/k8s/

# Build all template files
./travis/substitute_values.sh deploy-build \
    GOOGLE_APPLICATION_CREDENTIALS cred.json \
    KEY_FILE cred.json \
    API_MODE ${API_MODE} \
    PROJECT ${PROJECT} \
    BIGTABLE_INSTANCE ${BIGTABLE_INSTANCE} \
    BIGTABLE_CONFIG_DIR bigtable_configs \
    BIGTABLE_POOL_SIZE ${BIGTABLE_POOL_SIZE}

./travis/substitute_values.sh deploy-build/k8s \
    GOOGLE_APPLICATION_CREDENTIALS cred.json \
    KEY_FILE cred.json \
    API_MODE ${API_MODE} \
    PROJECT ${PROJECT} \
    BIGTABLE_INSTANCE ${BIGTABLE_INSTANCE} \
    BIGTABLE_CONFIG_DIR bigtable_configs \
    BIGTABLE_POOL_SIZE ${BIGTABLE_POOL_SIZE}

# build jar and docker container
if [[ $BUILD == 1 ]]; then
  echo "Building jar and docker image"
  ./build.sh -m ${API_MODE} -d 1
fi

kubectl create -f deploy-build/k8s/deployment.yaml
kubectl create -f deploy-build/k8s/service.yaml

echo "Your service is being created. You might need to wait a few minutes"
echo "to recieve your external IP. You can run\n\nkubectl get service\n"
echo "until you get one assigned. If no IP is being assigned, we are likely"
echo "out of IPs."

# # Remove temporary files
rm cred.json