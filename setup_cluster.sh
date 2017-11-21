#!/bin/bash

usage() {
  echo "Usage: KEY_FILE=<path> $0 -m staging|production|sandbox" $1 1>&2; exit 1;
}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PUSH=0

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

# Create Kubernetes cluster
gcloud container \
  --project ${PROJECT} clusters create ${K8_CLUSTER} \
  --zone "us-central1-b" \
  --machine-type=n1-standard-8 \
  --num-nodes 5 \
  --node-labels=vis-pipeline-jobs-node=true

# Set as active cluster
gcloud config set container/cluster ${K8_CLUSTER}

# Setup build files
cp templates/k8s/secret.yaml deploy-build/k8s/
cp templates/k8s/namespace.yaml deploy-build/k8s/

./travis/substitute_values.sh deploy-build/k8s \
    KEY_CONTENTS `more ${KEY_FILE} | base64`

# Create namespace
kubectl apply -f deploy-build/k8s/namespace.yaml

# Create secret keys
kubectl apply -f deploy-build/k8s/secret.yaml





