#!/bin/bash

###
# Staging environment variable setup
# Important: STAGING_LOCATION cannot be set to a root directory of a bucket
###

PROJECT=mlab-staging
BIGTABLE_INSTANCE=viz-pipeline
API_MODE=staging
BIGTABLE_POOL_SIZE=10
STAGING_LOCATION=gs://viz-pipeline-staging/staging
K8_CLUSTER=data-processing-cluster
