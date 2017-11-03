#!/bin/bash

###
# Staging environment variable setup
###

PROJECT=mlab-staging
BIGTABLE_INSTANCE=mlab-ndt-agg
API_MODE=staging
BIGTABLE_POOL_SIZE=10
STAGING_LOCATION=gs://mlab-data-viz-staging
PROMETHEUS=status-mlab-staging.measurementlab.net:9091
K8_CLUSTER=mlab-vis-pipeline-jobs