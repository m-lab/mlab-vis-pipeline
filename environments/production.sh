#!/bin/bash

###
# Production environment variable setup
# Important: STAGING_LOCATION cannot be set to a root directory of a bucket
###

PROJECT=mlab-oti
BIGTABLE_INSTANCE=mlab-data-viz-prod-2018
API_MODE=production
BIGTABLE_POOL_SIZE=40
STAGING_LOCATION=gs://viz-pipeline-production-2018/staging
K8_CLUSTER=viz-pipeline
