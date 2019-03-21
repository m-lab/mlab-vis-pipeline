#!/bin/bash

###
# Production environment variable setup
# Important: STAGING_LOCATION cannot be set to a root directory of a bucket
###

PROJECT=mlab-oti
BIGTABLE_INSTANCE=viz-pipeline
API_MODE=production
BIGTABLE_POOL_SIZE=40
STAGING_LOCATION=gs://viz-pipeline-production/production
K8_CLUSTER=viz-pipeline
