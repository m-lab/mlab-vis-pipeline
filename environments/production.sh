#!/bin/bash

###
# Production environment variable setup
###

PROJECT=mlab-oti
BIGTABLE_INSTANCE=mlab-data-viz-prod
API_MODE=production
BIGTABLE_POOL_SIZE=40
STAGING_LOCATION=gs://mlab-data-viz-production
PROMETHEUS=status-mlab-oti.measurementlab.net:9091
K8_CLUSTER=viz-pipeline