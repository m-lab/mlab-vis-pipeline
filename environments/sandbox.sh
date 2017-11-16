#!/bin/bash

###
# Sandbox environment variable setup
###

BIGTABLE_INSTANCE=data-api-sandbox-iros-cluster2
PROJECT=mlab-sandbox
API_MODE=sandbox
BIGTABLE_POOL_SIZE=10
STAGING_LOCATION=gs://mlab-data-viz-sandbox
PROMETHEUS=status-mlab-sandbox.measurementlab.net:9091
K8_CLUSTER=viz-pipeline