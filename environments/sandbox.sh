#!/bin/bash

###
# Sandbox environment variable setup
# Important: STAGING_LOCATION cannot be set to a root directory of a bucket
###

BIGTABLE_INSTANCE=viz-testing-apr-2018
PROJECT=mlab-sandbox
API_MODE=sandbox
BIGTABLE_POOL_SIZE=10
STAGING_LOCATION=gs://viz-testing-apr-2018/staging
K8_CLUSTER=viz-cluster
