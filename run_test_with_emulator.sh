#!/bin/bash

# This file should only be run *inside* a container. It starts up a datastore
# emulator, runs the local tests, and never shuts down the datastore.  If it
# exits successfully, then all the unit tests ran successfully.

set -e
set -x

source /root/google-cloud-sdk/path.bash.inc
gcloud config set project mlab-sandbox
gcloud beta emulators datastore start --consistency=1.0 --no-store-on-disk
$(gcloud beta emulators datastore env-init)
env
mvn test