#!/bin/bash

./build_cluster.sh -m sandbox -d 0 -t 1
docker run -w /mlab-vis-pipeline/dataflow pipeline mvn test