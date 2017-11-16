# M-Lab Visualizations - Data Pipeline

This repository contains the required pipelines to transform the ndt.all
table into a series of bigtable tables used by the RESTful API.

There are two main components in this repo:

1. `dataflow` - The pipelines constructed using Google's Dataflow. Written in Java
2. `tools` - Python files for preparing data that we join with (like maxmind,
location data etc.)


# Development

# Production

## Cluster

Make sure you have a kubernetes cluster for the pipeline.
You can do so by calling

```
gcloud container \
  --project "mlab-sandbox|mlab-staging|mlab-oti" clusters create "mlab-vis-pipeline-jobs" \
  --zone "us-central1-b" \
  --machine-type=n1-standard-8 \
  --scopes "https://www.googleapis.com/auth/cloud-platform" \
  --num-nodes 3 \
  --node-labels=mlab-vis-pipeline-jobs-node=true
```

You may want to adjust the zone to ensure you get a public IP assigned.
Make sure that it is set as your default cluster

`gcloud config set container/cluster mlab-vis-pipeline-jobs`


# Setup Docker image

First, build your docker image and deploy it to the container registry
if you're using gcloud

`KEY_FILE=<keyfile> ./build.sh -m sandbox|staging|production -d 0|1`

d=0 to not push image to container registry, 1 to do so.


# Setting up a new environment

Note that you must have the appropriate service account credentials json file
to access the bigtable instance. Store that file somewhere on your system
outside of this application root.

## Prerequisites

The following are requireents:
- gcloud
- maven
- kubernetis (gcloud components install kubectl)


## Setup Bigtable

The dataflow pipeline will create bigtable tables. In order for the pipeline to
write to them, they first need to exist. In order to create them do the
following:

1. In your google cloud console create a new big table instance. Note the
`instance-id` since it will be required. Update the appropriate `environments/`
file to point to that instance.

2. Create the required tables in the bigtable instance. To do so, from the
root of this app run the following:

```
API_MODE=sandbox|staging|production \
KEY_FILE=<cred file path> \
make setup_bigtable
```

## Setup Bigquery

Some helper tables are necessary for the pipeline. To create them, run:

```
API_MODE=sandbox|staging|production \
KEY_FILE=<path to cred file> \
make setup_bigquery
```

## Build a pipeline jar

In order to build the pipeline jar, you will need to have
[Maven](https://maven.apache.org/) version 3.3.9 installed.
Once you do, run:

`make build`

This actually runs `mvn package` from the dataflow directory.
Tests are automatically run on build.

# Development setup

The dataflow pipelines have been developed using Eclipse. You can see more
about the pipelines in the nested README files.

# Running the pipelines

It is easiest to run the pipelines from eclipse.
You do not need to use the Dataflow running, instead setup the appropriate
arguments via the command line. The pipelines README has more information
about the requirement arguments for each pipeline.

## Running the pipelines remotely

Since some of the pipelines take a very long time to run, it's best to run them
on a remote machine. We use a google cloud instance to do so. Make sure that
the public keys of all required parties have been added via the google console.
Details are explained in a Google Document entitled "Running Pipelines".

# Testing

You can run the tests by calling `make test`.


## Troubleshooting

#### 1. Are you able to connect to bigtable when trying to create initial tables?

Perform a simple test to see if bigtable connection is writable. Run:

```
API_MODE=sandbox|staging|production \
KEY_FILE=<key file path> make test_connection
```

Relies on the detals in the `environments/` files at the root of the
application. Be sure to change them to map to the instance you're pointing at.

#### 2. My kubernetes container is not getting a public IP assigned.

It's likley because there are no more public IPs to go around. Try switching
to another zone.