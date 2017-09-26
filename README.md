# M-Lab Visualizations - Data Pipeline

This repository contains the required pipelines to transform the ndt.all
table into a series of bigtable tables used by the RESTful API.

There are two main components in this repo:

1. `dataflow` - The pipelines constructed using Google's Dataflow. Written in Java
2. `tools` - Python files for preparing data that we join with (like maxmind,
location data etc.)


# Setting up a new environment

Note that you must have the appropriate service account credentials json file
to access the bigtable instance. Store that file somewhere on your system
outside of this application root.

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

`make build_jar`


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


# Building the executable JAR

To build the executable JAR, run:

```
mvn package
```

This produces `target/mlab-vis-pipeline.jar` that includes all dependencies inside of it. To run it, you can do something like:

```
java -jar target/mlab-vis-pipeline.jar \
  --runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner \
  --timePeriod="day" \
  --project=mlab-oti \
  --stagingLocation="gs://bocoup" \
  --skipNDTRead=0 \
  --endDate="2016-10-15" \
  --test=1
```

## Troubleshooting

#### 1. Are you able to connect to bigtable when trying to create initial tables?

Perform a simple test to see if bigtable connection is writable. Run:

```
API_MODE=sandbox|staging|production \
KEY_FILE=<key file path> make test_connection
```

Relies on the detals in the `environments/` files at the root of the
application. Be sure to change them to map to the instance you're pointing at.