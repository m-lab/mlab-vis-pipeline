# M-Lab Visualizations - Data Pipeline

This repository contains the required pipelines to transform the ndt.all
table into a series of bigtable tables used by the RESTful API.

There are two main components in this repo:

1. `dataflow` - The pipelines constructed using Google's Dataflow. Written in Java
2. `tools` - Python files for preparing data that we join with (like maxmind,
location data etc.)


# Setting up a new environment

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
KEY_FILE=<cred file path> ./tools/bigtable/setup_tables.sh production|staging|sandbox
```

This will create the required config files that describe all the tables that
need to be created, and then create them in bigtable.

Note that you must have the appropriate credentials json file to access
the bigtable instance. Store that file somewhere on your system outside of this
application.

## Setup Bigquery

Some helper tables are necessary for the pipeline.
To run them, run: `make bigquery`.
You will need to ensure you have the appropriate project selected for the
environment you are pushing these tables to.


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
KEY_FILE=../mlab-keys/production.json ./tools/bigtabe/test_connection.sh production|staging|sandbox
```

Relies on the detals in the `environments/` files at the root of the
application. Be sure to change them to map to the instance you're pointing at.