# M-Lab Visualizations - Data Pipeline

This repository contains the required pipelines to transform the ndt.all
table into a series of bigtable tables used by the RESTful API.

There are two main components in this repo:

1. `dataflow` - The pipelines constructed using Google's Dataflow. Written in Java
2. `tools` - Python files for preparing data that we join with (like maxmind,
location data etc.)

# Development

The dataflow pipelines have been developed using Eclipse. You can see more
about the pipelines in the nested README files.

# Running the tools

It is easiest to run the various tools via `make`.
See the `Makefile` for the available commands

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
