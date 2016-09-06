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

You can run linting on the python code in `tools/` by calling:

`make lint`

# Running the tools

It is easiest to run the various tools via `make`. See the `Makefile` for the 
available commands

# Running the pipelines

It is easiest to run the pipelines from eclipse.
You do not need to use the Dataflow running, instead setup the appropriate
arguments via the commandline. The pipelines README has more information
about the requirement arguments for each pipeline.

## Running the pipelines remotely

Since some of the pipelines take a very long time to run, it's best to run them
on a remote machine. We use a google cloud instance to do so. Make sure that
the public keys of all required parties have been added via the google console.

Here are setup  instructions from provisioning a machine to getting it
ready to run the required pipelines:

```
##
# Using a google compute engine instance
# Running ubuntu
#
# The following will set up a machine to run the necessary
# pipelines.

sudo apt-get --assume-yes install default-jdk
sudo apt-get --assume-yes install git

# add github to remote hosts
ssh-keyscan -t rsa github.com >> ~/.ssh/known_hosts

# SCP your key to this machine!
# scp ~/.ssh/id_rsa <yourUsername>@104.155.237.5:~/.ssh/
# scp ~/.ssh/id_rsa.pub <yourUsername>@104.155.237.5:~/.ssh/
#
# Make sure you can authenticate with github:
# ssh -T git@github.com

# Clone repo
git clone git@github.com:bocoup/mlab-research.git

# Generate the jar for the main class:
# It should be a Runnable jar
# Copy over jar
scp dataflow/dist/HistoricPipeline.jar <yourUsername>@104.155.237.5:~/mlab-research/pipeline/

# To run the jobs:
# cd ~/mlab-research/pipeline/dataflow
# java -cp ../HistoricPipeline.jar mlab.bocoup.HistoricPipeline --runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner --numWorkers=10 --timePeriod="day" --project=mlab-oti --stagingLocation="gs://bocoup" --skipNDTRead=1
```

