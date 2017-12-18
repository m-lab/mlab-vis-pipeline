# M-Lab Visualizations - Data Pipeline

This repository contains the required pipelines to transform the ndt.all
table into a series of bigtable tables used by the RESTful API.

There are two main components in this repo:

1. `dataflow` - The pipelines constructed using Google's Dataflow. Written in Java
2. `tools` - Python files for preparing data that we join with (like maxmind,
location data etc.)

The pipeline is actually comprised of two separate main pipelines:

1. Bigquery pipeline - responsible for reading from NDT and augmenting the data.
1. Bigtable pipeline - responsible for taking the produced data from the bq
pipeline and transforming it into bigtable tables.

In production, the pipelines run in Kubernetes. See below for more information.

# Running the pipelines

There are three ways to run the pipelines:

1. On your local machine
1. In a docker container on your local machine
1. In a kubernetes cluster

Below we define the ways to do so, and the benefit / drawback of each.

## Prerequisites for ANY pipeline run

1. Before diving into any of these, ensure the configurations in `/environments`
represent the environment you'll be running against. They should by default.

2. You need to have a storage bucket setup. You can do so by calling after
choosing the correct project via `gcloud config set project <mlab-sandbox|mlab-staging...>`

`gsutil mb -c regional -l us-east1 gs://viz-pipeline-{sandbox|staging|production}`

3. You need to create a bigtable instance to write to. You can do so by calling:

```
gcloud beta bigtable instances create viz-pipeline --cluster=viz-pipeline \
    --cluster-zone=us-east1-c \
    --cluster-num-nodes=3 \
    --description="Viz Pipeline Cluster"
```

Note that the instance name should match what's in your `/environment` files.
In this case we are using 'viz-pipeline`, but it can be anything (and has
a different name in production.)

4. You need to create helper tables in biqeury. To do so, run:

```
pip install -r requirements.txt
API_MODE=sandbox|staging|production \
KEY_FILE=<path to cred file> \
make setup_bigquery
```

5. You need to create the final container tables in bigtable. To do so, run:

```
API_MODE=sandbox|staging|production \
KEY_FILE=<path to cred file> \
make setup_bigtable
```

## 1. Running on your Local Machine

In order for you to run the pipeline on your local machine you will need the following
dependencies:

1. Maven 3.3.9 (you can check by running `mvn --version`)
1. Java 1.8.0
1. gcloud

First, you should build your pipeline jar from the source.
You can do so by running `cd /dataflow && mvn package`.
Note that it will also run the tests as part of this run.

Once the jar has been built successfully, you can run either one of the pipelines
as follows:

**Bigquery pipeline**

You can run the bigquery pipeline in two modes:
1. With predefined dates

`KEY_FILE=<path to your service key>./run_bq.sh -m <sandbox|staging|production>`

2. With custom dates

`KEY_FILE=<path to your service key>./run_bq.sh -m <sandbox|staging|production> -s <YYYY-MM-DD> -e <YYYY-MM-DD>`

**Bigtable pipeline**

You can run the bigtable pipeline like so:

`KEY_FILE=<path to your service key>./run_bt.sh -m <sandbox|staging|production>`

Note that the pipelines *do not* exit. They start an http server that runs
for prometheus metric collection. The pipelines will run, then shut down, and
restart after 1 day (bq) and 3 days (bt). You can just exit those scripts with
Ctrl + C in your shell.

## 2. Running in a docker container on your local machine

If you don't wish to setup any dependencies (or you want to test the container)
for deployment purposes, you can build a docker container of the pipeline and
run against it instead.

First, to build the container run:

`docker build -t viz-pipeline .`

Then, to run the container for either pipeline, call it like so:

```
docker run -it \
    -v <path to local service key folder>:/keys \
    -e KEY_FILE=/keys/<key name>.json \
    -e API_MODE=<sandbox|staging|production> pipeline \
    ./run_{bq|bt}.sh -m <sandbox|staging|production>
```

For example, this will run the bigquery pipeline:

```
docker run -it \
     -v /dev/opensource/mlab/mlab-keys:/keys \
     -e KEY_FILE=/keys/sandbox.json \
     -e API_MODE=sandbox pipeline \
     ./run_bq.sh -m sandbox
```

## 3. Running on a kubernetes cluster

In order to run on kubernetes you need to have it setup locally. You can do that
by calling `gcloud components install kubectl`.

**Setup cluster**

1. Setup a new cluster
First, you need to setup a cluster. Make sure that you're running in the
right project by calling `gcloud config set <project name>`, which could be
`mlab-sandbox`, `mlab-staging`, or `mlab-oti`.

Then, run the following script to setup your cluster:

`KEY_FILE=<path to your key file>./setup_cluster.sh -m <sandbox|staging|production>`

This script will setup a cluster, copy over the encrypted service key so it can be
mounted as an key later, and setup the namespace for this cluter.
This will take some time.

Your cluster will need to have the following permissions:

| Name | Permissions |
|------|-------------|
| User info | Disabled |
| Compute Engine | Read Write |
| Storage | Full |
| Task queue | Enabled |
| BigQuery | Enabled |
| Cloud SQL | Disabled |
| Cloud Datastore | Enabled |
| Stackdriver Logging API | Write Only |
| Stackdriver Monitoring API | Full |
| Cloud Platform | Enabled |
| Bigtable Data | Read Write |
| Bigtable Admin | Full |
| Cloud Pub/Sub | Enabled |
| Service Control | Enabled |
| Service Management | Read Write |
| Stackdriver Trace | Write Only |
| Cloud Source Repositories | Disabled |
| Cloud Debugger | Disabled |

2. Use an existing cluster

We also might be sharing a cluster with other services.
If you are, first authenticate with your service key (which should be given
permission to deploy to this cluster):

```
gcloud auth activate-service-account --key-file <path to key file>
```

Then, you will first need to fetch the credentials for that cluster:

`gcloud container clusters get-credentials data-processing-cluster --zone us-central1-a`

Then switch to it before deploying

`gcloud config set container/cluster data-processing-cluster`

**Build cluster**

You can build the cluster container image and required templates by calling:

`KEY_FILE=<path to your key file>./build_cluster.sh -m <sandbox|staging|production> -d 1 -t 0`

This will build and push a docker image to gcr.io that will then be pulled
by kubernetes. It is required to be able to deploy pods.
If you just want to build a local image, specify -d as 0.
The -t parameter is only relevant to travis.

**Deploy to cluster**

First, ensure you've indicated which cluster you're deploying to by calling:

`kubectl config use-context data-processing-cluster`

To deploy (and start) your pipelines on the cluster you can call:

`KEY_FILE=<path to your key file>./deploy_cluster.sh -m <sandbox|staging|production>`

This will deploy your image and startup the pods.

# Testing

You can run the tests locally by calling `make test`.

Alternatively you can run them through Docker, against the same
container that we use to run the pipelines.

`docker build -t pipeline -f Dockerfile .`

And then running the tests by calling

`docker run -it -w /mlab-vis-pipeline/dataflow pipeline mvn test`


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