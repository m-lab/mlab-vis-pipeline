# M-Lab Visualizations - Data Pipeline

This repository contains the required pipelines to transform the ndt.all
table into a series of bigtable tables used by the RESTful API.

There are two main components in this repo:

1. `dataflow` - The pipelines constructed using Google's Dataflow. Written in Java
2. `tools` - Python files for preparing data that we join with (like maxmind,
location data etc.)

The dataflow pipeline (#1 above) is actually comprised of two separate main pipelines:

1. Bigquery pipeline - responsible for reading from NDT and augmenting the data. Kicked off
by `BQRunner.java`.
1. Bigtable pipeline - responsible for taking the produced data from the bq
pipeline and transforming it into bigtable tables. Kicked off by `BTRunner.java`.

In production, the pipelines run in Kubernetes. See below for more information.
By default, the bigquery pipeline runs ever 1 day, and the bigtable pipeline
runs every 3 days. You can change those configurations.

# Running the pipelines

There are three ways to run the pipelines:

1. On your local machine
1. In a docker container on your local machine
1. In a kubernetes cluster

Below we define the ways to do so, and the benefit / drawback of each.

## Prerequisites for ANY pipeline run

1. Before diving into any of these, ensure the configurations in `/environments`
represent the environment you'll be running against. They should by default.
If you plan to deploy to kubernetes, check `templates/k8s/configmap.yaml`. Any
properties that do not change between environments are just set there. There
are defaults for all of them in the code, so they aren't required in other runs
but you can configure them by setting those env variables in other contexts.

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
    -e KEY_FILE=/keys/<key file name>.json \
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

In this example you should have a key file called `/dev/opensource/mlab/mlab-keys/sandbox.json`
on your machine.

## 3. Running on a kubernetes cluster

In order to run on kubernetes you need to have it setup locally. You can do that
by calling `gcloud components install kubectl`.

**Setup cluster**

1. Setup a new cluster
First, you need to setup a cluster. Make sure that you're running in the
right project by calling `gcloud config set <project name>`, which could be
`mlab-sandbox`, `mlab-staging`, or `mlab-oti`.

Create the cluster by hand. Make sure the name of the cluster is documented in
the right `/environments/` file under the `K8_CLUSTER` property.

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

Then, run the following script to setup your cluster:

`KEY_FILE=<path to your key file>./setup_cluster.sh -m <sandbox|staging|production>`

This script will setup an _existing_ cluster by copying over the encrypted service key so it can be
mounted as an key later, and setup the namespace for this cluter.

2. Use an existing cluster

We also might be sharing a cluster with other services.
If you are, first authenticate with your service key (which should be given
permission to deploy to this cluster):

```
gcloud auth activate-service-account --key-file <path to key file>
```

Then, you will first need to fetch the credentials for that cluster:

`gcloud container clusters get-credentials <your cluster name> --zone <your cluster zone>`

For example:

`gcloud container clusters get-credentials data-processing-cluster --zone us-central1-a`

Then switch to it before deploying

`gcloud config set container/cluster data-processing-cluster`

**Build cluster**

You can build the cluster container image and required templates by calling:

`KEY_FILE=<path to your key file>./build_cluster.sh -m <sandbox|staging|production> -d 1 -t 0`

This will build and push a docker image to gcr.io that will then be pulled
by kubernetes. It is required to be able to deploy pods.
If you just want to build a local image, specify -d as 0.

**Deploy to cluster**

First, ensure you've indicated which cluster you're deploying to by calling:

`kubectl config use-context data-processing-cluster`

To deploy (and start) your pipelines on the cluster you can call:

`KEY_FILE=<path to your key file>./deploy_cluster.sh -m <sandbox|staging|production>`

This will deploy your image and startup the pods. The pipelines will execute
immedietly. Note that you might not want to execute both the bigquery and bigtable
pipelines, since the bigtable pipeline relies on content in the bigquery tables.
The Bigtable pipeline will just skip the run if tables aren't present, but you'll
need to wait some number of days for it to try again. It is better to deploy the
bq pipeline, and then after it completes, deploy the bt pipeline. You can monitor
them in the dataflow dashboard on google cloud.

Note that a fresh Bigquery pipeline deploy will wipe the accumulated data and
refresh those tables.
This is to account for the fact that changes in the sourve ETL pipeline may have
invalidated the data in some form. Subsequent runs of the pipeline will be
incremental (though delayed by 48 hours.)

# Testing

You can run the tests locally by calling `make test`.

Alternatively you can run them through Docker, against the same
container that we use to run the pipelines.

`docker build -t pipeline -f Dockerfile .`

And then running the tests by calling

`docker run -it -w /mlab-vis-pipeline/dataflow pipeline mvn test`

# Configuring the pipeline

## In /environments files

* `BIGTABLE_INSTANCE` - Name of bigtable cluster instance (NOT the Kubernetes cluster.)
* `PROJECT` - Name of the project you're working with
* `API_MODE` - Type of deployement. Can be 'sandbox', 'staging' or 'production'
* `BIGTABLE_POOL_SIZE` - Size of the bigtable pool
* `STAGING_LOCATION` - The storage bucket used for temporary files
* `K8_CLUSTER` - Name of the Kubernetes cluster.

## In templates/k8s/configmap.yaml

The configuration options specificed in `/environments` files are passed down
to the configmap which will make them available as env variables on the k8s nodes.

Additional settings that are consistent between environments can be configured
inside the template file itself:

* `NDT_TABLE` - Name of the ndt table or view from which to read the data. If
new table versions are released, just update the string here. Note that it will
need to be inside of backticks "`", for example: "`measurement-lab.public_v3_1.ndt_all`".
* `SLOW_DOWN_NDT_READ_BY_DAYS` - number of days to slow down the NDT read pipeline.
Most recent tests are not always parsed sequentially. We assume that after 2
days (or another value provided by you), entire days will be complete. This
value should be negative and in quotes, for example: "-2".
* `RUN_BQ_UPDATE_EVERY` - how often should the bigquery pipeline run. Default
is: "1" day.
* `DAYS_BEFORE_FULL_BQ_REFRESH` - how often the bq tables should be wiped
clean. The default is every "7" days.
* `RUN_BT_UPDATE_EVERY` - how often should bigtable updates run. The default is
every "3" days.

Be sure not to change the settings in `deploy-build/k8s/configmap.yaml` - that
file gets overwritten by the deployment script.

## Troubleshooting

#### 1. Are you able to connect to bigtable when trying to create initial tables?

Perform a simple test to see if bigtable connection is writable. Run:

```
API_MODE=sandbox|staging|production \
KEY_FILE=<key file path> make test_connection
```

Relies on the detals in the `environments/` files at the root of the
application. Be sure to change them to map to the instance you're pointing at.
