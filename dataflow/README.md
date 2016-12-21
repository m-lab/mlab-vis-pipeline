## Dataflow Pipelines

Dataflow pipelines for processing NDT data and aggregating into bigtable tables.

Most of the pipelines exist as stand-alone tools, but also are integrated into the
HistoricPipeline or BigTableTransfer pipelines. The stand-alone versions can
be useful in debugging, but are not the primary use case.

### HistoricPipeline

This pipeline:

* Pulls all rows from the raw NDT data table for downloads and uploads.
* Merges them into a single source of upload and download data.
* Augments data with additional information.

See files under `data/batch-runs/historic` for more information.

Example command line execution of pipeline:

```
java -cp pipeline.jar mlab.bocoup.HistoricPipeline \
    --runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner \
    --timePeriod="day" --project=mlab-staging --stagingLocation="gs://mlab-data-viz" --skipNDTRead=0
```

Notes:

* `timePeriod` can be "day", "hour" or "sample".

### BigtableTransferPipeline

This pipeline:

* reads bigtable config files and performs a series of queries on the output of
the HistoricPipeline.
* outputs the aggregations from these queries to a series of bigtable tables

This pipeline is expected to be run after the historic pipeline.

### UpdatePipeline

This pipeline:

* Pulls the latest rows from `ndt.all` that we do not yet have in our base tables.
* It merges them
* It appends ISP information to them and adds them to our overall base table.

See files under `data/batch-runs/daily` for more information.

To run, execute as a java application with the following arguments:

`--runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner --timePeriod="hour" --project=mlab-staging --stagingLocation="gs://mlab-data-viz"`

Notes:
* `timePeriod` can be "day" or "hour".

### ExtractHistoricRowsPipeline

This pipeline:

* Pulls all of the rows from `ndt.all` from a set of time ranges for one
configuration of a historic run.

It is used by the `HistoricPipeline` where it is instantiated twice, once for
downloads and once for uploads. Note that it implements `Runnable` so that it
can run in its own thread.

To run, execute as a java application with the following arguments:

`--runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner --configfile="./data/batch-runs/historic/sample_download_base.json" --project=mlab-staging --stagingLocation="gs://mlab-data-viz"`


### ExtractUpdateRowsPipeline

This pipeline:

* Pulls the latest rows from `ndt.all` that we do not yet have in our base tables
and puts them into two separate update tables.

It is used by the `UpdatePipeline`.

To run, execute as a java application with the following arguments:

`--runner com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner --timePeriod="hour" -project=mlab-staging --stagingLocation="gs://mlab-data-viz"`

Notes:
* `timePeriod` can be "day" or "hour".


### MergeUploadsDownloadPipeline

This pipeline:

* Merges two tables together, upload and download. It uses a sql query to do so,
so it only takes in two table names, rather than a PCollection. As such, it needs
a pipeline to run.

It is used by both the `HistoricPipeline` and `UpdatePipeline`.


### AddISPsPipeline

This pipeline:

* Adds ISP information to a table. Generally this will run on the merged table
but that isn't required. It can either read from a table or take a PCollection
of rows and run on it.

It is used by both the `HistoricPipeline` and `UpdatePipeline`.

### AddLocalTimePipeline

This pipeline:

* Adds local time based on the location of the client.

It is used by both the `HistoricPipeline` and `UpdatePipeline`.

### AddMlabSitesInfoPipeline

This pipeline:

* Adds more information about mlab sites associated with the server entries.

It is used by both the `HistoricPipeline` and `UpdatePipeline`.

### LocationCleaningPipeline

This pipeline:

* Fixes names of a few locations to augment Maxmind's base information.

### MergeASNsPipeline

This pipeline:

* Combines multiple ASNs and provides sane ISP names for a few common ASNs.
