## Dataflow Pipelines

### HistoricPipeline

This pipeline:

* Pulls all rows from `ndt.all` for downloads and uploads
* Merges them
* Appends ISP information.

See files under `data/batch-runs/historic` for more information.

To run, execute as a java application with the following arguments:

`--runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner --timePeriod="sample" --project=mlab-oti --stagingLocation="gs://bocoup"`

Notes:
* `timePeriod` can be "day", "hour" or "sample".

### UpdatePipeline

This pipeline:

* Pulls the latest rows from `ndt.all` that we do not yet have in our base tables.
* It merges them
* It appends ISP information to them and adds them to our overall base table.

See files under `data/batch-runs/daily` for more information.

To run, execute as a java application with the following arguments:

`--runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner --timePeriod="hour" --project=mlab-oti --stagingLocation="gs://bocoup"`

Notes:
* `timePeriod` can be "day" or "hour".
* This pipeline doesn't need as many workers as the historic one.

### ExtractUpdateRowsPipeline

This pipeline:

* Pulls the latest rows from `ndt.all` that we do not yet have in our base tables
and puts them into two separate update tables.

It is used by the `UpdatePipeline`.

To run, execute as a java application with the following arguments:

`--runner com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner --timePeriod="hour" -project=mlab-oti --stagingLocation="gs://bocoup"`

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

`--runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner --configfile="./data/batch-runs/historic/sample_download_base.json" --project=mlab-oti --stagingLocation="gs://bocoup"`

### MergeUploadsDownloadPipeline

This pipeline:

* Merges two tables together, upload and download. It uses a sql query to do so,
so it only takes in two table names, rather than a PCollection. As such, it needs
a pipeline to run.

It is used by both the `HistoricPipeline` and `UpdatePipeline`.

To run:

*TODO*: Add runner code here. Just need to test it.

### AddISPsPipeline

This pipeline:

* Adds ISP information to a table. Generally this will run on the merged table
but that isn't required. It can either read from a table or take a PCollection
of rows and run on it.

It is used by both the `HistoricPipeline` and `UpdatePipeline`.

*TODO*: Add runner code here. Just need to test it.
