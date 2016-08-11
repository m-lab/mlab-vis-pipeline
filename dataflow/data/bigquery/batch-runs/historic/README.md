## Historic pipeline runs

These runs are meant to allow us to run a historic update on all of the
ndt.all rows through a given set of dates. This is a pretty long running
operation. See the `HistoricPipeline` for more details.

Note that we separate downloads and uploads, as well as day and hour based
aggregates, leading to there being 4 different combinations.

There are also sample configurations here that will let you test the pipeline
for a short time range.

### Required parameters:

* `queryFile` - The query file that will pull rows from ndt. This file
will likely require date ranges.
* `schemaFile` - the table schema for the resulting rows.
* `outputTable` - the table into which the new rows are written.
* `numberOfDays` - the length of chunks that we query fron ndt.all. 7 seems to be a
safe number.
* `dates` - an array of timestamps. Each pair will result in its own pipeline.
* `mergeTable` - the table into which the upload and download rows will be merged
* `mergeTableSchema` - the schema of the new merged table
* `withISPTable` - the table into which the rows will be moved when they are joined
with the ISP resolution data for client & server IPs.
* `withISPTableSchema` - the schema of the new table with ISP information.
