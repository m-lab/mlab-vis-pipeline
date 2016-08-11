## Daily pipeline runs

These runs are meant to update our data with the latest tests from
ndt.all that have happened since the last update up until 2 days before
the current date.

This isn't too long of an operation.
See the `UpdatePipeline` for more details.


Note that we separate downloads and uploads, as well as day and hour based
aggregates, leading to there being 4 different combinations.

### Required parameters:

* `queryFile` - The query file that will pull rows from ndt. This file
will likely require date ranges.
* `schemaFile` - the table schema for the resulting rows.
* `outputTable` - the table into which the new rows are written.
* `lastDateFrom` - the table from which to fetch the last date.
* `dateColumn` - the column name of the last date.
* `mergeTable` - the table into which the upload and download rows will be merged
* `mergeTableSchema` - the schema of the new merged table
* `withISPTable` - the table into which the rows will be moved when they are joined
with the ISP resolution data for client & server IPs.
* `withISPTableSchema` - the schema of the new table with ISP information.
