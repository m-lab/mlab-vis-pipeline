#!/bin/bash

basedir=`dirname $0`

tableName=data_viz.mlab_sites
tableSchema=$basedir/../../../dataflow/data/bigquery/mlab-sites/schemas/mlab_sites_schema.json


echo "Processing mlab_sites CSV"
python process_sites.py

echo "Removing $tableName from BigQuery"
bq rm -f $tableName

echo "Adding $tableName to BigQuery"
bq load --allow_quoted_newlines --skip_leading_rows=1 \
  $tableName $basedir/output/mlab_sites_processed.csv $tableSchema

echo "Done."