#!/bin/bash

basedir=`dirname $0`


tableName=data_viz.location_cleaning

tableSchema=$basedir/../../../dataflow/data/bigquery/location_cleaning/schemas/location_cleaning.json
outputFile=$basedir/output/location_cleaning.csv

echo "Processing location_cleaning CSV"
python $basedir/process_location_cleaning.py

echo "Removing $tableName from BigQuery"
bq rm -f $tableName

echo "Adding $tableName to BigQuery"
bq load --allow_quoted_newlines --skip_leading_rows=1 \
  $tableName $outputFile $tableSchema

echo "Done."
