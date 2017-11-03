#!/bin/bash

USAGE="$0 [production|staging|sandbox]"
basedir=`dirname "$BASH_SOURCE"`

set -e
set -x

# Initialize correct environment variables
if [[ "$1" == production ]]; then
  source ./environments/production.sh
elif [[ "$1" == staging ]]; then
  source ./environments/staging.sh
elif [[ "$1" == sandbox ]]; then
  source ./environments/sandbox.sh
else
  echo "BAD ARGUMENT TO $0"
  exit 1
fi

tableName="${PROJECT}:data_viz_helpers.location_cleaning"

tableSchema=./$basedir/data/schemas/location_cleaning.json
outputFile=$basedir/output/location_cleaning.csv

echo "Processing location_cleaning CSV"
python $basedir/process_location_cleaning.py

echo "Removing $tableName from BigQuery"
bq rm -f $tableName

echo "Adding $tableName to BigQuery"
bq load --allow_quoted_newlines --skip_leading_rows=1 \
  $tableName $outputFile $tableSchema

echo "Done."
