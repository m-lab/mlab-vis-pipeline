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

dataDir=./$basedir/data
outputDir=./$basedir/output
tableName="${PROJECT}:data_viz_helpers.localtime_timezones"

echo "Creating timezone tables"

echo "Processing timezones csvs CSV"
python -m tools.bigquery.timezones.process_timezones

echo "Removing existing table"
bq rm -f $tableName

echo "Adding ${tableName} to BigQuery"
bq load --allow_quoted_newlines --skip_leading_rows=1 --source_format=CSV \
  $tableName \
  $outputDir/merged_timezone.csv \
  $dataDir/schemas/merged_timezone.json

echo "Done."
