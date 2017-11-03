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

tableName="${PROJECT}:data_viz_helpers.asn_merge"

tableSchema=./$basedir/data/schemas/asn_merge.json
outputFile=./$basedir/output/asn_merge.csv

echo "Processing asn_merge CSV"
python -m tools.bigquery.asn_merge.process_asn_merge

echo "Removing $tableName from BigQuery"
bq rm -f $tableName

echo "Adding $tableName to BigQuery"
bq load --allow_quoted_newlines --skip_leading_rows=1 \
$tableName $outputFile $tableSchema

echo "Done."
