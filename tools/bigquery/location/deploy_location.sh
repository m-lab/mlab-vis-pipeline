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

locationDir=./dataflow/data/bigquery/location

echo "Creating location tables"

# == Country code locations
tableName1="${PROJECT}:data_viz_helpers.location_country_codes"
echo "Removing $tableName1 from BigQuery"
bq rm -f $tableName1

echo "Add ${tableName1}"
bq load --allow_quoted_newlines --skip_leading_rows=1 --source_format=CSV \
  ${tableName1}  \
  $locationDir/iso3166.csv \
  $locationDir/schemas/iso3166.json

# == Region code table
tableName2="${PROJECT}:data_viz_helpers.location_region_codes"

echo "Removing $tableName2 from BigQuery"
bq rm -f $tableName2

echo "Add ${tableName2}"
bq load --allow_quoted_newlines --skip_leading_rows=1 --source_format=CSV \
  ${tableName2}  \
  $locationDir/region_codes.csv \
  $locationDir/schemas/region_codes.json

echo "Done."
