#!/bin/bash
basedir=`dirname $0`
locationDir=$basedir/../../../dataflow/data/bigquery/location


echo "Creating location tables"

echo "Add data_viz_helpers.location_country_codes"
bq load --allow_quoted_newlines --skip_leading_rows=1 --source_format=CSV \
  data_viz_helpers.location_country_codes  \
  $locationDir/iso3166.csv \
  $locationDir/schemas/iso3166.json


echo "Add data_viz_helpers.location_region_codes"
bq load --allow_quoted_newlines --skip_leading_rows=1 --source_format=CSV \
  data_viz_helpers.location_region_codes  \
  $locationDir/region_codes.csv \
  $locationDir/schemas/region_codes.json

echo "Done."
