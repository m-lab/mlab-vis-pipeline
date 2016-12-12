#!/bin/bash

echo "Creating location tables"

echo "Add data_viz.location_country_codes"
bq load --allow_quoted_newlines --skip_leading_rows=1 --source_format=CSV \
  data_viz.location_country_codes  \
  ./dataflow/data/bigquery/location/iso3166.csv \
  ./dataflow/data/bigquery/location/schemas/iso3166.json


echo "Add dataviz.location_region_codes"
bq load --allow_quoted_newlines --skip_leading_rows=1 --source_format=CSV \
  data_viz.location_region_codes  \
  ./dataflow/data/bigquery/location/region_codes.csv \
  ./dataflow/data/bigquery/location/schemas/region_codes.json

echo "Done."
