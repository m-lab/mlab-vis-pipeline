#!/bin/bash

echo "Creating timezone tables"

echo "Processing timezones csvs CSV"
python ./tools/bigquery/timezones/process_timezones.py

echo "Adding bocoup.localtime_timezones to BigQuery"
bq load --allow_quoted_newlines --skip_leading_rows=1 --source_format=CSV \
  data_viz.localtime_timezones  \
  ./dataflow/data/bigquery/timezonedb/merged_timezone.csv \
  ./dataflow/data/bigquery/timezonedb/schemas/merged_timezone.json

echo "Done."
