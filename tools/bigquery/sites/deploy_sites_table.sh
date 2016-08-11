#!/bin/bash

echo "Processing mlab_sites CSV"
python process_sites.py

echo "Removing bocoup.mlab_sites from BigQuery"
bq rm -f bocoup.mlab_sites

echo "Adding bocoup.mlab_sites to BigQuery"
bq load --allow_quoted_newlines --skip_leading_rows=1 \
  bocoup.mlab_sites \
  ./output/mlab_sites_processed.csv \
  ./dataflow/data/bigquery/mlab-sites/schemas/schema.json

echo "Done."