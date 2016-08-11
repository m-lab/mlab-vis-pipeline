#!/bin/bash

echo "Processing maxmind CSV"
./format_maxmind_csv.py  ./dataflow/data/bigquery/asn/GeoIPASNum2.csv  ./dataflow/data/bigquery/asn/GeoIPASNum2v6.csv  ./dataflow/data/bigquery/asn/data/asn_name_map.csv

echo "Removing bocoup.maxmind_asn from BigQuery"
bq rm -f bocoup.maxmind_asn

# head -n 100 ./output/maxmind_asn.csv > ./output/maxmind_asn_short.csv

echo "Adding bocoup.maxmind_asn to BigQuery"
bq load --allow_quoted_newlines --skip_leading_rows=1 --source_format=CSV \
  bocoup.maxmind_asn \
  ./output/maxmind_asn.csv \
  ./dataflow/data/bigquery/asn/schemas/schema.json

echo "Done."
