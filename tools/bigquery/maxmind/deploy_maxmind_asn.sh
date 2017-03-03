#!/bin/bash

basedir=`dirname $0`

ipv4File=$basedir/../../../dataflow/data/bigquery/asn/GeoIPASNum2.csv
ipv6File=$basedir/../../../dataflow/data/bigquery/asn/GeoIPASNum2v6.csv
asnNameFile=$basedir/../../../dataflow/data/bigquery/asn/asn_name_map.csv

tableName=data_viz_helpers.maxmind_asn
tableSchema=$basedir/../../../dataflow/data/bigquery/asn/schemas/maxmind_asn_schema.json

# This outputs the ./output/maxmind_asn.csv
echo "Processing maxmind CSV"
./format_maxmind_csv.py $ipv4File $ipv6File $asnNameFile

echo "Removing $tableName from BigQuery"
bq rm -f $tableName

echo "Adding $tableName to BigQuery"
bq load --allow_quoted_newlines --skip_leading_rows=1 --source_format=CSV \
  $tableName $basedir/output/maxmind_asn.csv $tableSchema

echo "Done."
