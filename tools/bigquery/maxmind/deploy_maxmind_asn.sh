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

ipv4File=./$basedir/data/GeoIPASNum2.csv
ipv6File=./$basedir/data//GeoIPASNum2v6.csv
asnNameFile=./$basedir/data/asn_name_map.csv

tableName="${PROJECT}:data_viz_helpers.maxmind_asn"
tableSchema=./$basedir/data/schemas/maxmind_asn_schema.json

# This outputs the ./output/maxmind_asn.csv
echo "Processing maxmind CSV"

./$basedir/format_maxmind_csv.py $ipv4File $ipv6File $asnNameFile

echo "Removing $tableName from BigQuery"
bq rm -f $tableName

echo "Adding $tableName to BigQuery"
bq load --allow_quoted_newlines --skip_leading_rows=1 --source_format=CSV \
  $tableName $basedir/output/maxmind_asn.csv $tableSchema

echo "Done."
