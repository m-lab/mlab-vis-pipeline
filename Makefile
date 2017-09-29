lint:
	pylint --rcfile tools/.pylintrc tools/**/**.py

#-- Bigtable tasks:

# Call
# API_MODE=sandbox|staging|production \
# KEY_FILE=<key file path> make test_connection
test_connection:
	./tools/bigtable/test_connection.sh ${API_MODE}

create_table_configs:
	./tools/bigtable/create_table_configs.sh $(API_MODE)

create_tables:
	./tools/bigtable/create_tables.sh $(API_MODE)

clean_temp_datasets:
	./tools/bigquery/cleanup/remove_temp_bigquery_tables.sh ${API_MODE}

# Call:
# KEY_FILE=<path to cred file> \
# API_MODE=staging|production|sandbox make setup_bigtable
setup_bigtable: create_table_configs create_tables
	echo 'Done creating bigtable tables'

#-- Bigquery tasks:

authenticate:
	echo 'Authenticate service account'
	gcloud auth activate-service-account --key-file=${KEY_FILE}

asn_merge:
	./tools/bigquery/asn_merge/deploy_asn_merge.sh ${API_MODE}

maxmind:
	./tools/bigquery/maxmind/deploy_maxmind_asn.sh ${API_MODE}

location:
	./tools/bigquery/location/deploy_location.sh ${API_MODE}

location_cleaning:
	./tools/bigquery/location_cleaning/deploy_location_cleaning.sh ${API_MODE}

timezones:
	./tools/bigquery/timezones/deploy_timezones.sh ${API_MODE}

mlab_sites:
	./tools/bigquery/mlab-sites/deploy_sites_table.sh ${API_MODE}

# Call:
# KEY_FILE=<path to cred file> \
# API_MODE=staging|production|sandbox make setup_bigquery
setup_bigquery: authenticate asn_merge maxmind location location_cleaning timezones mlab_sites
	echo 'Done creating bigquery helper tables'

#-- Pipeline Jars
# Previously: cd dataflow && mvn -Dmaven.test.skip=true package && cd ../

build:
	cd dataflow && mvn package && cd ../

test:
	cd dataflow && mvn test && cd ../

