lint:
	pylint --rcfile tools/.pylintrc tools/**/**.py

#-- Bigtable tasks:

create_table_configs:
	./tools/bigtable/create_table_configs.sh

create_tables:
	./tools/bigtable/create_tables.sh $(API_MODE)

clean_temp_datasets:
	./tools/bigquery/cleanup/remove_temp_bigquery_tables.py

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
	cd ./tools/bigquery/asn_merge && ./deploy_asn_merge.sh

maxmind:
	cd ./tools/bigquery/maxmind && ./deploy_maxmind_asn.sh

location:
	cd ./tools/bigquery/location && ./deploy_location.sh

location_cleaning:
	cd ./tools/bigquery/location_cleaning && ./deploy_location_cleaning.sh

timezones:
	cd ./tools/bigquery/timezones && ./deploy_timezones.sh

mlab_sites:
	cd ./tools/bigquery/mlab-sites && ./deploy_sites_table.sh

# Call:
# KEY_FILE=<path to cred file> make setup_bigquery
setup_bigquery: authenticate asn_merge maxmind location location_cleaning timezones mlab_sites
	echo 'Done creating bigquery helper tables'

