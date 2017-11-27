#!/bin/bash

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

setup_bq_dataset:
	@if [ $(shell bq ls | grep data_viz_helpers | wc -l) = 0 ]; then\
        echo "Making data_viz_helpers dataset";\
		bq mk data_viz_helpers;\
    fi

datastore_indices:
	./tools/bigquery/datastore_indices/setup_datastore_indices.sh ${API_MODE}

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
setup_bigquery: authenticate datastore_indices setup_bq_dataset asn_merge maxmind location location_cleaning timezones mlab_sites
	echo 'Done creating bigquery helper tables'

# Create a kubernetis cluster
setup_cluster: authenticate
	gcloud container clusters create --zone us-central1-b mlab-vis-pipeline-jobs

setup: setup_bigquery setup_bigtable setup_cluster
	echo "setup complete."

#-- Pipeline Jars, Docker container
build:
	./build_cluster.sh -m ${API_MODE}

test:
	cd dataflow && mvn test && cd ../

