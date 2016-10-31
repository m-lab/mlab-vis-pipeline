lint:
	pylint --rcfile tools/.pylintrc tools/**/**.py

#-- Bigquery tasks:
asn_merge:
	cd ./tools/bigquery/asn_merge && ./deploy_asn_merge.sh

sites:
	cd ./tools/bigquery/mlab-sites && ./deploy_sites_table.sh

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

bigquery: asn_merge sites maxmind location location_cleaning timezones mlab_sites
	echo 'DONE'

#-- Bigtable tasks:

bigtable_configs:
	./tools/bigtable/create_bigtable_time_configs.py && \
	./tools/bigtable/create_bigtable_search_configs.py && \
	./tools/bigtable/create_bigtable_premade_configs.py

clean_temp_datasets:
	./tools/bigquery/cleanup/remove_temp_bigquery_tables.py
