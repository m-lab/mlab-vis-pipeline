lint:
	pylint --rcfile tools/.pylintrc tools/**/**.py

#-- Bigquery tasks:
sites:
	cd ./tools/bigquery/mlab-sites && ./deploy_sites_table.sh

maxmind:
	cd ./tools/bigquery/maxmind && ./deploy_maxmind_asn.sh

location:
	cd ./tools/bigquery/location && ./deploy_location.sh

timezones:
	cd ./tools/bigquery/timezones && ./deploy_timezones.sh

#-- Bigtable tasks:

bigtable-configs:
	./tools/bigtable/create_bigtable_configs.py && \
	./tools/bigtable/create_bigtable_search_configs.py
