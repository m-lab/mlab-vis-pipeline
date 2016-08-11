
# Scripts to setup Bigtable the `right` way

## init_bigtable_tables.py

According to [dataflow documentation](https://cloud.google.com/bigtable/docs/dataflow-hbase),
the tables dataflow writes to must already be present.

I haven't looked at a Java-only solution for this yet, but I will.

For now, we have `init_bigtable_tables.py`.

The script requires 3 inputs.

```
--project_id - Google Project ID
--instance_id - Bigtable Instance ID to create tables in
--configs - Bigtable configs directory
```

The first two have default values, so you can run with:

```
./init_bigtable_tables.py --configs ../dataflow/data/bigtable/
```

The script reads the bigtable config files and creates a new table for
each found. This relies on the `bigtable_table_name` attribute of the config files.

**WARNING**

This will delete tables if they exist. It will destroy all information.


## create_bigtable_configs.py

This script generates `.json` bigtable config files and `.sql` query files.

The two files are used to query our bigquery tables and load that data into
bigtable.

Right now, it doesn't take any parameters.

```
./create_bigtable_configs.py
```

It will create these files in `dataflow/data/bigtable`.

Adding more bigtables is as simple as adding more fields to the top of the script.

## create_bigtable_search_configs.py

This script generates `.json` bigtable config files and `.sql` query files for
search and list tables specifically.

The two files are used to query our bigquery tables and load that data into
bigtable.

Right now, it doesn't take any parameters.

If you run `create_bigtable_configs.py` it will also run this one but you can
run it separatly. It does not take arguments.

It will create these files in `dataflow/data/bigtable` as well.

It is driven by configurations inside of `configurations/search_tables.py`.