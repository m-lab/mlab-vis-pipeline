
# Tools for Generating and Maintaining BigTable tables and config files

## Creating Base BigTable Tables

Use:

```
init_bigtable_tables.py
```

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

An optional `--pattern` flag can be used to specify a subset of config files to look for.

By default it searches for `"*.json"`.

An optional `--remove` flag can be used to remove existing tables prior to creating them.

**WARNING**

Using `--remove` will delete tables if they exist. It will destroy all information contained in those tables.

## Creating Config Files

Use:

```
create_bigtable_time_configs.py

create_bigtable_search_configs.py

create_bigtable_premade_configs.py
```

These scripts generate `.json` bigtable config files and `.sql` query files.

The two files are used to query our bigquery tables and load that data into
bigtable. They are also used on the api side to know how to read from these tables.

Right now, these scripts take no parameters, so they can be run with no additional options.


Config files and query files will be created in: `dataflow/data/bigtable`.

`create_bigtable_time_configs` creates scripts around tables that aggregate by time.

`create_bigtable_search_configs` creates scripts for search and list tables.

`create_bigtable_premade_configs` just copies over files from `./templates/premade`

## Other Tools

```
remove_unused_tables.py
```

Reads config files and removes tables that are not used in any config file.

```
test_bigtable_connection.py
```

Performs simple test to see if bigtable connection is writable.
