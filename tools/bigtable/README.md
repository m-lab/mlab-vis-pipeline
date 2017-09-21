# BigTable Tools

These scripts are for generating and maintaining BigTable tables and config files.

**Note**

The configuration files output by these scripts are checked into this repo.

If the data columns to be stored in the bigtable tables has not changed and if the
names of the columns of the source bigquery table has not changed, then there is
no need to run these scripts again.

## Creating Base BigTable Tables

This should normally be run from the root of the application by using the
`setup_tables.sh` script. See the main `README.md` for instructions. To manually
run this script, you can follow the instructions below.

This requires two steps:

### 1. Create configuration files

These scripts generate `.json` bigtable config files and `.sql` query files.

```
create_bigtable_time_configs.py --configs <dir>
create_bigtable_search_configs.py --configs <dir>
create_bigtable_premade_configs.py --configs <dir>
```

The two files are used to query our bigquery tables and load that data into
bigtable. They are also used on the api side to know how to read from these
tables.

These scripts take an optional configs parameter. By default, they point to
`dataflow/data/bigtable`. they need to be there to be packaged with the jar.

* `create_bigtable_time_configs` creates scripts around tables that aggregate by time.
* `create_bigtable_search_configs` creates scripts for search and list tables.
* `create_bigtable_premade_configs` just copies over files from `./templates/premade`

### 2. Create bigtable tables

According to [dataflow documentation](https://cloud.google.com/bigtable/docs/dataflow-hbase),
the tables dataflow writes to must already be present. To create them, we use
the script `init_bigtable_tables.py`.

The script requires 3 inputs.

`--configs - Bigtable configs directory`

The first two have default values, so you can run with:

`./init_bigtable_tables.py --configs ../../dataflow/data/bigtable/`

The script reads the bigtable config files and creates a new table for each
found. This relies on the `bigtable_table_name` attribute of the config files.

An optional `--pattern` flag can be used to specify a subset of config files
to look for. By default it searches for `"*.json"`.

An optional `--remove` flag can be used to remove existing tables prior to
creating them.

**WARNING**

Using `--remove` will delete tables if they exist. It will destroy all
information contained in those tables.

## Other Tools

```
remove_unused_tables.py
```

Reads config files and removes tables that are not used in any config file.

