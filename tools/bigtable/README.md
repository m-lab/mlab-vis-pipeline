# BigTable Tools

These scripts are for generating and maintaining BigTable tables and config
files.

**Note**

The configuration files output by these scripts are checked into this repo.

If the data columns to be stored in the bigtable tables has not changed and if
the names of the columns of the source bigquery table has not changed, then
there is no need to run these scripts again.

## Running

Instructions for running these are in the main application README.md under
"Setup Bigtable".

## Other Tools

Read config files and removes tables that are not used in any config file.
From application root:

```
CONFIG_DIR=<path to config dir. Optional> ./tools/bigtable/remove_unused_tables.sh staging|sandbox|production
```



