#!/usr/bin/env python

import argparse
import glob
import time
from tools.bigtable.bigtable_utils import init_pool
from tools.utils import read_json

def main(config_dir, remove_tables):
    '''
    main
    '''

    connection_pool = None
    print config_dir
    config_files = glob.glob(config_dir + "/*.json")

    connection_pool = init_pool()
    with connection_pool.connection(timeout=5) as connection:
        current_tables = []

        for config_file in config_files:

            config = read_json(config_file)
            table_id = config['bigtable_table_name']
            current_tables.append(table_id)

        all_tables = connection.tables()

        for table in all_tables:
            if remove_tables:
                print 'Removing: ' + table
                connection.delete_table(table)
                time.sleep(2)
            else:
                print 'Unused table: ' + table

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description = __doc__,
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)
    PARSER.add_argument(
        '--configs',
        help = 'Directory of Bigtable config files')
    PARSER.add_argument(
        '--remove',
        help = 'Destroy existing tables',
        action = 'store_true',
        dest = 'remove')
    PARSER.add_argument(
        '--no-remove',
        help = 'Do not attempt to destroy existing tables',
        action = 'store_false',
        dest = 'remove')

    PARSER.set_defaults(remove=False)

    ARGS = PARSER.parse_args()
    if not ARGS.configs:
        print '--configs required. Provide config file directory'
    else:
        main(ARGS.configs, ARGS.remove)
