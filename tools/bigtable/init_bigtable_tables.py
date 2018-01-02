#!/usr/bin/env python

import argparse
import glob
import time
from tools.bigtable.bigtable_utils import init_pool
from tools.utils import read_json

COLUMN_FAMILIES = {
    'data': dict(max_versions=1),
    'meta': dict(max_versions=1)
}

def main(config_dir, remove_tables, pattern):
    '''
    Create big tables based on configuration directory
    '''

    connection_pool = None

    print config_dir
    config_files = glob.glob(config_dir + "/" + pattern)

    connection_pool = init_pool()

    with connection_pool.connection(timeout=5) as connection:
        for config_file in config_files:
            config = read_json(config_file)

            table_id = config['bigtable_table_name']
            if remove_tables:
                print 'REMOVING ' + table_id + ' bigtable table'
                try:
                    connection.delete_table(table_id)
                except Exception as err:
                    print err
                    print 'NO TABLE FOUND TO DELETE'

            print 'CREATING ' + table_id + ' bigtable table'

            # create table family
            try:
                connection.create_table(table_id, COLUMN_FAMILIES)
            except Exception as err:
                print err
                print 'TABLE EXISTS'

            time.sleep(2)
        connection.close()

    print 'Tables created'

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    PARSER.add_argument(
        '--configs',
        help='Directory of Bigtable config files')

    PARSER.add_argument(
        '--pattern',
        help='Config file pattern', default="*.json")

    PARSER.add_argument(
        '--remove',
        help='Destroy existing tables',
        action='store_true',
        dest='remove')

    PARSER.add_argument(
        '--no-remove',
        help='Do not attempt to destroy existing tables',
        action='store_false',
        dest='remove')

    PARSER.set_defaults(remove=False)

    ARGS = PARSER.parse_args()
    if(not ARGS.configs):
        print('--configs required. Provide config file directory')
    else:
        main(ARGS.configs, ARGS.remove, ARGS.pattern)
