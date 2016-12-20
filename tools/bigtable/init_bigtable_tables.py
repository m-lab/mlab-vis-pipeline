#!/usr/bin/python

import argparse
import json
import glob
import sys
import time
from gcloud import bigtable
from gcloud.bigtable import happybase

DEFAULT_PROJECT_ID = 'mlab-staging'
DEFAULT_INSTANCE_ID = 'mlab-ndt-agg'
COLUMN_FAMILIES = {'data': dict(), 'meta': dict()}

def read_json(filename):
    '''
    read json
    '''
    data = {}
    with open(filename) as data_file:
        data = json.load(data_file)
    return data


def main(project_id, instance_id, config_dir, remove_tables, pattern):
    '''
    main
    '''
    print(config_dir)

    config_files = glob.glob(config_dir + "/" + pattern)
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)

    connection = happybase.Connection(instance=instance)

    for config_file in config_files:

        config = read_json(config_file)

        table_id = config['bigtable_table_name']
        if remove_tables:
            print('REMOVING ' + table_id + ' bigtable table')
            try:
                connection.delete_table(table_id)
            except Exception as err:
                print err
                print('NO TABLE FOUND TO DELETE')

        print('CREATING ' + table_id + ' bigtable table')

        try:
            connection.create_table(table_id, COLUMN_FAMILIES)
        except Exception as err:
            print(err)
            print('TABLE EXISTS')

        time.sleep(2)

    connection.close()



if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--project_id', help='Your Cloud Platform project ID.', default=DEFAULT_PROJECT_ID)
    parser.add_argument(
        '--instance_id', help='ID of the Cloud Bigtable instance to connect to.', default=DEFAULT_INSTANCE_ID)
    parser.add_argument(
        '--configs',
        help='Directory of Bigtable config files')

    parser.add_argument(
        '--pattern',
        help='Config file pattern', default="*.json")

    parser.add_argument(
        '--remove',
        help='Destroy existing tables',
        action='store_true',
        dest='remove')

    parser.add_argument(
        '--no-remove',
        help='Do not attempt to destroy existing tables',
        action='store_false',
        dest='remove')

    parser.set_defaults(remove=False)

    args = parser.parse_args()
    if(not args.configs):
        print('--configs required. Provide config file directory')
    else:
        main(args.project_id, args.instance_id, args.configs, args.remove, args.pattern)
