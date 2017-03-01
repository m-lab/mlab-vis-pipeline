#!/usr/bin/env python

import argparse
import json
import glob
import sys
import time
from gcloud import bigtable
from gcloud.bigtable import happybase

DEFAULT_PROJECT_ID = 'mlab-sandbox'
DEFAULT_INSTANCE_ID = 'mlab-ndt-agg'

def read_json(filename):
    '''
    read json
    '''
    data = {}
    with open(filename) as data_file:
        data = json.load(data_file)
    return data

def main(project_id, instance_id, config_dir, remove_tables):
    '''
    main
    '''
    print(config_dir)
    config_files = glob.glob(config_dir + "/*.json")
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)

    connection = happybase.Connection(instance=instance)

    current_tables = []

    for config_file in config_files:

        config = read_json(config_file)
        table_id = config['bigtable_table_name']
        current_tables.append(table_id)

    all_tables = connection.tables()

    for table in all_tables:
        if True:
            if remove_tables:
                print 'Removing: ' + table
                connection.delete_table(table)
                time.sleep(2)
            else:
                print 'Unused table: ' + table






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
        main(args.project_id, args.instance_id, args.configs, args.remove)
