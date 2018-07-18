#!/usr/bin/env python
#pylint: disable=no-name-in-module, relative-import

import os
import argparse
from google.cloud import bigtable
from google.cloud import happybase
from google.oauth2 import service_account

def main():
    '''
    Main connection establishmnent
    '''
    credentials = service_account.Credentials.from_service_account_file(
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'])

    connection_pool = None
    project_id = os.environ.get("PROJECT")
    bigtable_instance = os.environ.get("BIGTABLE_INSTANCE")
    bigtable_pool_size = os.environ.get("BIGTABLE_POOL_SIZE")

    if project_id and bigtable_instance:
        try:
            client = bigtable.Client(project=project_id,
                                     admin=True,
                                     credentials=credentials)

            instance = client.instance(bigtable_instance)
            size = int(bigtable_pool_size) if bigtable_pool_size else 10

            connection_pool = happybase.pool.ConnectionPool(size,
                                                            instance=instance)

            with connection_pool.connection(timeout=5) as connection:
                connection.open()
                all_tables = connection.tables()
                print "There are {} tables for project {} on instance {}".format(len(all_tables), project_id, bigtable_instance)
                for table in all_tables:
                    print table
        except Exception as err:
            print "Failed to connect"
            print err

if __name__ == '__main__':
    main()
