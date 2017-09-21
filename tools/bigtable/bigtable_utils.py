'''
Returns a big table connection pool
'''
import logging
import os

#pylint: disable=no-name-in-module, relative-import
from google.cloud import bigtable
from google.cloud import happybase
from google.cloud.bigtable.row_filters import FamilyNameRegexFilter
from google.oauth2 import service_account

def init_pool():
    '''
    Setup Connection
    From the documentation:
    Creating a Connection object is a heavyweight operation;
     you should create a single Connection and
     share it among threads in your application.
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
                                     admin=True, credentials=credentials)

            instance = client.instance(bigtable_instance)

            size = int(bigtable_pool_size) if bigtable_pool_size else 10

            connection_pool = happybase.pool.ConnectionPool(size,
                                                            instance=instance)
            logging.info("Connection made to %s for project %s",
                         bigtable_instance, project_id)
        except Exception as err:  #pylint: disable=W0703
            logging.exception("ERROR: Could not make connection")
            logging.exception(err)
    else:
        logging.warning('WARNING: no connection made')
    return connection_pool
