#!/usr/bin/env python

from oauth2client.client import GoogleCredentials
from googleapiclient.discovery import build

from googleapiclient.errors import HttpError

import pprint

PROJECT_ID = 'mlab-oti'

# Grab the application's default credentials from the environment.
credentials = GoogleCredentials.get_application_default()
# Construct the service object for interacting with the BigQuery API.
bigquery_service = build('bigquery', 'v2', credentials=credentials)


# List all tables
def listTables(service, project, dataset):
    try:
        tables = service.tables()
        listReply = tables.list(projectId=project, datasetId=dataset).execute()
        print 'Tables list:'
        pprint.pprint(listReply)

    except HttpError as err:
        print 'Error in listTables:', pprint.pprint(err.content)

def listDatasets(service, project):
    dataset_ids = []
    try:
        datasets = service.datasets()
        listReply = datasets.list(projectId=project).execute()
        dataset_ids = [d['id'] for d in listReply['datasets']]
        # print 'dataset list:'
        # pprint.pprint(listReply)

    except HttpError as err:
        print 'Error in listTables:', pprint.pprint(err.content)

    return dataset_ids

# Retrieve the specified table resource
def getTable(service, projectId, datasetId, tableId):
    tableCollection = service.tables()
    try:
        tableReply = tableCollection.get(projectId=projectId,
                                   datasetId=datasetId,
                                   tableId=tableId).execute()
        print 'Printing table resource %s:%s.%s' % (projectId, datasetId, tableId)
        pprint.pprint(tableReply)

    except HttpError as err:
        print 'Error in querytableData: ', pprint.pprint(err)


def deleteTable(service, projectId, datasetId, tableId):
    service.tables().delete(projectId=projectId,
                          datasetId=datasetId,
                          tableId=tableId).execute()

def deleteDataset(service, projectId, datasetId):
    try:
        service.datasets().delete(projectId=projectId,
                          datasetId=datasetId, deleteContents=True).execute()
    except HttpError as err:
        print(err)

datasets = listDatasets(bigquery_service, PROJECT_ID)

for dataset_id in datasets:
    if "temp" in dataset_id:
        real_id = dataset_id.split(":")[1]
        print "Going to remove {0}".format(real_id)

        deleteDataset(bigquery_service, PROJECT_ID, real_id)
