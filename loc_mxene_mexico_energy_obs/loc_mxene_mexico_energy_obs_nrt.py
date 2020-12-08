import webbrowser, sys, bs4
import pandas as pd
from datetime import date, timedelta, datetime
import numpy as np
import re
from urllib.parse import urljoin
import os
from os import listdir
from os.path import isfile, join
import requests
import logging
import sys
import time
from collections import OrderedDict
import cartosql
import json
from filecmp import cmp
import urllib
import geocoder
import logging
import sys
import time
from collections import OrderedDict
import cartosql
import json

#Setting up logging
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = False

# name of data directory in Docker container
DATA_DIR = 'data'

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')
# asserting table structure rather than reading from input
# We will create four tables for this dataset, due to the different dissagregation levels.
CARTO_NODES_PML_TABLE = 'loc_mx_ene_nodes_pml'
CARTO_LOAD_PML_TABLE = 'loc_mx_ene_load_pml'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_NODES_PML_SCHEMA = OrderedDict([
       ('the_geom', 'geometry'),
       ('uid', 'numeric'),
       ('entry_date', 'timestamp'),
       ('node_id', 'text'),
       ('pml','numeric'),
       ('energy','numeric'),
       ('losses','numeric'),
       ('congestion','numeric'),
       ('node_name','text'),
       ('system','text'),
       ('control_center','text'),
       ('load_zone','text'),
       ('state','text'),
       ('municipality','text'),
       ('latitude','numeric'),
       ('longitude','numeric')
    ])

CARTO_LOAD_PML_SCHEMA = OrderedDict([
       ('the_geom', 'geometry'),
       ('uid', 'numeric'),
       ('entry_date', 'timestamp'),
       ('load','numeric'),
       ('system', 'text'),
       ('load_zone','numeric'),
       ('state','text'),
       ('municipality','text'),
       ('latitude','numeric'),
       ('longitude','numeric')
    ])

UID_FIELD = 'uid'
TIME_FIELD = 'entry_date'

CARTO_USER = os.environ.get('CARTO_USER')
CARTO_KEY = os.environ.get('CARTO_KEY')

#DATASET_ID =
'''
FUNCTIONS FOR ALL DATASETS
The functions below must go in every near real-time script.
Their format should not need to be changed.
'''

def lastUpdateDate(dataset, date):
    '''
    Given a Resource Watch dataset's API ID and a datetime,
    this function will update the dataset's 'last update date' on the API with the given datetime
    INPUT   dataset: Resource Watch API dataset ID (string)
            date: date to set as the 'last update date' for the input dataset (datetime)
    '''
    # generate the API url for this dataset
    apiUrl = f'http://api.resourcewatch.org/v1/dataset/{dataset}'
    # create headers to send with the request to update the 'last update date'
    headers = {
    'Content-Type': 'application/json',
    'Authorization': os.getenv('apiToken')
    }
    # create the json data to send in the request
    body = {
        "dataLastUpdated": date.isoformat() # date should be a string in the format 'YYYY-MM-DDTHH:MM:SS'
    }
    # send the request
    try:
        r = requests.patch(url = apiUrl, json = body, headers = headers)
        logging.info('[lastUpdated]: SUCCESS, '+ date.isoformat() +' status code '+str(r.status_code))
        return 0
    except Exception as e:
        logging.error('[lastUpdated]: '+str(e))

'''
FUNCTIONS FOR CARTO DATASETS
The functions below must go in every near real-time script for a Carto dataset.
Their format should not need to be changed.
'''

def checkCreateTable(table, schema, id_field, time_field=''):
    '''
    Create the table if it does not exist, and pull list of IDs already in the table if it does
    INPUT   table: Carto table to check or create (string)
            schema: dictionary of column names and types, used if we are creating the table for the first time (dictionary)
            id_field: name of column that we want to use as a unique ID for this table; this will be used to compare the
                    source data to the our table each time we run the script so that we only have to pull data we
                    haven't previously uploaded (string)
            time_field:  optional, name of column that will store datetime information (string)
    RETURN  list of existing IDs in the table, pulled from the id_field column (list of strings)
    '''
    # check it the table already exists in Carto
    if cartosql.tableExists(table, user=CARTO_USER, key=CARTO_KEY):
        # if the table does exist, get a list of all the values in the id_field column
        logging.info('Fetching existing IDs')
        r = cartosql.getFields(id_field, table, f='csv', post=True, user=CARTO_USER, key=CARTO_KEY)
        # turn the response into a list of strings, removing the first and last entries (header and an empty space at end)
        return r.text.split('\r\n')[1:-1]
    else:
        # if the table does not exist, create it with columns based on the schema input
        logging.info('Table {} does not exist, creating'.format(table))
        cartosql.createTable(table, schema, user=CARTO_USER, key=CARTO_KEY)
        # if a unique ID field is specified, set it as a unique index in the Carto table; when you upload data, Carto
        # will ensure no two rows have the same entry in this column and return an error if you try to upload a row with
        # a duplicate unique ID
        if id_field:
            cartosql.createIndex(table, id_field, unique=True, user=CARTO_USER, key=CARTO_KEY)
        # if a time_field is specified, set it as an index in the Carto table; this is not a unique index
        if time_field:
            cartosql.createIndex(table, time_field, user=CARTO_USER, key=CARTO_KEY)
        # return an empty list because there are no IDs in the new table yet
        return []


'''
FUNCTIONS FOR THIS DATASET
The functions below have been tailored to this specific dataset.
They should all be checked because their format likely will need to be changed.
'''
