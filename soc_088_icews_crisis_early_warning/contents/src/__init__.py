import os
import logging
import sys
from collections import OrderedDict
import datetime
import cartosql
import requests
from bs4 import BeautifulSoup
import numpy as np
import json
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import pandas as pd

# Constants
SOURCE_URL = 'https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/QI2T9A'

CARTO_TABLE = 'soc_088_icews_crisis_early_warning'
CARTO_SCHEMA = OrderedDict([
    ('uid', 'text'),
    ('the_geom', 'geometry'),
    ('Event_ID', 'text'),
    ('Event_Date', 'timestamp'),
    ('Source_Name', 'text'),
    ('Source_Sectors', 'text'),
    ('Source_Country', 'text'),
    ('Event_Text', 'text'),
    ('CAMEO_Code', 'numeric'),
    ('Intensity', 'numeric'),
    ('Target_Name', 'text'),
    ('Target_Sectors', 'text'),
    ('Target_Country', 'text'),
    ('Story_ID', 'numeric'),
    ('Sentence_Number', 'numeric'),
    ('Publisher', 'text'),
    ('City', 'text'),
    ('District', 'text'),
    ('Province', 'text'),
    ('Country', 'text'),
    ('Latitude', 'numeric'),
    ('Longitude', 'numeric'),
    ('File_ID', 'text')
])
UID_FIELD = 'uid'
FILE_ID_FIELD = 'File_ID'
TIME_FIELD = 'Event_Date'
DATA_DIR = 'data'
LOG_LEVEL = logging.INFO
CLEAR_TABLE_FIRST = False
BAD_FILES = ['3238491', '3396817', '3396818', '3386839', '3386904', '3390902', '3392301', '3393823', '3407187']
# Limit 1M rows, drop older than 20yrs
MAXROWS = 10000000
DATASET_ID = '60c7561e-6e4c-4e6c-9cc7-517f13022083'
def lastUpdateDate(dataset, date):
   apiUrl = 'http://api.resourcewatch.org/v1/dataset/{0}'.format(dataset)
   headers = {
   'Content-Type': 'application/json',
   'Authorization': os.getenv('apiToken')
   }
   body = {
       "dataLastUpdated": date.isoformat()
   }
   try:
       r = requests.patch(url = apiUrl, json = body, headers = headers)
       logging.info('[lastUpdated]: SUCCESS, '+ date.isoformat() +' status code '+str(r.status_code))
       return 0
   except Exception as e:
       logging.error('[lastUpdated]: '+str(e))

def list_available_files(url):
    page = requests.get(url).content
    soup = BeautifulSoup(page, 'html.parser')
    json_dict = soup.find_all('script')[2].get_text()
    dict = json.loads(json_dict)['distribution']
    fileList = [element['contentUrl'] for element in dict]
    return fileList

def get_file_id(file):
    return os.path.split(file)[1]


def processNewData(existing_ids, existing_files):
    file_list = list_available_files(SOURCE_URL)
    file_ids = [get_file_id(file) for file in file_list]
    file_base = 'https://dataverse.harvard.edu/api/access/datafile/'
    new_file_urls = []
    new_ids = []
    for file_id in file_ids:
        if file_id not in existing_files:
            new_file_urls.append(file_base+file_id)
            new_ids.append(file_id)
    logging.info('Number of new files: {}'.format(len(new_ids)))
    all_urls = range(len(new_file_urls))
    total_new = 0
    for file_num in all_urls:
        if new_ids[file_num] in BAD_FILES:
            continue
        file_url = new_file_urls[file_num]
        logging.info('Processing file {}'.format(new_ids[file_num]))
        new_rows = []
        res = urlopen(file_url)
        zipfile = ZipFile(BytesIO(res.read()))
        df = pd.read_csv(zipfile.open(zipfile.namelist()[0]), sep='\t')
        df['File ID'] = new_ids[file_num]

        for row_num in range(df.shape[0]):
            row = df.iloc[row_num]
            if not len(row):
                break
            elif pd.isna(row['Longitude']) or pd.isna(row['Latitude']):
                continue
            else:
                if row['Event ID'] not in existing_ids:
                    new_row = []
                    for field in CARTO_SCHEMA:
                        if field == 'uid':
                            new_row.append(str(row['Event ID']))
                        elif field == 'Event_ID':
                            new_row.append(str(row['Event ID']))
                        elif field == 'Event_ID':
                            new_row.append(str(row['Event ID']))
                        elif field == 'the_geom':
                            # Check for whether valid lat lon provided, will fail if either are ''
                            lon = float(row['Longitude'])
                            lat = float(row['Latitude'])
                            geometry = {
                                'type': 'Point',
                                'coordinates': [lon, lat]
                            }
                            new_row.append(geometry)
                        else:
                            # To fix trouble w/ cartosql not being able to handle '' for numeric:
                            val=row[field.replace('_', ' ')]
                            if val == '' or (type(val)==float and np.isnan(val)):
                                val=None
                            new_row.append(val)
                    new_rows.append(new_row)
        num_new = len(new_rows)

        if num_new:
            cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), new_rows, user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))
            total_new += num_new

    return total_new


##############################################################
# General logic for Carto
# should be the same for most tabular datasets
##############################################################

def createTableWithIndex(table, schema, id_field, time_field=''):
    '''Get existing ids or create table'''
    cartosql.createTable(table, schema)
    cartosql.createIndex(table, id_field, unique=True)
    if time_field:
        cartosql.createIndex(table, time_field)


def getIds(table, id_field):
    '''get ids from table'''
    r = cartosql.getFields(id_field, table, f='csv', user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))
    return np.unique(r.text.split('\r\n')[1:-1])


def deleteExcessRows(table, max_rows, time_field, max_age=''):
    '''Delete excess rows by age or count'''
    num_dropped = 0
    if isinstance(max_age, datetime.datetime):
        max_age = max_age.isoformat()

    # 1. delete by age
    if max_age:
        r = cartosql.deleteRows(table, "{} < '{}'".format(time_field, max_age))
        num_dropped = r.json()['total_rows']

    # 2. get sorted ids (old->new)
    r = cartosql.getFields('cartodb_id', table, order='{}'.format(time_field),
                           f='csv')
    ids = r.text.split('\r\n')[1:-1]

    # 3. delete excess
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[:-max_rows])
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))

def get_most_recent_date(url):
    page = requests.get(url).content
    soup = BeautifulSoup(page, 'html.parser')
    json_dict = soup.find_all('script')[2].get_text()
    most_recent_date_str = json.loads(json_dict)['distribution'][-1]['name'][0:8]
    most_recent_date = datetime.datetime.strptime(most_recent_date_str, '%Y%m%d')
    return most_recent_date

def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    if CLEAR_TABLE_FIRST:
        if cartosql.tableExists(CARTO_TABLE):
            cartosql.deleteRows(CARTO_TABLE, 'cartodb_id IS NOT NULL', user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))

    # 1. Check if table exists and create table
    existing_ids = []
    existing_files = []
    if cartosql.tableExists(CARTO_TABLE):
        logging.info('Fetching existing ids')
        existing_ids = getIds(CARTO_TABLE, UID_FIELD)
        existing_files = getIds(CARTO_TABLE, FILE_ID_FIELD)
    else:
        logging.info('Table {} does not exist, creating'.format(CARTO_TABLE))
        createTableWithIndex(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)

    # 2. Iterively fetch, parse and post new data
    new_count = processNewData(existing_ids, existing_files)
    total_count = len(existing_ids)
    logging.info('Total rows: {}, New: {}, Max: {}'.format(
        total_count, new_count, MAXROWS))

    # 3. Remove old observations
    deleteExcessRows(CARTO_TABLE, MAXROWS, TIME_FIELD)

    # Get most recent update date
    most_recent_date = get_most_recent_date(SOURCE_URL)
    lastUpdateDate(DATASET_ID, most_recent_date)

    logging.info('SUCCESS')
