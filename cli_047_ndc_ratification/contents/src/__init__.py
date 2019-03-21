import os
import logging
import sys
from collections import OrderedDict
import datetime
import cartosql
import requests
import pandas as pd

# Constants
SOURCE_URL = 'https://www.climatewatchdata.org/api/v1/data/ndc_content?indicator_ids[]=2366&page={page}'

CARTO_TABLE = 'cli_047_ndc_ratification'
CARTO_SCHEMA = OrderedDict([
    ('id', 'text'),
    ('iso_code3', 'text'),
    ('country', 'text'),
    ('value', 'text'),
])
UID_FIELD = 'id'
DATA_DIR = 'data'
LOG_LEVEL = logging.INFO

DATASET_ID = '136aab69-c625-4347-b16a-c2296ee5e99e'

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

def createTableWithIndex(table, schema, id_field, time_field=''):
    '''Get existing ids or create table'''
    cartosql.createTable(table, schema)
    cartosql.createIndex(table, id_field, unique=True)
    if time_field:
        cartosql.createIndex(table, time_field)

def processNewData():
    # Check if data is available, clear table and replace with new data
    page = 1
    r = requests.get(SOURCE_URL.format(page=page))
    raw_data = r.json()['data']
    if len(raw_data)>0:
        if cartosql.tableExists(CARTO_TABLE):
            cartosql.dropTable(CARTO_TABLE)
        logging.info('Updating {}'.format(CARTO_TABLE))
        createTableWithIndex(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD)
    else:
        logging.error("Source data missing. Table will not update.")
    new_rows = []
    while len(raw_data)>0:
        logging.info('Processing page {}'.format(page))
        df = pd.DataFrame(raw_data)
        for row_num in range(df.shape[0]):
            row = df.iloc[row_num]
            new_row = []
            for field in CARTO_SCHEMA:
                if field == 'uid':
                    new_row.append(row[UID_FIELD])
                else:
                    val = row[field] if row[field] != '' else None
                    new_row.append(val)
            new_rows.append(new_row)
        # go to the next page and check for data
        page += 1
        r = requests.get(SOURCE_URL.format(page=page))
        raw_data = r.json()['data']
    num_new = len(new_rows)
    if num_new:
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                CARTO_SCHEMA.values(), new_rows)
    return num_new

def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    # 2. Iterively fetch, parse and post new data
    new_count = processNewData()
    logging.info('New data has been uploaded')

    # If the source had data available and we updated the table, set last update time to now
    if new_count>0:
        lastUpdateDate(DATASET_ID, datetime.datetime.utcnow())

    logging.info('SUCCESS')
