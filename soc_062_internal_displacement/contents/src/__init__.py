import os
import logging
import sys
from collections import OrderedDict
import datetime
import cartosql
import requests


# Constants
LATEST_URL = 'https://backend.idmcdb.org/data/idus_view_flat'
MIN_PAGES = 15
MAX_PAGES = 400
CLEAR_TABLE_FIRST = True

CARTO_TABLE = 'soc_062_internal_displacement'
CARTO_SCHEMA = OrderedDict([
    ("the_geom", "geometry"),
    ("id", "int"),
    ("country", "text"),
    ("iso3", "text"),
    ("latitude", "int"),
    ("longitude", "int"),
    ("displacement_type", "text"),
    ("figure", "int"),
    ("qualifier", "text"),
    ("displacement_date_timestamp", "timestamp"),
    ("displacement_date", "text"),
    ("displacement_start_date", "text"),
    ("displacement_end_date", "text"),
    ("year", "int"),
    ("event_name", "text"),
    ("event_start_date", "text"),
    ("event_end_date", "text"),
    ("category", "text"),
    ("subcategory", "text"),
    ("type", "text"),
    ("subtype", "text"),
    ("standard_popup_text", "text"),
    ("link", "text")
])
UID_FIELD = 'id'
TIME_FIELD = 'displacement_date_timestamp'
DATA_DIR = 'data'
LOG_LEVEL = logging.INFO

# Limit 1M rows
MAXROWS = 10000000
DATASET_ID = 'd2f6245d-5b9b-4508-874b-d42a2be7d058'

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


def genUID(obs):
    '''Generate unique id'''
    return str(obs[UID_FIELD])


def processNewData(exclude_ids):
    '''
    Iterively fetch parse and post new data
    '''
    r = requests.get(LATEST_URL)
    new_ids = []

    # 1. Fetch new data
    logging.info("Fetching events")
    # 2. Parse data excluding existing observations
    new_rows = []
    for obs in r.json():
        uid = genUID(obs)
        if uid not in exclude_ids + new_ids:
            new_ids.append(uid)
            row = []
            for field in CARTO_SCHEMA.keys():
                if field == 'the_geom':
                    # construct geojson geometry
                    geom = {
                        "type": "Point",
                        "coordinates": [
                            obs['longitude'],
                            obs['latitude']
                        ]
                    }
                    row.append(geom)
                elif field == 'displacement_date_timestamp':
                    dt=datetime.datetime.strptime(obs['displacement_date'], '%Y-%m-%d')
                    if dt>datetime.datetime.today():
                        fixed_date = str(obs['year']) + obs['displacement_date'][4:]
                        dt = datetime.datetime.strptime(fixed_date, '%Y-%m-%d')
                    row.append(dt)
                elif field == 'standard_popup_text':
                    text=obs[field].split('<a href="')[0]
                    row.append(text)
                elif field == 'link':
                    link = obs['standard_popup_text'].split('<a href="')[1]
                    link = link.split('"')[0]
                    row.append(link)
                else:
                    try:
                        row.append(obs[field])
                    except:
                        logging.debug('{} not available for this row'.format(field))
                        row.append('')
            new_rows.append(row)
    # 3. Insert new rows
    new_count = len(new_rows)
    if new_count:
        logging.info('Pushing {} new rows'.format(new_count))
        cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                            CARTO_SCHEMA.values(), new_rows)
    return new_ids


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
    r = cartosql.getFields(id_field, table, f='csv')
    return r.text.split('\r\n')[1:-1]


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

def get_most_recent_date(table):
    r = cartosql.getFields(TIME_FIELD, table, f='csv', post=True)
    dates = r.text.split('\r\n')[1:-1]
    dates.sort()
    most_recent_date = datetime.datetime.strptime(dates[-1], '%Y-%m-%d %H:%M:%S')
    return most_recent_date

def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    if CLEAR_TABLE_FIRST:
        if cartosql.tableExists(CARTO_TABLE):
            cartosql.dropTable(CARTO_TABLE)

    # 1. Check if table exists and create table
    existing_ids = []
    if cartosql.tableExists(CARTO_TABLE):
        logging.info('Fetching existing ids')
        existing_ids = getIds(CARTO_TABLE, UID_FIELD)
    else:
        logging.info('Table {} does not exist, creating'.format(CARTO_TABLE))
        createTableWithIndex(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)

    # 2. Iterively fetch, parse and post new data
    new_ids = processNewData(existing_ids)

    new_count = len(new_ids)
    existing_count = new_count + len(existing_ids)
    logging.info('Total rows: {}, New: {}, Max: {}'.format(
        existing_count, new_count, MAXROWS))

    # 3. Remove old observations
    deleteExcessRows(CARTO_TABLE, MAXROWS, TIME_FIELD)

    # Get most recent update date
    most_recent_date = get_most_recent_date(CARTO_TABLE)
    logging.info(most_recent_date)
    lastUpdateDate(DATASET_ID, most_recent_date)

    logging.info('SUCCESS')
