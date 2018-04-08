import logging
import sys
import os
import time
import requests as req
from collections import OrderedDict
from datetime import datetime, timedelta
import cartosql

### Constants
SOURCE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={startTime}&endtime={endTime}&minsig={minSig}"

PROCESS_HISTORY = False
DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
SIGNIFICANT_THRESHOLD = 600

LOG_LEVEL = logging.INFO
CLEAR_TABLE_FIRST = False

### Table name and structure
CARTO_TABLE = 'dis_001_significant_earthquakes'
CARTO_SCHEMA = OrderedDict([
    ('uid', 'text'),
    ('the_geom', 'geometry'),
    ('depth_in_km', 'numeric'),
    ('datetime', 'timestamp'),
    ('mag', 'numeric'),
    ('place', 'text'),
    ('sig', 'numeric'),
    ('magType', 'text'),
    ('nst', 'numeric'),
    ('dmin', 'numeric'),
    ('rms', 'numeric'),
    ('gap', 'numeric'),
    ('tsunami', 'numeric'),
    ('felt', 'numeric'),
    ('cdi', 'numeric'),
    ('mmi', 'numeric'),
    ('net', 'text'),
    ('alert', 'text')
])
UID_FIELD = 'uid'
TIME_FIELD = 'datetime'

# Table limits
MAX_ROWS = 1000000
MAX_AGE = datetime.today() - timedelta(days=365*5)

###
## Carto code
###

def checkCreateTable(table, schema, id_field, time_field):
    '''
    Get existing ids or create table
    Return a list of existing ids in time order
    '''
    if cartosql.tableExists(table):
        logging.info('Table {} already exists'.format(table))
    else:
        logging.info('Creating Table {}'.format(table))
        cartosql.createTable(table, schema)
        cartosql.createIndex(table, id_field, unique=True)
        if id_field != time_field:
            cartosql.createIndex(table, time_field)

def deleteExcessRows(table, max_rows, time_field, max_age=''):
    '''Delete excess rows by age or count'''
    num_dropped = 0
    if isinstance(max_age, datetime):
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
    return num_dropped

###
## Accessing remote data
###

def genUID(lat,lon,depth,dt):
    return '{}_{}_{}_{}'.format(lat,lon,depth,dt)

def appendTimeFrame(existing_ids, startTime, endTime, new_data, new_ids):
    query = SOURCE_URL.format(startTime=startTime, endTime=endTime, minSig=SIGNIFICANT_THRESHOLD)
    res = req.get(query).json()
    num_start_ids = len(new_ids)
    for feature in res['features']:
        coords = feature['geometry']['coordinates']
        lat = coords[1]
        lon = coords[0]
        depth = coords[2]
        geom = {
            'type':'Point',
            'coordinates':[lon,lat]
        }

        props = feature['properties']
        dt = datetime.utcfromtimestamp(props['time']/1000).strftime(DATETIME_FORMAT)

        _uid = genUID(lat,lon,depth,dt)
        if _uid not in existing_ids + new_ids:
            new_ids.append(_uid)
            row = []
            for field in CARTO_SCHEMA:
                if field == 'uid':
                    row.append(_uid)
                elif field == 'the_geom':
                    row.append(geom)
                elif field == 'depth_in_km':
                    row.append(depth)
                elif field == 'datetime':
                    row.append(dt)
                else:
                    row.append(props[field])

            new_data.append(row)

    logging.info('{} new ids added'.format(len(new_ids) - num_start_ids))
    return new_data, new_ids

def processData(existing_ids):
    """
    Inputs: FTP SOURCE_URL and filename where data is stored, existing_ids not to duplicate
    Actions: Retrives data, dedupes and formats it, and adds to Carto table
    Output: Number of new rows added
    """
    new_data = []
    new_ids = []

    today = datetime.today()

    if PROCESS_HISTORY:

        startTime = MAX_AGE
        endTime = startTime + timedelta(days=31)
        while startTime < today:
            logging.info('Fetching data between {} and {}'.format(startTime, endTime))
            new_data, new_ids = appendTimeFrame(existing_ids, startTime, endTime, new_data, new_ids)
            startTime = endTime
            endTime = startTime + timedelta(days=31)

    else:
        # Use defaults of endpoint
        startTime = ''
        endTime = ''
        logging.info('Fetching data for last 30 days')

        new_data, new_ids = appendTimeFrame(existing_ids, startTime, endTime, new_data, new_ids)

    num_new = len(new_ids)
    if num_new:
        logging.info('Adding {} new records'.format(num_new))
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), new_data)

    return(num_new)

###
## Application code
###

def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)

    if CLEAR_TABLE_FIRST:
        if cartosql.tableExists(CARTO_TABLE):
            cartosql.dropTable(CARTO_TABLE)

    ### 1. Check if table exists, if not, create it
    checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)

    ### 2. Retrieve existing data
    r = cartosql.getFields(UID_FIELD, CARTO_TABLE, order='{} desc'.format(TIME_FIELD), f='csv')
    existing_ids = r.text.split('\r\n')[1:-1]
    num_existing = len(existing_ids)

    ### 3. Fetch data from FTP, dedupe, process
    num_new = processData(existing_ids)

    ### 4. Delete data to get back to MAX_ROWS
    num_dropped = deleteExcessRows(CARTO_TABLE, MAX_ROWS, TIME_FIELD, MAX_AGE)

    ### 5. Notify results
    total = num_existing + num_new - num_dropped
    logging.info('Existing rows: {},  New rows: {}, Max: {}'.format(total, num_new, MAX_ROWS))
    logging.info("SUCCESS")
