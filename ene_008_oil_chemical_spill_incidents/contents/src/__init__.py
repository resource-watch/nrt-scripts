import logging
import sys
import os
import time
import urllib.request
from collections import OrderedDict
from datetime import datetime, timedelta
import cartosql
import csv

### Constants
SOURCE_URL = "https://incidentnews.noaa.gov/raw/incidents.csv"
CLEAR_TABLE_FIRST = False
ENCODING = 'utf-8'
LOG_LEVEL = logging.INFO

### Table name and structure
CARTO_TABLE = 'ene_008_us_oil_chemical_spills'
CARTO_SCHEMA = OrderedDict([
    ('uid', 'numeric'),
    ('the_geom', 'geometry'),
    ('open_date', 'timestamp'),
    ('name', 'text'),
    ('location', 'text'),
    ('threat', 'text'),
    ('tags', 'text'),
    ('commodity' ,'text'),
    ('measure_skim', 'numeric'),
    ('measure_shore', 'numeric'),
    ('measure_bio', 'numeric'),
    ('measure_disperse', 'numeric'),
    ('measure_burn', 'numeric'),
    ('max_ptl_release_gallons', 'numeric'),
    ('posts', 'numeric'),
    ('description', 'text')
])

UID_FIELD = 'uid'
TIME_FIELD = 'open_date'

# Table limits
MAX_ROWS = 1000000

###
## Accessing remote data
###

def structure_row(headers, values):
    logging.debug("Headers: " + str(headers))
    logging.debug("Values: " + str(values))
    row = {}
    for key, val in zip(headers, values):
        row[key] = val
    return row

# Deal w/ CSV being sent in Bytes, not String type:
# https://stackoverflow.com/questions/18897029/read-csv-file-from-url-into-python-3-x-csv-error-iterator-should-return-str
def processData(existing_ids):
    """
    Inputs: FTP SOURCE_URL and filename where data is stored, existing_ids not to duplicate
    Actions: Retrives data, dedupes and formats it, and adds to Carto table
    Output: Number of new rows added
    """
    new_rows = []
    num_new = 0

    # Use .splitlines():
    # https://stackoverflow.com/questions/21351882/reading-data-from-a-csv-file-online-in-python-3
    csv_stream = urllib.request.urlopen(SOURCE_URL)
    csv_reader = csv.reader(csv_stream.read().decode(ENCODING).splitlines())
    # See comment under John Machin's answer:
    # https://stackoverflow.com/questions/3428532/how-to-import-a-csv-file-using-python-with-headers-intact-where-first-column-is
    headers = next(csv_reader, None)

    for _row in csv_reader:
        if len(headers) == len(_row):
            row = structure_row(headers, _row)
            if row['id'] not in existing_ids:

                new_row = []
                for field in CARTO_SCHEMA:
                    if field == 'uid':
                        new_row.append(row['id'])
                    elif field == 'the_geom':
                        # Check for whether valid lat lon provided, will fail if either are ''
                        try:
                            lon = float(row['lon'])
                            lat = float(row['lat'])
                            geometry = {
                                'type':'Point',
                                'coordinates':[lon, lat]
                            }
                            new_row.append(geometry)
                        except:
                            logging.error('No lat long available for this data point - skipping!')
                            new_row.append(None)
                    else:
                        # To fix trouble w/ cartosql not being able to handle '':
                        val = row[field]
                        if val:
                            new_row.append(val)
                        else:
                            new_row.append(None)

                new_rows.append(new_row)


    if len(new_rows):
        num_new = len(new_rows)
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), new_rows)

    return num_new

###
## Carto code
###

def getFieldAsList(table, field, orderBy=''):
    assert isinstance(field, str), 'Field must be a single string'
    r = cartosql.getFields(field, table, order='{}'.format(orderBy),
                           f='csv')
    return(r.text.split('\r\n')[1:-1])

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
    ids = getFieldAsList(CARTO_TABLE, 'cartodb_id', orderBy=''.format(TIME_FIELD))

    # 3. delete excess
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[:-max_rows])
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))

###
## Application code
###

def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)

    if CLEAR_TABLE_FIRST:
        if cartosql.tableExists(CARTO_TABLE):
            cartosql.dropTable(CARTO_TABLE)

    ### 1. Check if table exists and create table
    checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)
    existing_ids = getFieldAsList(CARTO_TABLE, UID_FIELD)
    num_existing = len(existing_ids)

    ### 2. Fetch data from FTP, dedupe, process
    num_new = processData(existing_ids)
    num_total = num_existing + num_new

    ### 3. Notify results
    logging.info('Total rows: {}, New rows: {}, Max: {}'.format(num_total, num_new, MAX_ROWS))
    deleteExcessRows(CARTO_TABLE, MAX_ROWS, TIME_FIELD)

    logging.info("SUCCESS")
