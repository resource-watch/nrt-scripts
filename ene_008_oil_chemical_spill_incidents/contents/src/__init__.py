import logging
import sys
import os

import requests
from collections import OrderedDict
import datetime
import cartosql
import csv
import requests

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
    ('commodity', 'text'),
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
DATASET_ID = '8746e75d-2749-405e-8f3b-0c12097860a1'
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

DATASET_ID = '8746e75d-2749-405e-8f3b-0c12097860a1'



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


# https://stackoverflow.com/questions/18897029/read-csv-file-from-url-into-python-3-x-csv-error-iterator-should-return-str
def processData(existing_ids):
    """
    Inputs: FTP SOURCE_URL and filename where data is stored, existing_ids not to duplicate
    Actions: Retrives data, dedupes and formats it, and adds to Carto table
    Output: Number of new rows added
    """
    new_rows = []

    res = requests.get(SOURCE_URL)
    csv_reader = csv.reader(res.iter_lines(decode_unicode=True))
    headers = next(csv_reader, None)
    idx = {k: v for v, k in enumerate(headers)}

    for row in csv_reader:
        #skip empty rows
        if not len(row):
            continue
        else:
            # This data set has some entries with breaks in the last column, which the csv_reader interprets
            # as an individual row. See if new id can be converted to an integer. If it can, it is probably a
            # new row.
            try:
                int(row[idx['id']])
                id = row[idx['id']]
                if id not in existing_ids:
                    logging.info('new row for {}'.format(id))
                    new_row = []
                    for field in CARTO_SCHEMA:
                        if field == 'uid':
                            new_row.append(row[idx['id']])
                        elif field == 'the_geom':
                            # Check for whether valid lat lon provided, will fail if either are ''
                            lon = row[idx['lon']]
                            lat = row[idx['lat']]
                            if lat and lon:
                                geometry = {
                                    'type': 'Point',
                                    'coordinates': [float(lon), float(lat)]
                                }
                                new_row.append(geometry)
                            else:
                                logging.debug('No lat long available for this data point - skipping!')
                                new_row.append(None)
                        else:
                            # To fix trouble w/ cartosql not being able to handle '' for numeric:
                            try:
                                val = row[idx[field]] if row[idx[field]] != '' else None
                                new_row.append(val)
                            except IndexError:
                                pass
                    new_rows.append(new_row)
            #If we can't convert to an integer, the last row probably got cut off.
            except ValueError:
                #  Using the id from the last entry, if this id was already in the Carto table, we will skip it
                if id in existing_ids:
                    pass
                # If it is a new id, we need to go fix that row.
                else:
                    # If the row is only one item, append the rest of the information to the last description.
                    if len(row) == 1:
                        new_rows[-1][-1] = new_rows[-1][-1] + ' ' + row[0].replace('\t', '')
                    # If several things are in the row, the break was probably mid-row.
                    elif len(row) > 1 and len(row) < 17:
                        # finish the last desciption
                        new_rows[-1][-1] = new_rows[-1][-1] + ' ' + row[0].replace('\t', '')
                        # append other items to row
                        new_row = new_rows[-1]
                        offset_factor = len(new_rows[-1])-1
                        for field in CARTO_SCHEMA:
                            if field == 'uid' or field == 'the_geom':
                                continue
                            try:
                                loc=idx[field]-offset_factor
                                if loc>0:
                                    val = row[loc] if row[loc] != '' else None
                                    new_row.append(val)
                            except IndexError:
                                pass
                        new_rows[-1]==new_row
                        '''
                        for item in row[1:]:
                            val = row[idx[field]] if row[idx[field]] != '' else None
                            new_row.append(val)
                            new_rows[-1].append(item)
                        '''

    num_new = len(new_rows)
    if num_new:
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                                 CARTO_SCHEMA.values(), new_rows)

    return num_new

###
# Carto code
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
    if isinstance(max_age, datetime.datetime):
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

#function to get most recent spill date from table
def get_most_recent_date(table):
    r = cartosql.getFields(TIME_FIELD, table, f='csv', post=True)
    dates = r.text.split('\r\n')[1:-1]
    dates.sort()
    most_recent_date = datetime.datetime.strptime(dates[-1], '%Y-%m-%d %H:%M:%S')
    return most_recent_date

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

    # Get most recent update date
    #if new rows were added to table, make today the most recent update date
    if num_new > 0:
        most_recent_date = datetime.datetime.utcnow()
        lastUpdateDate(DATASET_ID, most_recent_date)

    logging.info("SUCCESS")
