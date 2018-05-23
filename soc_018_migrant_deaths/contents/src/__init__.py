import logging
import sys
import os
import time
import pandas as pd
from collections import OrderedDict
from datetime import datetime, timedelta
from dateutil import parser
import cartosql

### Constants
SOURCE_URL = "https://missingmigrants.iom.int/global-figures/{year}/xls"
CLEAR_TABLE_FIRST = False
PROCESS_HISTORY = False
DATE_FORMAT = '%Y-%m-%d'
LOG_LEVEL = logging.DEBUG

### Table name and structure
CARTO_TABLE = 'soc_018_missing_migrants'
CARTO_SCHEMA = OrderedDict([
    ('uid', 'text'),
    ('the_geom', 'geometry'),
    ('Reported_Date', 'timestamp'),
    ('Region_of_Incident', 'text'),
    ('Number_Dead', 'numeric'),
    ('Number_Missing', 'numeric'),
    ('Total_Dead_and_Missing', 'numeric'),
    ('Number_of_survivors', 'numeric'),
    ('Number_of_Female', 'numeric'),
    ('Number_of_Male', 'numeric'),
    ('Number_of_Children', 'numeric'),
    ('Cause_of_death', 'text'),
    ('Location_Description', 'text'),
    ('Information_Source', 'text'),
    ('Migrant_Route', 'text'),
    ('URL', 'text'),
    ('UNSD_Geographical_Grouping', 'text'),
    ('Verification_level', 'text')
])
UID_FIELD = 'uid'
TIME_FIELD = 'Reported_Date'

# Table limits
MAX_ROWS = 1000000
MAX_AGE = datetime.today() - timedelta(days=365*10)

###
## Accessing remote data
###

def fetchAndFormatData(year):
    df = pd.read_excel(SOURCE_URL.format(year=year))
    df["Reported Date"] = df["Reported Date"].apply(lambda item: parser.parse(item, fuzzy=True).strftime(DATE_FORMAT))
    return list(df.columns), list(df.values)

def structure_row(headers, values):
    row = {}
    for key, val in zip(headers, values):
        row[key] = val
    return row

def clean_row(row):
    clean_row = []
    for entry in row:
        if entry == 'nan':
            clean_row.append(None)
        elif pd.isnull(entry):
            clean_row.append(None)
        else:
            clean_row.append(entry)
    return clean_row

def processData(existing_ids):
    """
    Inputs: FTP SOURCE_URL and filename where data is stored, existing_ids not to duplicate
    Actions: Retrives data, dedupes and formats it, and adds to Carto table
    Output: Number of new rows added
    """
    num_new = 0

    year = datetime.today().year
    logging.info("Fetching data for {}".format(year))
    headers, rows = fetchAndFormatData(year)
    logging.info("Num rows: {}".format(len(rows)))

    if PROCESS_HISTORY:
        year_history = 5
    else:
        year_history = 1

    count = 0
    while count < year_history:
        year -= 1
        logging.info("Fetching data for {}".format(year))
        try:
            more_headers, more_rows = fetchAndFormatData(year)
            # Check that headers for historical data match the newest data
            logging.info('More headers: {}'.format(more_headers))
            assert(headers == more_headers)
            rows.extend(more_rows)
            logging.info('Fetched additional data for year {}'.format(year))
        except:
            logging.info('Couldn\'t fetch data for year {}'.format(year))
        logging.info("Num rows: {}".format(len(rows)))
        count += 1
        
    new_rows = []
    for _row in rows:
        row = structure_row(headers, _row)
        if str(row['Web ID']) not in existing_ids:
            uid = row['Web ID']
            lat, lon = [float(loc.strip()) for loc in row['Location'].split(',')]
            geometry = {
                'type':'Point',
                'coordinates':[lon, lat]
            }

            new_row = []
            for field in CARTO_SCHEMA:
                if field == UID_FIELD:
                    new_row.append(uid)
                elif field == 'the_geom':
                    new_row.append(geometry)
                else:
                    new_row.append(row[field.replace('_', ' ')])

            new_row = clean_row(new_row)
            new_rows.append(new_row)

    if len(new_rows):
        num_new = len(new_rows)
        logging.debug("15 rows from middle of new_rows: {}".format(new_rows[1000:1015]))
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), new_rows)

    return(num_new)

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

def getFieldAsList(table, field, orderBy=''):
    assert isinstance(field, str), 'Field must be a single string'
    r = cartosql.getFields(field, table, order='{}'.format(orderBy),
                           f='csv')
    return(r.text.split('\r\n')[1:-1])

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
            logging.info('Dropping table')
            cartosql.dropTable(CARTO_TABLE)

    ### 1. Check if table exists and create table
    checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)
    existing_ids = getFieldAsList(CARTO_TABLE, UID_FIELD)
    num_existing = len(existing_ids)

    ### 2. Fetch and upload new data
    logging.info("Processing data")
    num_new = processData(existing_ids)
    num_total = num_existing + num_new

    ### 3. Notify results
    logging.info('Total rows: {}, New rows: {}, Max: {}'.format(num_total, num_new, MAX_ROWS))
    deleteExcessRows(CARTO_TABLE, MAX_ROWS, TIME_FIELD)

    logging.info("SUCCESS")
