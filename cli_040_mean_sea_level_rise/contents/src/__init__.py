import logging
import sys
import os
import time
from collections import OrderedDict
import cartosql
import requests
import datetime
from bs4 import BeautifulSoup
from requests.auth import HTTPBasicAuth

### Constants
SOURCE_URL = "https://podaac-tools.jpl.nasa.gov/drive/files/allData/merged_alt/L2/TP_J1_OSTM/global_mean_sea_level/"

FILENAME_INDEX = -1
DATETIME_INDEX = 2
TIMEOUT = 300
ENCODING = 'utf-8'
STRICT = False
CLEAR_TABLE_FIRST = False

### Table name and structure
CARTO_TABLE = 'cli_040_mean_sea_level_rise'
CARTO_SCHEMA = OrderedDict([
    ('altimeter_type', 'numeric'),
    ('merged_file_cycle', 'numeric'),
    ('date', 'timestamp'),
    ('num_obs', 'numeric'),
    ('num_weighted_obs', 'numeric'),
    ('gmsl_no_gia', 'numeric'),
    ('sd_gmsl_no_gia', 'numeric'),
    ('gauss_filt_gmsl_no_gia', 'numeric'),
    ('gmsl_gia', 'numeric'),
    ('sd_gmsl_gia', 'numeric'),
    ('gauss_filt_gmsl_gia', 'numeric'),
    ('gauss_filt_gmsl_gia_ann_signal_removed', 'numeric')
])
UID_FIELD = 'date'
TIME_FIELD = 'date'

CARTO_USER = os.environ.get('CARTO_USER')
CARTO_KEY = os.environ.get('CARTO_KEY')

# Table limits
MAX_ROWS = 1000000
MAX_AGE = datetime.datetime.today() - datetime.timedelta(days=365*150)
DATASET_ID = 'f655d9b2-ea32-4753-9556-182fc6d3156b'
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

def cleanOldRows(table, time_field, max_age, date_format='%Y-%m-%d %H:%M:%S'):
    '''
    Delete excess rows by age
    Max_Age should be a datetime object or string
    Return number of dropped rows
    '''
    num_expired = 0
    if cartosql.tableExists(table):
        if isinstance(max_age, datetime.datetime):
            max_age = max_age.strftime(date_format)
        elif isinstance(max_age, str):
            logging.error('Max age must be expressed as a datetime.datetime object')

        r = cartosql.deleteRows(table, "{} < '{}'".format(time_field, max_age))
        num_expired = r.json()['total_rows']
    else:
        logging.error("{} table does not exist yet".format(table))

    return(num_expired)

def deleteExcessRows(table, max_rows, time_field):
    '''Delete rows to bring count down to max_rows'''
    num_dropped=0
    # 1. get sorted ids (old->new)
    r = cartosql.getFields('cartodb_id', table, order='{} desc'.format(time_field),
                           f='csv')
    ids = r.text.split('\r\n')[1:-1]

    # 2. delete excess
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[max_rows:])
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))

    return(num_dropped)

###
## Accessing remote data
###

def fetchDataFileName(SOURCE_URL):
    """
    Select the appropriate file from FTP to download data from
    """
    r = requests.get(SOURCE_URL, auth=HTTPBasicAuth(os.getenv('EARTHDATA_USER'), os.getenv('EARTHDATA_KEY')), stream=True)
    soup = BeautifulSoup(r.text, 'html.parser')
    s = soup.findAll('a')
    ALREADY_FOUND=False
    for item in s:
        if item['href'].endswith(".txt") and ("README" not in item['href']):
            if ALREADY_FOUND:
                logging.warning("There are multiple filenames which match criteria, passing most recent")
            filename = item['href'].split('/')[-1]
            ALREADY_FOUND=True

    if ALREADY_FOUND:
        logging.info("Selected filename: {}".format(filename))
    else:
        logging.warning("No valid filename found")

    return(filename)

def tryRetrieveData(SOURCE_URL, filename, TIMEOUT, ENCODING):
    # Optional logic in case this request fails with "unable to decode" response
    start = time.time()
    elapsed = 0
    resource_location = os.path.join(SOURCE_URL, filename)

    while elapsed < TIMEOUT:
        elapsed = time.time() - start
        try:
            with requests.get(resource_location, auth=HTTPBasicAuth(os.getenv('EARTHDATA_USER'), os.getenv('EARTHDATA_KEY')), stream=True) as f:
                res_rows = f.content.decode(ENCODING).splitlines()
                return(res_rows)
        except:
            logging.error("Unable to retrieve resource on this attempt.")
            time.sleep(5)

    logging.error("Unable to retrive resource before timeout of {} seconds".format(TIMEOUT))
    if STRICT:
        raise Exception("Unable to retrieve data from {}".format(resource_locations))
    return([])

# https://stackoverflow.com/questions/20911015/decimal-years-to-datetime-in-python
def decimalToDatetime(dec, date_pattern="%Y-%m-%d %H:%M:%S"):
    """
    Convert a decimal representation of a year to a desired string representation
    I.e. 2016.5 -> 2016-06-01 00:00:00
    """
    year = int(dec)
    rem = dec - year
    base = datetime.datetime(year, 1, 1)
    dt = base + datetime.timedelta(seconds=(base.replace(year=base.year + 1) - base).total_seconds() * rem)
    result = dt.strftime(date_pattern)
    return(result)

def insertIfNew(newUID, newValues, existing_ids, new_data):
    '''
    For new UID, values, check whether this is already in our table
    If not, add it
    Return new_ids and new_data
    '''
    seen_ids = existing_ids + list(new_data.keys())
    if newUID not in seen_ids:
        new_data[newUID] = newValues
        logging.debug("Adding {} data to table".format(newUID))
    else:
        logging.debug("{} data already in table".format(newUID))
    return(new_data)

def processData(SOURCE_URL, filename, existing_ids):
    """
    Inputs: FTP SOURCE_URL and filename where data is stored, existing_ids not to duplicate
    Actions: Retrives data, dedupes and formats it, and adds to Carto table
    Output: Number of new rows added
    """
    num_new = 0

    res_rows = tryRetrieveData(SOURCE_URL, filename, TIMEOUT, ENCODING)
    new_data = {}
    for row in res_rows:
        if not (row.startswith("HDR")):
            row = row.split()
            if len(row)==len(CARTO_SCHEMA):
                logging.debug("Processing row: {}".format(row))
                date = decimalToDatetime(float(row[DATETIME_INDEX]))
                row[DATETIME_INDEX] = date
                new_data = insertIfNew(date, row, existing_ids, new_data)
            else:
                logging.debug("Skipping row: {}".format(row))

    if len(new_data):
        num_new += len(new_data)
        new_data = list(new_data.values())
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), new_data)

    return(num_new)

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
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)

    if CLEAR_TABLE_FIRST:
        cartosql.deleteRows(CARTO_TABLE, 'cartodb_id IS NOT NULL', user=os.getenv('CARTO_USER'),
                            key=os.getenv('CARTO_KEY'))

    ### 1. Check if table exists, if not, create it
    checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)

    ### 2. Delete old rows
    num_expired = cleanOldRows(CARTO_TABLE, TIME_FIELD, MAX_AGE)

    ### 3. Retrieve existing data
    r = cartosql.getFields(UID_FIELD, CARTO_TABLE, order='{} desc'.format(TIME_FIELD), f='csv')
    existing_ids = r.text.split('\r\n')[1:-1]
    num_existing = len(existing_ids)

    logging.debug("First 10 IDs already in table: {}".format(existing_ids[:10]))

    ### 4. Fetch data from FTP, dedupe, process
    filename = fetchDataFileName(SOURCE_URL)
    num_new = processData(SOURCE_URL, filename, existing_ids)

    ### 5. Delete data to get back to MAX_ROWS
    num_deleted = deleteExcessRows(CARTO_TABLE, MAX_ROWS, TIME_FIELD)

    # Get most recent update date
    most_recent_date = get_most_recent_date(CARTO_TABLE)
    lastUpdateDate(DATASET_ID, most_recent_date)

    ### 6. Notify results
    logging.info('Expired rows: {}, Previous rows: {},  New rows: {}, Dropped rows: {}, Max: {}'.format(num_expired, num_existing, num_new, num_deleted, MAX_ROWS))
    logging.info("SUCCESS")
