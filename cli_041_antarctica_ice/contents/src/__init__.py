import logging
import sys
import os
import time
import urllib.request
from collections import OrderedDict
from datetime import datetime, timedelta
from dateutil import parser
import cartosql

### Constants
SOURCE_URL = 'ftp://podaac-ftp.jpl.nasa.gov/allData/tellus/L3/mascon/RL05/JPL/CRI/mass_variability_time_series/'
DATE_INDEX = 0
FILENAME_INDEX = -1
ENCODING = 'utf-8'
STRICT = False
TIMEOUT = 300

### Table name and structure
CARTO_TABLE = 'cli_041_antarctic_ice'
CARTO_SCHEMA = OrderedDict([
        ('date', 'timestamp'),
        ('mass', 'numeric'),
        ('uncertainty', 'text')
    ])
UID_FIELD = 'date'
TIME_FIELD = 'date'

CARTO_USER = os.environ.get('CARTO_USER')
CARTO_KEY = os.environ.get('CARTO_KEY')

# Table limits
MAX_ROWS = 100
MAX_AGE = datetime.today() - timedelta(days=365*20)
CLEAR_TABLE_FIRST = False

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
    return []

def deleteIndices(CARTO_TABLE):
    r = cartosql.sendSql("select * from pg_indexes where tablename='{}'".format(CARTO_TABLE))
    indexes = r.json()["rows"]
    logging.debug("Existing indices: {}".format(indexes))
    for index in indexes:
        try:
            sql = "alter table {} drop constraint {}".format(CARTO_TABLE, index["indexname"])
            r = cartosql.sendSql(sql)
            logging.debug(r.text)
        except:
            logging.error("couldn't drop constraint")
        try:
            sql = "drop index {}".format(index["indexname"])
            r = cartosql.sendSql(sql)
            logging.debug(r.text)
        except:
            logging.error("couldn't drop index")

def cleanOldRows(table, time_field, max_age, date_format='%Y-%m-%d %H:%M:%S'):
    '''
    Delete excess rows by age
    Max_Age should be a datetime object or string
    Return number of dropped rows
    '''
    num_expired = 0
    if cartosql.tableExists(table):
        if isinstance(max_age, datetime):
            max_age = max_age.strftime(date_format)
        elif isinstance(max_age, str):
            logging.error('Max age must be expressed as a datetime.datetime object')

        r = cartosql.deleteRows(table, "{} < '{}'".format(time_field, max_age))
        num_expired = r.json()['total_rows']
    else:
        logging.error("{} table does not exist yet".format(table))

    return(num_expired)

def makeRoomForNewData(table, schema, uidfield, max_rows, leftover_ids, new_ids):
    '''
    If new rows would push over limit, delete some first
    Will delete new_ids if there are too many for the table to hold
    '''
    seen_ids = leftover_ids + new_ids
    num_new_rows = len(new_ids)

    # Placeholder
    overflow_ids = []

    if len(seen_ids) > max_rows:
        if max_rows > num_new_rows:
            logging.debug("can accommodate all new_ids")
            drop_ids = leftover_ids[(max_rows - num_new_rows):]
            drop_response = cartosql.deleteRowsByIDs(table, drop_ids, id_field=uidfield, dtype=schema[uidfield])

            leftover_ids = leftover_ids[:(max_rows - num_new_rows)]
        else:
            logging.debug("cannot accommodate all new_ids")

            overflow_ids = new_ids[max_rows:]
            new_ids = new_ids[:max_rows]

            drop_ids = leftover_ids + overflow_ids
            drop_response = cartosql.deleteRowsByIDs(table, drop_ids, id_field=uidfield, dtype=schema[uidfield])

            leftover_ids = []

            num_overflow = len(overflow_ids)
            logging.warning("Drop all existing_ids, and enough oldest new ids to have MAX_ROWS number of final entries in the table.")
            logging.warning("{} new data values were lost.".format(num_overflow))

        numdropped = drop_response.json()['total_rows']
        if numdropped > 0:
            logging.info('Dropped {} old rows'.format(numdropped))

    return(leftover_ids, new_ids, overflow_ids)



###
## Accessing remote data
###

def fetchDataFileName(SOURCE_URL):
    """
    Select the appropriate file from FTP to download data from
    """
    with urllib.request.urlopen(SOURCE_URL) as f:
        ftp_contents = f.read().decode('utf-8').splitlines()

    filename = ''
    ALREADY_FOUND=False
    for fileline in ftp_contents:
        fileline = fileline.split()
        logging.debug("Fileline as formatted on server: {}".format(fileline))
        potential_filename = fileline[FILENAME_INDEX]

        ###
        ## Set conditions for finding correct file name for this FTP
        ###

        if (potential_filename.endswith(".txt") and ("antarctica" in potential_filename)):
            if not ALREADY_FOUND:
                filename = potential_filename
                ALREADY_FOUND=True
            else:
                logging.warning("There are multiple filenames which match criteria, passing most recent")
                filename = potential_filename

    logging.info("Selected filename: {}".format(filename))
    if not ALREADY_FOUND:
        logging.warning("No valid filename found")

    # Return the file name
    return(filename)

def tryRetrieveData(SOURCE_URL, filename, TIMEOUT, ENCODING):
    # Optional logic in case this request fails with "unable to decode" response
    start = time.time()
    elapsed = 0
    resource_location = os.path.join(SOURCE_URL, filename)

    while elapsed < TIMEOUT:
        elapsed = time.time() - start
        try:
            with urllib.request.urlopen(resource_location) as f:
                res_rows = f.read().decode(ENCODING).splitlines()
                return(res_rows)
        except:
            logging.error("Unable to retrieve resource on this attempt.")
            time.sleep(5)

    logging.error("Unable to retrive resource before timeout of {} seconds".format(TIMEOUT))
    if STRICT:
        raise Exception("Unable to retrieve data from {}".format(resource_locations))
    return([])

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

def recentEnough(date, MAX_AGE):
    '''Assume date is a string, MAX_AGE a datetime'''
    return(parser.parse(date) > MAX_AGE)

def processData(SOURCE_URL, filename, existing_ids, max_rows):
    """
    Inputs: FTP SOURCE_URL and filename where data is stored, existing_ids not to duplicate
    Actions: Retrives data, dedupes and formats it, and adds to Carto table
    Output: Number of new rows added
    """

    # Totals, persist throughout any pagination in next step
    leftover_ids = existing_ids.copy()
    num_new = 0
    num_overflow = 0

    ### Specific to each page/chunk in data processing

    res_rows = tryRetrieveData(SOURCE_URL, filename, TIMEOUT, ENCODING)
    new_data = {}
    for row in res_rows:
        if not (row.startswith("HDR")):
            row = row.split()
            if len(row)==len(CARTO_SCHEMA):
                logging.debug("Processing row: {}".format(row))
                # Pull data available in each line
                date = decimalToDatetime(row[DATE_INDEX])
                if recentEnough(date, MAX_AGE):
                    MASS_INDEX = 1
                    UNCERTAINTY_INDEX = 2
                    values = [date, row[MASS_INDEX], row[UNCERTAINTY_INDEX]]
                    new_data = insertIfNew(date, values, leftover_ids, new_data)
            else:
                logging.debug("Skipping row: {}".format(row))

    if len(new_data):
        # Check whether should delete to make room
        new_ids = list(new_data.keys())
        leftover_ids, new_ids, overflow_ids = makeRoomForNewData(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, max_rows, leftover_ids, new_ids)
        for overflow in overflow_ids:
            new_data.pop(overflow)

        num_overflow += len(overflow_ids)
        num_new += len(new_ids)
        new_data = list(new_data.values())
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), new_data)

    ### End page/chunk processing

    num_leftover = len(leftover_ids)
    return(num_leftover, num_new, num_overflow)


###
## Processing data for Carto
###

# https://stackoverflow.com/questions/20911015/decimal-years-to-datetime-in-python
def decimalToDatetime(dec, date_pattern="%Y-%m-%d %H:%M:%S"):
    """
    Convert a decimal representation of a year to a desired string representation
    I.e. 2016.5 -> 2016-06-01 00:00:00
    """
    dec = float(dec)
    year = int(dec)
    rem = dec - year
    base = datetime(year, 1, 1)
    dt = base + timedelta(seconds=(base.replace(year=base.year + 1) - base).total_seconds() * rem)
    result = dt.strftime(date_pattern)
    return(result)

###
## Application code
###

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

    if CLEAR_TABLE_FIRST:
        cartosql.dropTable(CARTO_TABLE)
        deleteIndices(CARTO_TABLE)

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
    num_leftover, num_new, num_overflow = processData(SOURCE_URL, filename, existing_ids, MAX_ROWS)

    ### 5. Notify results
    num_overwritten = num_existing - num_leftover
    logging.info('Expired rows: {}, Previous rows: {}, Overwritten rows: {}'.format(num_expired, num_existing, num_overwritten))
    logging.info('Overflow rows: {}, New rows: {}, Max: {}'.format(num_overflow, num_new, MAX_ROWS))
    ###
    logging.info("SUCCESS")
