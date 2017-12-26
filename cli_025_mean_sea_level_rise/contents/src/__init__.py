import logging
import sys
import os
import urllib.request
from collections import OrderedDict
from datetime import datetime, timedelta
import cartosql

### Constants
SOURCE_URL = "ftp://podaac.jpl.nasa.gov/allData/merged_alt/L2/TP_J1_OSTM/global_mean_sea_level/"
FILENAME_INDEX = 8
DATETIME_INDEX = 2

### Table name and structure
CARTO_TABLE = 'cli_025_mean_sea_level_rise'
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

###
## Accessing remote data
###

def fetchDataFileName(SOURCE_URL):
    """
    Select the appropriate file from FTP to download data from
    """
    with urllib.request.urlopen(SOURCE_URL) as f:
        ftp_contents = f.read().decode('utf-8').splitlines()

    filename = ""
    ALREADY_FOUND=False
    for fileline in ftp_contents:
        fileline = fileline.split()
        potential_filename = fileline[FILENAME_INDEX]
        # This is specific to this FTP
        if (potential_filename.endswith(".txt") and ("V4" in potential_filename)):
            if not ALREADY_FOUND:
                filename = potential_filename
                ALREADY_FOUND=True
            else:
                logging.warning("There are multiple filenames which match criteria, passing most recent")
                filename = potential_filename

    logging.info("Selected filename: " + filename)
    if not ALREADY_FOUND:
        logging.warning("No valid filename found")

    # Return the file name
    return(filename)

def processData(SOURCE_URL, filename, existing_ids):
    """
    Inputs: FTP SOURCE_URL and filename where data is stored, existing_ids not to duplicate
    Actions: Retrives data, dedupes and formats it, and adds to Carto table
    Output: Number of new rows added
    """
    with urllib.request.urlopen(os.path.join(SOURCE_URL, filename)) as f:
        res_rows = f.read().decode('utf-8').splitlines()

    # Do not keep header rows, or data observations marked 999
    deduped_formatted_rows = []
    for row in res_rows:
        if not (row.startswith("HDR")):
            potential_row = row.split()
            if len(potential_row)==len(CARTO_SCHEMA):
                date = decimalToDatetime(float(potential_row[DATETIME_INDEX]))
                if date not in existing_ids:
                    potential_row[DATETIME_INDEX] = date
                    deduped_formatted_rows.append(potential_row)
                    logging.debug("Adding " + date + " to table")
                else:
                    logging.debug(date + " already in table")

    logging.debug("First ten deduped, formatted rows from ftp: " + str(deduped_formatted_rows[:10]))

    if len(deduped_formatted_rows):
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA, deduped_formatted_rows)

    return(len(deduped_formatted_rows))

###
## Processing data for Carto
###

# https://stackoverflow.com/questions/20911015/decimal-years-to-datetime-in-python
def decimalToDatetime(dec, date_pattern="%Y-%m-%d %H:%M:%S"):
    """
    Convert a decimal representation of a year to a desired string representation
    I.e. 2016.5 -> 2016-06-01 00:00:00
    """
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

    ### 1. Check if table exists, if so, retrieve UIDs
    ## Q: If not getting the field for TIME_FIELD, can you still order by it?
    if cartosql.tableExists(CARTO_TABLE):
        r = cartosql.getFields(UID_FIELD, CARTO_TABLE, order='{} desc'.format(TIME_FIELD), f='csv')
        # quick read 1-column csv to list
        logging.debug("Table detected")
        logging.debug(r.text)
        existing_ids = r.text.split('\r\n')[1:-1]

    ### 2. If not, create table
    else:
        logging.info('Table {} does not exist'.format(CARTO_TABLE))
        cartosql.createTable(CARTO_TABLE, CARTO_SCHEMA)
        existing_ids = []

    logging.debug("First 10 IDs already in table: " + str(existing_ids[:10]))

    ### 3. Fetch data from FTP, dedupe, process
    filename = fetchDataFileName(SOURCE_URL)
    num_new_rows = processData(SOURCE_URL, filename, existing_ids)

    ### 4. Remove old to make room for new
    oldcount = len(existing_ids)
    logging.info('Previous rows: {}, New rows: {}, Max: {}'.format(oldcount, num_new_rows, MAX_ROWS))

    if oldcount + num_new_rows > MAX_ROWS:
        if MAX_ROWS > len(new_rows):
            # ids_already_in_table are arranged in increasing order
            # Drop all except the most recent ones we have room to keep
            drop_ids = existing_ids[(MAX_ROWS - len(new_rows)):]
            drop_response = cartosql.deleteRowsByIDs(CARTO_TABLE, UID_FIELD, drop_ids)
        else:
            logging.warning("There are more new rows than can be accommodated in the table. All existing_ids were dropped")
            drop_response = cartosql.deleteRowsByIDs(CARTO_TABLE, UID_FIELD, existing_ids)

        numdropped = drop_response.json()['total_rows']
        if numdropped > 0:
            logging.info('Dropped {} old rows'.format(numdropped))

    ###
    logging.info("SUCCESS")
