import logging
import sys
import os
import urllib.request
from collections import OrderedDict
from datetime import datetime, timedelta
from dateutil import parser
import cartosql

### Constants
SOURCE_URL = 'ftp://aftp.cmdl.noaa.gov/products/trends/co2/'
DATE_INDEX = 2
FILENAME_INDEX = -1

### Table name and structure
CARTO_TABLE = 'cli_045_carbon_dioxide_concentration'
CARTO_SCHEMA = OrderedDict([
        ('date', 'timestamp'),
        ('average', 'numeric'),
        ('interpolated', 'numeric'),
        ('season_adjusted_trend', 'numeric'),
        ('num_days', 'numeric')
    ])
UID_FIELD = 'date'
TIME_FIELD = 'date'

CARTO_USER = os.environ.get('CARTO_USER')
CARTO_KEY = os.environ.get('CARTO_KEY')

# Table limits
MAX_ROWS = 1000000
CLEAR_TABLE_FIRST = False
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
        # Current: "co2_mm_mlo.txt". There are multiple files on this server -
        # Liz: we're sure this is the one we want? Are there others?
        # Weekly data available at co2_weekly_mlo.txt
        if (potential_filename.endswith(".txt") and ("co2_mm_mlo" in potential_filename)):
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
    new_ids = []
    for row in res_rows:
        ###
        ## CHANGE TO REFLECT CRITERIA FOR KEEPING ROWS FROM THIS DATA SOURCE
        ###
        if not (row.startswith("#")):
            row = row.split()
            ###
            ## CHANGE TO REFLECT CRITERIA FOR KEEPING ROWS FROM THIS DATA SOURCE
            ###
            if len(row)==7:
                logging.debug("Processing row: {}".format(row))
                # Pull data available in each line
                date = decimalToDatetime(row[DATE_INDEX])

                AVERAGE_INDEX = 3
                INTERPOLATED_INDEX = 4
                TREND_INDEX = 5
                NUM_DAYS_INDEX = 6
                values = [date, row[AVERAGE_INDEX], row[INTERPOLATED_INDEX],
                            row[TREND_INDEX], row[NUM_DAYS_INDEX]]

                seen_ids = existing_ids + new_ids
                if date not in seen_ids:
                    deduped_formatted_rows.append(values)
                    logging.debug("Adding {} data to table".format(date))
                    new_ids.append(date)
                else:
                    logging.debug("{} data already in table".format(date))
            else:
                logging.debug("Skipping row: {}".format(row))

    logging.debug("First ten deduped, formatted rows from ftp: {}".format(deduped_formatted_rows[:10]))

    if len(deduped_formatted_rows):
        cartosql.blockInsertRows(CARTO_TABLE, list(CARTO_SCHEMA.keys()), list(CARTO_SCHEMA.values()), deduped_formatted_rows)

    return(new_ids)

###
## Processing data for Carto
###

def genUID(value_type, value_date):
    return("_".join([str(value_type), str(value_date)]).replace(" ", "_"))

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
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)

    if CLEAR_TABLE_FIRST:
        cartosql.dropTable(CARTO_TABLE)

    ### 1. Check if table exists, if so, retrieve UIDs
    ## Q: If not getting the field for TIME_FIELD, can you still order by it?
    if cartosql.tableExists(CARTO_TABLE):
        r = cartosql.getFields(UID_FIELD, CARTO_TABLE, order='{} desc'.format(TIME_FIELD), f='csv')
        # quick read 1-column csv to list
        logging.debug("Table detected")
        logging.debug("Carto's response: {}".format(r.text))
        existing_ids = r.text.split('\r\n')[1:-1]

    ### 2. If not, create table
    else:
        logging.info('Table {} does not exist, creating now'.format(CARTO_TABLE))
        cartosql.createTable(CARTO_TABLE, CARTO_SCHEMA)
        existing_ids = []

    logging.debug("First 10 IDs already in table: {}".format(existing_ids[:10]))

    ### 3. Fetch data from FTP, dedupe, process
    filename = fetchDataFileName(SOURCE_URL)
    new_ids = processData(SOURCE_URL, filename, existing_ids)

    ### 4. Remove old to make room for new
    oldcount = len(existing_ids)
    num_new_rows = len(new_ids)
    logging.info('Previous rows: {}, New rows: {}, Max: {}'.format(oldcount, num_new_rows, MAX_ROWS))

    if oldcount + num_new_rows > MAX_ROWS:
        if MAX_ROWS > num_new_rows:
            # ids_already_in_table are arranged in increasing order
            # Drop all except the most recent ones we have room to keep
            drop_ids = existing_ids[(MAX_ROWS - num_new_rows):]
            drop_response = cartosql.deleteRowsByIDs(CARTO_TABLE, drop_ids, id_field=UID_FIELD, dtype=CARTO_SCHEMA[UID_FIELD])
        else:
            num_lost_new_data = num_new_rows - MAX_ROWS
            logging.warning("Drop all existing_ids, and enough oldest new ids to have MAX_ROWS number of final entries in the table.")
            logging.warning("{} new data values were lost.".format(num_lost_new_data))

            new_ids.sort(reverse=True)
            drop_ids = existing_ids + new_ids[MAX_ROWS:]
            drop_response = cartosql.deleteRowsByIDs(CARTO_TABLE, drop_ids, id_field=UID_FIELD, dtype=CARTO_SCHEMA[UID_FIELD])

        numdropped = drop_response.json()['total_rows']
        if numdropped > 0:
            logging.info('Dropped {} old rows'.format(numdropped))

    ###
    logging.info("SUCCESS")
