import logging
import sys
import os
import urllib.request
from collections import OrderedDict
from datetime import datetime, timedelta
import cartosql

### Constants
SOURCE_URL = 'https://climate.nasa.gov/system/internal_resources/details/original/'
# One or both of these may not be needed
DATETIME_INDEX = 0
FILENAME_INDEX = 0

### Table name and structure
CARTO_TABLE = 'cli_043_arctic_sea_ice_minimum'
CARTO_SCHEMA = OrderedDict([
        ('UID', 'text'),
        ('date', 'timestamp'),
        ('value_type', 'text'),
        ('value', 'numeric')
    ])

UID_FIELD = 'UID'
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

    filename = ''
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

            # Pull data available in each line
            AREA_VALUE_INDEX = 3
            area_value = potential_row[AREA_VALUE_INDEX]
            EXTENT_VALUE_INDEX = 7
            extent_value = potential_row[EXTENT_VALUE_INDEX]

            # Pull times associated with those data
            dttm_elems_area = {
                "year_ix":0,
                "month_ix":1,
                "day_ix":2
            }
            dttm_elems_extent = {
                "year_ix":4,
                "month_ix":5,
                "day_ix":6
            }

            area_date = fix_datetime_UTC(potential_row, dttm_elems_area)
            extent_date = fix_datetime_UTC(potential_row, dttm_elems_extent)

            area_uid = genUID("area", area_date)
            extent_uid = genUID("extent", extent_date)

            if area_date not in existing_ids:
                deduped_formatted_rows.append([areaUID, area_date, "minimum_area_measurement", area_value])
                logging.debug("Adding " + area_date + " area data to table")
            else:
                logging.debug(area_date + " area data already in table")

            if extent_date not in existing_ids:
                deduped_formatted_rows.append([extentUID, extent_date, "minimum_extent_measurement", extent_value])
                logging.debug("Adding " + extent_date + " extent data to table")
            else:
                logging.debug(date_area + " extent data already in table")

    logging.debug("First ten deduped, formatted rows from ftp: " + str(deduped_formatted_rows[:10]))

    if len(deduped_formatted_rows):
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA, deduped_formatted_rows)

    return(len(deduped_formatted_rows))

###
## Processing data for Carto
###

def genUID(value_type, value_date):
    return("_".join([str(value_type), str(value_date)].replace(" ", "_")))

def fix_datetime_UTC(row, construct_datetime_manually=True,
                     dttm_elems={},
                     dttm_columnz=None,
                     dttm_pattern="%Y-%m-%d %H:%M:%S"):
    """
    Desired datetime format: 2017-12-08T15:16:03Z
    Corresponding date_pattern for strftime: %Y-%m-%dT%H:%M:%SZ

    If date_elems_in_sep_columns=True, then there will be a dictionary date_elems
    That at least contains the following elements:
    date_elems = {"year_col":`int or string`,"month_col":`int or string`,"day_col":`int or string`}
    OPTIONAL KEYS IN date_elems:
    * hour_col
    * min_col
    * sec_col
    * milli_col
    * micro_col
    * tz_col

    Depends on:
    from dateutil import parser
    """
    default_date = parser.parse("January 1 1900 00:00:00")

    # Mutually exclusive to provide broken down datetime factors,
    # and either a date, time, or datetime object
    if construct_datetime_manually:
        assert(type(dttm_elems)==dict)
        assert(dttm_columnz==None)

        if "year_ix" in dttm_elems:
            year = int(row[dttm_elems["year_ix"]])
        else:
            year = 1900
            logging.warning("Default year set to 1900")

        if "month_ix" in dttm_elems:
            month = int(row[dttm_elems["month_ix"]])
        else:
            month = 1900
            logging.warning("Default mon set to January")

        if "day_ix" not in dttm_elems:
            day = int(row[dttm_elems["day_ix"]])
        else:
            day = 1
            logging.warning("Default day set to first of month")

        dt = datetime.datetime(year=year,month=month,day=day)
        if "hour_ix" in dttm_elems:
            dt = dt.replace(hour=int(row[dttm_elems["hour_ix"]]))
        if "min_ix" in dttm_elems:
            dt = dt.replace(minute=int(row[dttm_elems["min_ix"]]))
        if "sec_ix" in dttm_elems:
            dt = dt.replace(second=int(row[dttm_elems["sec_ix"]]))
        if "milli_ix" in dttm_elems:
            dt = dt.replace(milliseconds=int(row[dttm_elems["milli_ix"]]))
        if "micro_ix" in dttm_elems:
            dt = dt.replace(microseconds=int(row[dttm_elems["micro_ix"]]))
        if "tzinfo_ix" in dttm_elems:
            timezone = pytz.timezone(str(row[dttm_elems["tzinfo_ix"]]))
            dt = timezone.localize(dt)

        formatted_date = dt.strftime(dttm_pattern)
    else:
        # Make sure dttm_columnz was provided
        assert(dttm_columnz!=None)
        default_date = datetime.datetime(year=1990, month=1, day=1)
        # If dttm_columnz is not a list, it must be a single list index, type int
        if type(dttm_columnz) != list:
            assert(type(dttm_columns) == int)
            formatted_date = parser.parse(row[dttm_columnz], default=default_date).strftime(dttm_pattern)
            # Need to provide the default parameter to parser.parse so that missing entries don't default to current date

        elif len(dttm_columnz)>1:
            # Concatenate these entries with a space in between, use dateutil.parser
            dttm_contents = " ".join(row[dttm_columnz])
            formatted_date = parser.parse(dttm_contents, default=default_date).strftime(dttm_pattern))

    return(formatted_date)

###
## Application code
###

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)

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
    #filename = fetchDataFileName(SOURCE_URL)
    filename = "1270_minimum_extents_and_area_north_SBA_reg_20171001_2_.txt"
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
