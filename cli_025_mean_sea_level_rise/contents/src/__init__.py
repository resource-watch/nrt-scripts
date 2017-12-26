import logging
import sys
import os
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

import urllib.request

# If not running in nrt-container, then .env variables must be symlinked
import src.utilities.carto as carto
import src.utilities.misc as misc
import src.utilities.cli_025 as cli_025

from collections import OrderedDict

# SOURCE_URL = "ftp://podaac.jpl.nasa.gov/allData/merged_alt/L2/TP_J1_OSTM/global_mean_sea_level/"

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

MAX_ROWS = 1000000
SOURCE_URL = "ftp://podaac.jpl.nasa.gov/allData/merged_alt/L2/TP_J1_OSTM/global_mean_sea_level/"

def fetchData(SOURCE_URL):
    # Read the files that are on the FTP
    file_name = cli_025.fetchDataFileName(SOURCE_URL)
    with urllib.request.urlopen(os.path.join(SOURCE_URL, file_name)) as f:
        res_rows = f.read().decode('utf-8').splitlines()

    # Do not keep header rows, or data observations marked 999
    rows = []
    for row in res_rows:
        if not (row.startswith("HDR") or row.startswith("999")):
            potential_row = row.split()
            if len(potential_row)==len(CARTO_SCHEMA):
                rows.append(potential_row)

    logging.debug("First ten rows from ftp: " + str(rows[:10]))
    return(rows)

def parseData(rows, ids_already_in_table):
    deduped_rows = []
    for row in rows:
        # Reformat date information, in index 2 for each row
        date = cli_025.dec_to_datetime(float(row[2]))
        if date not in ids_already_in_table:
            row[2] = date
            deduped_rows.append(row)
    logging.debug("First ten deduped, date formatted rows: " + str(deduped_rows[:10]))
    return(deduped_rows)

def uploadData(rows):
    if len(rows):
        carto.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA, rows)

def main():
    ### 1. Check if table exists, if so, retrieve UIDs
    ## Q: If not getting the field for TIME_FIELD, can you still order by it?
    if carto.tableExists(CARTO_TABLE):
        r = carto.getFields(UID_FIELD, CARTO_TABLE, order=TIME_FIELD, f='csv')
        # quick read 1-column csv to list
        ids_already_in_table = r.split('\r\n')[1:-1]

    ### 2. If not, create table
    else:
        logging.info('Table {} does not exist'.format(CARTO_TABLE))
        carto.createTable(CARTO_TABLE, CARTO_SCHEMA)
        ids_already_in_table = []

    logging.debug("First 10 IDs already in table: " + str(ids_already_in_table[:10]))

    ### 3. Fetch data from FTP
    rows = fetchData(SOURCE_URL)

    ### 4. Parse data into correct format, dedupe rows
    new_rows = parseData(rows, ids_already_in_table)

    ### 5. Remove old to make room for new
    logging.info('Existing rows count: {}, New rows: {}, Max: {}'.format(len(ids_already_in_table), len(new_rows), MAX_ROWS))
    if len(ids_already_in_table) + len(new_rows) > MAX_ROWS:
        if MAX_ROWS > len(new_rows):
            # ids_already_in_table are arranged in increasing order
            # Drop all except the most recent ones we have room to keep
            drop_ids = ids_already_in_table[:-(MAX_ROWS - len(new_rows))]
            carto.deleteRowsByIDs(CARTO_TABLE, UID_FIELD, drop_ids)
            logging.debug("Deleted obsevations by id:")
            logging.debug(drop_ids)
            logging.info("Deleted " + str(len(drop_ids)) + " rows from table.")
        else:
            carto.truncateTable(CARTO_TABLE)
            logging.warning("There are more new rows than can be accommodated in the table")
            logging.warning("Only MAX_ROWS number of rows will be kept")
            new_rows = new_rows[:MAX_ROWS]

    ### 6. Upload new data to Carto
    uploadData(new_rows)

    ###
    logging.info("SUCCESS")
