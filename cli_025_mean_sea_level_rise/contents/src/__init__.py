import logging
import sys
import os
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

import pandas as pd

# If not running in nrt container, environ variables will not be set
import src.utilities.carto as carto
import src.utilities.misc as misc
import src.utilities.cli_025 as cli_025

from collections import OrderedDict

CARTO_TABLE = 'cli_025_mean_sea_level_rise'
CARTO_SCHEMA = OrderedDict([
    ('altimeter_type', 'numeric'),
    ('merged_file_cycle', 'numeric'),
    ('date', 'timestamp'),
    ('num_obs', 'numeric'),
    ('num_weighted_obs', 'numeric'),
    ('gmsl_no_gia', 'numeric'),
    ('sd_gmsl_no_gia', 'numeric'),
    ('gmsl_gia', 'numeric'),
    ('sd_gmsl_gia', 'numeric'),
    ('gauss_filt_gmsl_gia', 'numeric'),
    ('gauss_filt_gmsl_gia_ann_signal_removed', 'numeric')
])

UID_FIELD = 'year_and_fraction'
TIME_FIELD = 'year_and_fraction'

def fetchData():
    # Read the files that are on the FTP
    df = cli_025.fetchData()
    logging.info(df)
    # Date data is in column 2
    df[2] = df[2].apply(lambda date: cli_025.dec_to_datetime(float(date)))
    logging.info(df[2])
    return(df)

def uploadData(df):
    rows = df.values.tolist()
    logging.info(rows[:10])
    if len(rows):
        carto.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA, rows)

def main():
    ### 1. Check if table exists and create table
    if not carto.tableExists(CARTO_TABLE):
        logging.info('Table {} does not exist'.format(CARTO_TABLE))
        carto.createTable(CARTO_TABLE, CARTO_SCHEMA)
    else:
        carto.dropTable(CARTO_TABLE)
        carto.createTable(CARTO_TABLE, CARTO_SCHEMA)

    ### 2. Fetch data from FTP
    df = fetchData()

    ### 3. Upload data to Carto
    uploadData(df)

    ### 4. Write data to s3
    misc.write_to_S3(df, "resourcewatch/cli_025_mean_sea_level_rise.csv")
