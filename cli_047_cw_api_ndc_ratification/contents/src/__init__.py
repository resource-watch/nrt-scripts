import os
import logging
import sys
import requests
from collections import OrderedDict
from datetime import datetime, timedelta
from functools import reduce
import cartoframes
import cartosql
import pandas as pd

# Constants
SOURCE_URL = 'https://www.climatewatchdata.org/api/v1/ndcs/'
CLEAR_TABLE_FIRST = False
DATE_FORMAT = '%Y-%m-%d'
COUNTRY_CODE = 'country_code'

CARTO_KEY_WRIRW = os.environ.get('CARTO_KEY_WRIRW')
CARTO_KEY_RWNRT = os.environ.get('CARTO_KEY')

cc_wrirw = cartoframes.CartoContext(base_url='https://wri-rw.carto.com/',
                              api_key=CARTO_KEY_WRIRW)
cc_rwnrt = cartoframes.CartoContext(base_url='https://rw-nrt.carto.com/',
                              api_key=CARTO_KEY_RWNRT)

# Need to drop the alias column b/c otherwise get multiple matches
ISO_ALIAS_INFO = cc_wrirw.read('country_aliases_extended')
ISO_ALIAS_INFO = ISO_ALIAS_INFO.drop(['alias', 'index', 'the_geom'], axis=1).drop_duplicates()
logging.info('Alias table shape: {}'.format(ISO_ALIAS_INFO))

CARTO_TABLE = 'cli_047_ndc_ratification'
CARTO_SCHEMA = OrderedDict([
    ("country_code", "text"),
    ("ratification_status", "text"),
    ("last_update", "timestamp"),
    ("rw_country_code", "text"),
    ("rw_country_name", "text"),
])
UID_FIELD = 'country_code'
TIME_FIELD = 'last_update'
DATA_DIR = 'data'
LOG_LEVEL = logging.INFO

def genRow(obs):
    last_update = datetime.today().strftime(DATE_FORMAT)
    row = []
    for field in CARTO_SCHEMA.keys():
        if field == 'country_code':
            row.append(obs['country_code'])
        elif field == 'ratification_status':
            row.append(obs['ratification_status'])
        elif field == 'last_update':
            row.append(last_update)
        else:
            # Placeholder for rw_country_code and rw_country_name
            row.append(' ')
    return row

def keep_rat_stat(item):
    if item['name'] == 'Status of ratification':
        return True
    else:
        return False

def make_obs(agg, elem):
    obs = {
        'country_code':elem[0],
        'ratification_status':elem[1][0]['value']
    }
    agg.append(obs)
    return agg

def georef_by_ccode(df, ccode):
    # Weird behavior of globals in a local scope here:
    # https://stackoverflow.com/questions/10851906/python-3-unboundlocalerror-local-variable-referenced-before-assignment
    df.index = list(range(df.shape[0]))
    data_with_alias = df.merge(ISO_ALIAS_INFO,
                       left_on=ccode,
                       right_on='iso',
                       how='left')
    try:
        null_isos = pd.isnull(data_with_alias['iso'])
    except:
        null_isos = pd.isnull(data_with_alias['iso_y'])

    if sum(null_isos):
        no_iso_match = data_with_alias[null_isos]
        logging.info('no match for these isos in the data being processed: ')
        try:
            missed_isos = no_iso_match[ccode].unique()
            logging.info(missed_isos)
        except:
            ccode = ccode +'_x'
            missed_isos = no_iso_match[ccode].unique()
            logging.info(missed_isos)

    logging.info('df shape: {}'.format(df.shape))
    logging.info('data_with_alias shape: {}'.format(data_with_alias.shape))

    try:
        df['rw_country_code'] = data_with_alias['iso'].values
    except:
        df['rw_country_code'] = data_with_alias['iso_y'].values
    try:
        df['rw_country_name'] = data_with_alias['name']
    except:
        df['rw_country_name'] = data_with_alias['name_y']

    # Enforce correct ordering of columns here
    return df[list(CARTO_SCHEMA.keys())]

def processNewData():
    '''
    Iterively fetch parse and post new data
    '''
    data = requests.get(SOURCE_URL).json()['indicators']
    rat_stat = list(filter(keep_rat_stat, data))
    country_rat_stat = rat_stat[0]['locations']
    country_rat_stat_obs = reduce(make_obs, country_rat_stat.items(), [])
    parsed_rows = pd.DataFrame(list(map(genRow, country_rat_stat_obs)))
    logging.info(parsed_rows)
    parsed_rows.columns = CARTO_SCHEMA.keys()
    georeffed_rows = georef_by_ccode(parsed_rows, COUNTRY_CODE)
    num_new = len(georeffed_rows)
    if num_new:
        logging.info('Pushing {} new rows'.format(num_new))
        #cc_rwnrt.write(georeffed_rows, CARTO_TABLE, overwrite=True)
        cartosql.truncateTable(CARTO_TABLE)
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                                CARTO_SCHEMA.values(), list(georeffed_rows.values))

    return num_new


##############################################################
# General logic for Carto
# should be the same for most tabular datasets
##############################################################

def createTableWithIndex(table, schema, id_field, time_field=''):
    '''Get existing ids or create table'''
    cartosql.createTable(table, schema)
    cartosql.createIndex(table, id_field, unique=True)
    if time_field:
        cartosql.createIndex(table, time_field)

def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    # 1. Check if table exists and create table
    if cartosql.tableExists(CARTO_TABLE):
        logging.info('Table {} already exists'.format(CARTO_TABLE))
    else:
        logging.info('Table {} does not exist, creating'.format(CARTO_TABLE))
        createTableWithIndex(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)

    # 2. Fetch and post new data
    num_new = processNewData()
    logging.info('Total rows: {}'.format(num_new))

    logging.info('SUCCESS')
