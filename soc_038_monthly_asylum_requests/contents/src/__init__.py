import os
import logging
import sys
import requests
from collections import OrderedDict, defaultdict
from datetime import datetime
import cartosql

# Constants
LATEST_URL = 'http://popdata.unhcr.org/api/stats/asylum_seekers_monthly.json?year={year}'

CARTO_TABLE = 'soc_038_monthly_asylum_requests'
CARTO_SCHEMA = OrderedDict([
    ("_UID", "text"),
    ("date", "timestamp"),
    ("country", "text"),
    ("value_type", "text"),
    ("num_people", "numeric")
])
UID_FIELD = '_UID'
TIME_FIELD = 'date'
DATA_DIR = 'data'
LOG_LEVEL = logging.INFO
DATE_FORMAT = '%Y-%m-%d'
CLEAR_TABLE_FIRST = False

# Limit 1M rows, drop older than 20yrs
MAXROWS = 1000000
MAXAGE = datetime.today().year - 20

def init_months():
    _init_months = defaultdict(int)
    [_init_months[i] for i in range(1,13)]
    return(_init_months)

def genUID(date, country, valuetype):
    '''Generate unique id'''
    return '{}_{}_{}'.format(country, date, valuetype)

def insertIfNewNonZero(data, year, valuetype, existing_ids, new_ids, new_rows, date_format=DATE_FORMAT):
    '''Loop over months in the data, add to new rows if new'''
    last_day = [31,28,31,30,31,30,31,31,30,31,30,31]
    for cntry in data:
        for month, val in data[cntry].items():
            if val != 0:
                date = datetime(year=year, month=month, day=last_day[month-1]).strftime(date_format)
                UID = genUID(date, cntry, valuetype)
                if UID not in existing_ids + new_ids:
                    new_ids.append(UID)
                    values = [UID, date, cntry, valuetype, val]
                    new_rows.append(values)

def processNewData(existing_ids):
    '''
    Iterively fetch parse and post new data
    '''
    year = datetime.today().year
    new_count = 1
    new_ids = []

    while year > MAXAGE and new_count:
        # get and parse each page; stop when no new results or 200 pages
        # 1. Fetch new data
        logging.info("Fetching data for year {}".format(year))
        r = requests.get(LATEST_URL.format(year=year))
        data = r.json()
        logging.debug('data: {}'.format(data))

        # 2. Collect Totals
        origins = defaultdict(init_months)
        asylums = defaultdict(init_months)
        off_limits_origins = defaultdict(list)
        off_limits_asylums = defaultdict(list)

        for obs in data:
            try:
                origins[obs['country_of_origin']][obs['month']] += obs['value']
            except Exception as e:
                logging.error("Error processing value {} for country of origin {} in {}-{}. Value set to -9999. Error: {}".format(obs['value'],obs['country_of_origin'],year,obs['month'],e))
                off_limits_origins[obs['country_of_origin']].append(obs['month'])
            try:
                asylums[obs['country_of_asylum']][obs['month']] += obs['value']
            except Exception as e:
                logging.error("Error processing value {} for country of asylum {} in {}-{}. Value set to -9999. Error: {}".format(obs['value'],obs['country_of_asylum'],year,obs['month'],e))
                off_limits_asylums[obs['country_of_asylum']].append(obs['month'])

        for cntry, months in off_limits_origins.items():
            for month in months:
                origins[cntry][month] = -9999
        for cntry, months in off_limits_asylums.items():
            for month in months:
                asylums[cntry][month] = -9999

        # 3. Create Unique IDs, parse rows
        new_rows = []
        insertIfNewNonZero(origins,year,'country_of_origin',existing_ids,new_ids,new_rows)
        insertIfNewNonZero(asylums,year,'country_of_asylum',existing_ids,new_ids,new_rows)

        # 4. If new, insert
        new_count = len(new_rows)
        if new_count:
            logging.info('Pushing {} new rows'.format(new_count))
            cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                                CARTO_SCHEMA.values(), new_rows)
        # Decrement year
        year -= 1

    num_new = len(new_ids)
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


def getIds(table, id_field):
    '''get ids from table'''
    r = cartosql.getFields(id_field, table, f='csv')
    return r.text.split('\r\n')[1:-1]


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
    r = cartosql.getFields('cartodb_id', table, order='{}'.format(time_field),
                           f='csv')
    ids = r.text.split('\r\n')[1:-1]

    # 3. delete excess
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[:-max_rows])
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))


def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    if CLEAR_TABLE_FIRST:
        logging.info('Clearing table')
        cartosql.dropTable(CARTO_TABLE)

    # 1. Check if table exists and create table
    existing_ids = []
    if cartosql.tableExists(CARTO_TABLE):
        logging.info('Fetching existing ids')
        existing_ids = getIds(CARTO_TABLE, UID_FIELD)
    else:
        logging.info('Table {} does not exist, creating'.format(CARTO_TABLE))
        createTableWithIndex(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)

    # 2. Iterively fetch, parse and post new data
    num_new = processNewData(existing_ids)

    existing_count = num_new + len(existing_ids)
    logging.info('Total rows: {}, New: {}, Max: {}'.format(
        existing_count, num_new, MAXROWS))

    # 3. Remove old observations
    deleteExcessRows(CARTO_TABLE, MAXROWS, TIME_FIELD, datetime(year=MAXAGE, month=1, day=1))

    logging.info('SUCCESS')
