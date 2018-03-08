import os
import logging
import sys
import requests
from collections import OrderedDict
import datetime
import cartosql


# Constants
LATEST_URL = 'https://api.acleddata.com/acled/read?page={page}'
MIN_PAGES = 10
MAX_PAGES = 200
CLEAR_TABLE_FIRST = False

CARTO_TABLE = 'soc_016_conflict_protest_events'
CARTO_SCHEMA = OrderedDict([
    ("the_geom", "geometry"),
    ("data_id", "int"),
    ("event_date", "timestamp"),
    ("year", "int"),
    ("time_precision", "int"),
    ("event_type", "text"),
    ("actor1", "text"),
    ("assoc_actor_1", "text"),
    ("inter1", "int"),
    ("actor2", "text"),
    ("assoc_actor_2", "text"),
    ("inter2", "int"),
    ("interaction", "int"),
    ("country", "text"),
    ("iso3", "text"),
    ("region", "text"),
    ("admin1", "text"),
    ("admin2", "text"),
    ("admin3", "text"),
    ("location", "text"),
    ("geo_precision", "int"),
    ("time_precision", "int"),
    ("source", "text"),
    ("source_scale", "text"),
    ("notes", "text"),
    ("fatalities", "int"),
])
UID_FIELD = 'data_id'
TIME_FIELD = 'event_date'
DATA_DIR = 'data'
LOG_LEVEL = logging.INFO

# Limit 1M rows, drop older than 10yrs
MAXROWS = 1000000
#MAXAGE = datetime.datetime.today() - datetime.timedelta(days=3650)


def genUID(obs):
    '''Generate unique id'''
    return str(obs[UID_FIELD])


def processNewData(exclude_ids):
    '''
    Iterively fetch parse and post new data
    '''
    page = 1
    new_count = 1
    new_ids = []

    # get and parse each page; stop when no new results or 200 pages
    while page <= MIN_PAGES or new_count and page < MAX_PAGES:
        # 1. Fetch new data
        logging.info("Fetching page {}".format(page))
        r = requests.get(LATEST_URL.format(page=page))
        page += 1

        # 2. Parse data excluding existing observations
        new_rows = []
        for obs in r.json()['data']:
            uid = genUID(obs)
            if uid not in exclude_ids + new_ids:
                new_ids.append(uid)
                row = []
                for field in CARTO_SCHEMA.keys():
                    if field == 'the_geom':
                        # construct geojson geometry
                        geom = {
                            "type": "Point",
                            "coordinates": [
                                obs['longitude'],
                                obs['latitude']
                            ]
                        }
                        row.append(geom)
                    elif field == UID_FIELD:
                        row.append(uid)
                    else:
                        try:
                            row.append(obs[field])
                        except:
                            logging.debug('{} not available for this row'.format(field))
                            row.append('')

                new_rows.append(row)

        # 3. Insert new rows
        new_count = len(new_rows)
        if new_count:
            logging.info('Pushing {} new rows'.format(new_count))
            cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                                CARTO_SCHEMA.values(), new_rows)
    return new_ids


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
    if isinstance(max_age, datetime.datetime):
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
        if cartosql.tableExists(CARTO_TABLE):
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
    new_ids = processNewData(existing_ids)

    new_count = len(new_ids)
    existing_count = new_count + len(existing_ids)
    logging.info('Total rows: {}, New: {}, Max: {}'.format(
        existing_count, new_count, MAXROWS))

    # 3. Remove old observations
    deleteExcessRows(CARTO_TABLE, MAXROWS, TIME_FIELD) # MAXAGE)

    logging.info('SUCCESS')
