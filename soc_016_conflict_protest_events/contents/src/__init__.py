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

CARTO_TABLE = 'soc_016_conflict_protest_events_afr'
CARTO_SCHEMA = OrderedDict([
    ("the_geom", "geometry"),
    ("data_id", "int"),
    ("event_date", "timestamp"),
    ("year", "int"),
    ("time_precision", "int"),
    ("event_type", "text"),
    ("actor1", "text"),
    ("ally_actor_1", "text"),
    ("inter1", "int"),
    ("actor2", "text"),
    ("ally_actor_2", "text"),
    ("inter2", "int"),
    ("interaction", "int"),
    ("country", "text"),
    ("admin1", "text"),
    ("admin2", "text"),
    ("admin3", "text"),
    ("location", "text"),
    ("geo_precision", "int"),
    ("source", "text"),
    ("notes", "text"),
    ("fatalities", "int"),
])
UID_FIELD = 'data_id'
TIME_FIELD = 'event_date'
DATA_DIR = 'data'
LOG_LEVEL = logging.INFO

# Limit 1M rows, drop older than 10yrs
MAXROWS = 1000000
MAXAGE = datetime.datetime.today() - datetime.timedelta(days=3650)


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
            if uid not in exclude_ids:
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
                        row.append(obs[field])
                new_rows.append(row)

        # 3. Insert new rows
        new_count = len(new_rows)
        if new_count:
            logging.info('Pushing new rows')
            cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                                CARTO_SCHEMA.values(), new_rows)
    return new_ids


##############################################################
# General logic for Carto
# should be the same for most tabular datasets
##############################################################

def checkCreateTable(table, schema, id_field, time_field):
    '''Get existing ids or create table'''
    if cartosql.tableExists(table):
        logging.info('Fetching existing IDs')
        r = cartosql.getFields(id_field, table, f='csv')
        return r.text.split('\r\n')[1:-1]
    else:
        logging.info('Table {} does not exist, creating'.format(table))
        cartosql.createTable(table, schema)
        cartosql.createIndex(table, time_field)
    return []


def deleteExcessRows(table, max_rows, time_field, max_age='',
                     id_field='cartodb_id'):
    '''Delete excess rows by age or count'''
    num_dropped = 0
    if isinstance(max_age, datetime.datetime):
        max_age = max_age.isoformat()

    # 1. delete by age
    if max_age:
        r = cartosql.deleteRows(table, "{} < '{}'".format(time_field, max_age))
        num_dropped = r.json()['total_rows']

    # 2. get sorted ids (old->new)
    ids = cartosql.getFields(id_field, table, order='{}'.format(time_field),
                             f='csv').text.split('\r\n')[1:-1]

    # 3. delete excess
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIds(table, ids[:-max_rows])
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))


def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    # 1. Check if table exists and create table
    existing_ids = checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD,
                                    TIME_FIELD)

    # 2. Iterively fetch, parse and post new data
    new_ids = processNewData(existing_ids)

    new_count = len(new_ids)
    existing_count = new_count + len(existing_ids)
    logging.info('Total rows: {}, New: {}, Max: {}'.format(
        existing_count, new_count, MAXROWS))

    # 3. Remove old observations
    deleteExcessRows(CARTO_TABLE, MAXROWS, TIME_FIELD, MAXAGE)

    logging.info('SUCCESS')
