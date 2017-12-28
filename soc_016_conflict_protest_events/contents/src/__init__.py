import os
import logging
import sys
import requests
from collections import OrderedDict
from datetime import datetime, timedelta
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
MAXAGE = datetime.today() - timedelta(days=365*10)


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
            logging.info('Pushing {} new rows'.format(new_count))
            cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                                CARTO_SCHEMA.values(), new_rows)
    return(len(new_ids))


##############################################################
# General logic for Carto
# should be the same for most tabular datasets
##############################################################

def checkCreateTable(table, schema, id_field, time_field):
    '''
    Create table if it doesn't already exist
    '''
    if cartosql.tableExists(table):
        logging.info('Table {} already exists'.format(table))
    else:
        logging.info('Creating Table {}'.format(table))
        cartosql.createTable(table, schema)
        cartosql.createIndex(table, id_field, unique=True)
        if id_field != time_field:
            cartosql.createIndex(table, time_field)

def cleanOldRows(table, time_field, maxage, date_format='%Y-%m-%d %H:%M:%S'):
    '''
    Delete excess rows by age
    maxage should be a datetime object or string
    Return number of dropped rows
    '''
    num_expired = 0
    if cartosql.tableExists(table):
        if isinstance(maxage, datetime):
            maxage = maxage.strftime(date_format)
        else:
            logging.error('Max age must be expressed as a datetime.datetime object')

        r = cartosql.deleteRows(table, "{} < '{}'".format(time_field, maxage))
        num_expired = r.json()['total_rows']
    else:
        logging.error("{} table does not exist yet".format(table))

    return(num_expired)

def deleteExcessRows(table, maxrows, time_field):
    '''Delete rows to bring count down to maxrows'''
    num_dropped=0
    # 1. get sorted ids (old->new)
    r = cartosql.getFields('cartodb_id', table, order='{} desc'.format(time_field),
                           f='csv')
    ids = r.text.split('\r\n')[1:-1]

    # 2. delete excess
    if len(ids) > maxrows:
        r = cartosql.deleteRowsByIDs(table, ids[maxrows:])
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))

    return(num_dropped)


def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    ### 1. Check if table exists, if not, create it
    checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)

    ### 2. Delete old rows
    num_expired = cleanOldRows(CARTO_TABLE, TIME_FIELD, MAXAGE)

    ### 3. Retrieve existing data
    r = cartosql.getFields(UID_FIELD, CARTO_TABLE, f='csv')
    existing_ids = r.text.split('\r\n')[1:-1]
    num_existing = len(existing_ids)

    logging.debug("First 10 IDs already in table: {}".format(existing_ids[:10]))

    ### 4. Iterively fetch, parse and post new data
    num_new = processNewData(existing_ids)

    ### 5. Remove excess rows until MAXROWS reached
    num_deleted = deleteExcessRows(CARTO_TABLE, MAXROWS, TIME_FIELD)

    ### 6. Notify results
    logging.info('Expired rows: {}, Previous rows: {},  New rows: {}, Dropped rows: {}, Max: {}'.format(num_expired, num_existing, num_new, num_deleted, MAXROWS))
    logging.info("SUCCESS")
