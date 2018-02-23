import os
import logging
import sys
import requests
from collections import OrderedDict
import datetime
import cartosql


# Constants
LATEST_URL = 'http://ucdpapi.pcr.uu.se/api/gedevents/17.2?pagesize=1000&page={page}'
CLEAR_TABLE_FIRST = False

CARTO_TABLE = 'soc_048_organized_violence_events'
CARTO_SCHEMA = OrderedDict([
    ("the_geom", "geometry"),
    ("date_start", "timestamp"),
    ("date_end", "timestamp"),
    ("uid", "text"),
    ("active_year", "text"),
    ("code_status", "text"),
    ("type_of_violence", "numeric"),
    ("conflict_dset_id", "text"),
    ("conflict_new_id", "numeric"),
    ("conflict_name", "text"),
    ("dyad_dset_id", "text"),
    ("dyad_new_id", "numeric"),
    ("dyad_name", "text"),
    ("size_a_dset_id", "text"),
    ("size_a_new_id", "numeric"),
    ("side_a", "text"),
    ("side_b_dset_id", "text"),
    ("side_b_new_id", "text"),
    ("side_b", "text"),
    ("number_of_sources", "numeric"),
    ("source_article", "text"),
    ("source_office", "text"),
    ("source_date", "text"),
    ("source_headline", "text"),
    ("source_original", "text"),
    ("where_prec", "numeric"),
    ("where_coordinates", "text"),
    ("where_description", "text"),
    ("adm_1", "text"),
    ("adm_2", "text"),
    ("priogrid_gid", "numeric"),
    ("country", "text"),
    ("country_id", "numeric"),
    ("region", "text"),
    ("event_clarity", "numeric"),
    ("date_prec", "numeric"),
    ("deaths_a", "numeric"),
    ("deaths_b", "numeric"),
    ("deaths_civilians", "numeric"),
    ("deaths_unknown", "numeric"),
    ("best", "numeric"),
    ("high", "numeric"),
    ("low", "numeric"),
    ("gwnoa", "text"),
    ("gwnob", "numeric")
])
UID_FIELD = 'uid'
TIME_FIELD = 'date_start'
DATA_DIR = 'data'
LOG_LEVEL = logging.INFO

# Limit 1M rows, drop older than 10yrs
MAXROWS = 1000000
MAXAGE = datetime.datetime.today() - datetime.timedelta(days=365*20)


def genUID(obs):
    '''Generate unique id'''
    return str(obs['id'])

def fetchResults(page):
    return requests.get(LATEST_URL.format(page=page)).json()['Result']

def genRow(obs, exclude_ids, new_ids):
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


def parseData(results, exclude_ids):
    return map(genRow, results)

def processNewData(exclude_ids):
    '''
    Iterively fetch parse and post new data
    '''
    ping = requests.get(LATEST_URL.format(page=0))
    num_pages = ping.json()['TotalPages']
    all_pages = list(range(num_pages))

    # Map - create a list of paginated responses
    all_results = map(fetchResults, all_pages)
    parse_results = map(parseData, all_results)
    # Reduce - collapse list of lists into a single list of rows

    # get and parse each page; stop when no new results or 200 pages
    while page <= MIN_PAGES or new_count and page < MAX_PAGES:
        # 1. Fetch new data
        logging.info("Fetching page {}".format(page))
        r = requests.get(LATEST_URL.format(page=page))
        page += 1

        # 2. Parse data excluding existing observations
        new_rows = []


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
    deleteExcessRows(CARTO_TABLE, MAXROWS, TIME_FIELD, MAXAGE)

    logging.info('SUCCESS')
