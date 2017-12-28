import os
import logging
import sys
import requests
from collections import OrderedDict
import cartosql
import datetime
import hashlib

logging.basicConfig(stream=sys.stderr, level=logging.INFO)

### Constants
DATA_DIR = 'data'
# max page size = 10000
DATA_URL = 'https://api.openaq.org/v1/measurements?limit=10000&include_fields=attribution&page={page}'
# always check first 10 pages
MIN_PAGES = 10
MAX_PAGES = 200

# asserting table structure rather than reading from input
PARAMS = ('pm25', 'pm10', 'so2', 'no2', 'o3', 'co', 'bc')
CARTO_TABLES = {
    'pm25':'cit_003a_air_quality_pm25',
    'pm10':'cit_003b_air_quality_pm10',
    'so2':'cit_003c_air_quality_so2',
    'no2':'cit_003d_air_quality_no2',
    'o3':'cit_003e_air_quality_o3',
    'co':'cit_003f_air_quality_co',
    'bc':'cit_003g_air_quality_bc',
}
CARTO_SCHEMA = OrderedDict([
    ("the_geom", "geometry"),
    ("_UID", "text"),
    ("utc", "timestamp"),
    ("value", "numeric"),
    ("parameter", "text"),
    ("location", "text"),
    ("city", "text"),
    ("country", "text"),
    ("unit", "text"),
    ("attribution", "text")
])
UID_FIELD = '_UID'
TIME_FIELD = 'utc'

CARTO_USER = os.environ.get('CARTO_USER')
CARTO_KEY = os.environ.get('CARTO_KEY')

# Limit to 5M rows / 30 days
MAXROWS = 5000000
MAXAGE = datetime.datetime.now() - datetime.timedelta(days=30)


# Generate UID
def genUID(obs):
    # location should be unique, plus measurement timestamp
    id_str = '{}_{}'.format(obs['location'], obs['date']['utc'])
    return hashlib.md5(id_str.encode('utf8')).hexdigest()


def checkCreateTable(table, schema, id_field, time_field):
    '''Get existing ids or create table'''
    if cartosql.tableExists(table):
        logging.info('Fetching existing IDs')
        r = cartosql.getFields(id_field, table, f='csv')
        return r.text.split('\r\n')[1:-1]
    else:
        logging.info('Table {} does not exist, creating'.format(table))
        cartosql.createTable(table, schema)
        cartosql.createIndex(table, id_field, unique=True)
        cartosql.createIndex(table, time_field)
    return []


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
    logging.info('BEGIN')

    # 1. Get existing uids, if none create tables
    existing_ids = {}
    for param in PARAMS:
        existing_ids[param] = checkCreateTable(CARTO_TABLES[param],
                                               CARTO_SCHEMA, UID_FIELD,
                                               TIME_FIELD)

    # 2. Iterively fetch, parse and post new data
    # this is done all together because OpenAQ endpoint filter by parameter
    # doesn't work
    new_counts = dict(((param, 0) for param in PARAMS))
    new_count = 1
    page = 1
    # get and parse each page
    # read at least 10 pages; stop when no new results or 100 pages
    while page <= MIN_PAGES or new_count and page < MAX_PAGES:
        logging.info("Fetching page {}".format(page))
        r = requests.get(DATA_URL.format(page=page))
        page += 1
        new_count = 0

        # separate row lists per param
        rows = dict(((param, []) for param in PARAMS))

        # parse data excluding existing observations
        for obs in r.json()['results']:
            param = obs['parameter']
            uid = genUID(obs)
            if uid not in existing_ids[param] and 'coordinates' in obs:
                # OpenAQ may contain duplicate obs
                existing_ids[param].append(uid)
                row = []
                for field in CARTO_SCHEMA.keys():
                    if field == 'the_geom':
                        # construct geojson
                        geom = {
                            "type": "Point",
                            "coordinates": [
                                obs['coordinates']['longitude'],
                                obs['coordinates']['latitude']
                            ]
                        }
                        row.append(geom)
                    elif field == UID_FIELD:
                        row.append(uid)
                    elif field == TIME_FIELD:
                        row.append(obs['date'][TIME_FIELD])
                    elif field == 'attribution':
                        row.append(str(obs['attribution']))
                    else:
                        row.append(obs[field])
                rows[param].append(row)

        # insert new rows
        for param in PARAMS:
            count = len(rows[param])
            if count:
                logging.info('Pushing {} new {} rows'.format(count, param))
                cartosql.insertRows(CARTO_TABLES[param], CARTO_SCHEMA.keys(),
                                    CARTO_SCHEMA.values(), rows[param])
                new_count += count
            new_counts[param] += count

    # 3. Remove old observations
    for param in PARAMS:
        logging.info('Total rows: {}, New: {}, Max: {}'.format(
            len(existing_ids[param]), new_counts[param], MAXROWS))
        deleteExcessRows(CARTO_TABLES[param], MAXROWS, TIME_FIELD, MAXAGE)

    logging.info('SUCCESS')
