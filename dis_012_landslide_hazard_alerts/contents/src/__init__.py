import os
import logging
import sys
import requests as req
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta
import cartosql
from operator import methodcaller

LOG_LEVEL = logging.INFO
CLEAR_TABLE_FIRST = False

# Constants
_3HR_LATEST_URL = 'https://pmmpublisher.pps.eosdis.nasa.gov/opensearch?q=global_landslide_nowcast_3hr&limit=100000000&startTime={startTime}&endTime={endTime}'
DAILY_LATEST_URL = 'https://pmmpublisher.pps.eosdis.nasa.gov/opensearch?q=global_landslide_nowcast&limit=100000000&startTime={startTime}&endTime={endTime}'

CARTO_TABLE_EXPLORE = 'dis_012_landslide_hazard_alerts_explore'
CARTO_TABLE_PLANETPULSE = 'dis_012_landslide_hazard_alerts_planetpulse'
CARTO_SCHEMA = OrderedDict([
    ('_UID', 'text'),
    ('datetime', 'timestamp'),
    ('nowcast', 'numeric'),
    ('the_geom', 'geometry')
])
UID_FIELD = '_UID'
TIME_FIELD = 'datetime'

QUERY_DATE_FORMAT = '%Y-%m-%d'
INPUT_DATE_FORMAT = '%Y%m%d%H%M'
OUTPUT_DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

# Limit 1M rows, drop older than 1 yr
MAXROWS_EXPLORE = 1000000
MAXROWS_PLANETPULSE = 100000
MAXAGE_EXPLORE = datetime.today() - timedelta(days=365)
MAXAGE_PLANETPULSE = datetime.now() - timedelta(days=1)

def genUID(datetime, position_in_geojson):
    '''Generate unique id'''
    return '{}_{}'.format(datetime, position_in_geojson)

def insertIfNew(response, existing_ids, explore=True,
                input_date=INPUT_DATE_FORMAT,output_date=OUTPUT_DATE_FORMAT):
    '''Loop over months in the data, add to new rows if new'''

    m = map(methodcaller('split', '_'),(existing_ids))
    seen_dates = list(zip(*m))[0]
    today = datetime.today().replace(hour=23, minute=30, second=0).strftime(OUTPUT_DATE_FORMAT)

    new_rows = []
    for item in response['items']:
        if explore:
            dt_info = item['displayName'].split('_')[-1]
            dt = datetime.strptime(dt_info+"2330", INPUT_DATE_FORMAT)
            dt = dt.strftime(OUTPUT_DATE_FORMAT)
        else:
            dt_info = item['displayName'].split('_')[-2:]
            dt = datetime.strptime(dt_info[0]+dt_info[1][:4], INPUT_DATE_FORMAT)
            dt = dt.strftime(OUTPUT_DATE_FORMAT)

        if dt == today:
            # Delete existing data for this date
            delete_ids = [existing_ids[ix] for ix, date in enumerate(seen_dates) if date == today ]
            logging.info('deleting ids related to this day: ' + str(delete_ids))
            cartosql.deleteRowsByIDs(CARTO_TABLE_EXPLORE, delete_ids, id_field=UID_FIELD, dtype='text')

        if (dt not in seen_dates) or (dt == today) :
            logging.info('adding data for datetime ' + dt)
            for act in item['action']:
                if act['displayName'] == 'export':
                    for use in act['using']:
                        if use['displayName'] == 'geojson':
                            geojson = req.get(use['url']).json()
                            for ix, feature in enumerate(geojson['features']):
                                UID = genUID(dt, ix)
                                the_geom = feature['geometry']
                                nowcast = feature['properties']['nowcast']
                                values = []
                                for field in CARTO_SCHEMA:
                                    if field == UID_FIELD:
                                        values.append(UID)
                                    if field == TIME_FIELD:
                                        values.append(dt)
                                    if field == 'nowcast':
                                        values.append(nowcast)
                                    if field == 'the_geom':
                                        values.append(the_geom)
                                new_rows.append(values)
        logging.debug('running count of num new rows: '  + str(len(new_rows)))

    return new_rows

def processNewData(explore_ids, planetpulse_ids):
    '''
    Iterively fetch parse and post new data
    '''
    now = datetime.now().strftime(QUERY_DATE_FORMAT)
    yesterday = (datetime.today() - timedelta(days=1)).strftime(QUERY_DATE_FORMAT)
    twoMonthsAgo = (datetime.today() - timedelta(days=62)).strftime(QUERY_DATE_FORMAT)
    ### DAILY DATA - EXPLORE
    # 1. Fetch new daily data
    logging.info("Fetching all available daily data from " + twoMonthsAgo)
    r = req.get(DAILY_LATEST_URL.format(startTime=twoMonthsAgo,endTime=now))
    data = r.json()

    # 2. Create new rows
    new_rows = insertIfNew(data, explore_ids)

    # 3. Insert new rows
    new_count = len(new_rows)
    if new_count:
        logging.info('Pushing {} new rows'.format(new_count))
        cartosql.insertRows(CARTO_TABLE_EXPLORE, CARTO_SCHEMA.keys(),
                            CARTO_SCHEMA.values(), new_rows)

    num_new_explore = len(new_rows)

    ### NRT DATA - PLANET PULSE
    # 1. Fetch new nrt 3hr data
    logging.info("Fetching most recent 24 hours of 3hr data")
    r = req.get(_3HR_LATEST_URL.format(startTime=yesterday, endTime=now))
    data = r.json()

    # 2. Create new rows for most recent observation
    new_rows = insertIfNew(data, planetpulse_ids, explore=False)

    # 3. Insert new rows
    new_count = len(new_rows)
    if new_count:
        logging.info('Pushing {} new rows'.format(new_count))
        cartosql.insertRows(CARTO_TABLE_PLANETPULSE, CARTO_SCHEMA.keys(),
                            CARTO_SCHEMA.values(), new_rows)

    num_new_planetpulse = len(new_rows)

    return num_new_explore, num_new_planetpulse


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
        logging.info('Clearing explore table')
        cartosql.dropTable(CARTO_TABLE_EXPLORE)

    # 1. Check if table exists and create table

    ## EXPLORE TABLE
    existing_explore_ids = []
    if cartosql.tableExists(CARTO_TABLE_EXPLORE):
        logging.info('Fetching existing explore ids')
        existing_explore_ids = getIds(CARTO_TABLE_EXPLORE, UID_FIELD)
    else:
        logging.info('Table {} does not exist, creating'.format(CARTO_TABLE_EXPLORE))
        createTableWithIndex(CARTO_TABLE_EXPLORE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)

    ## PLANET PULSE TABLE
    existing_pp_ids = []
    if cartosql.tableExists(CARTO_TABLE_PLANETPULSE):
        logging.info('Fetching existing planet pulse ids')
        existing_pp_ids = getIds(CARTO_TABLE_PLANETPULSE, UID_FIELD)
    else:
        logging.info('Table {} does not exist, creating'.format(CARTO_TABLE_PLANETPULSE))
        createTableWithIndex(CARTO_TABLE_PLANETPULSE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)

    # 2. Iterively fetch, parse and post new data
    num_new_explore, num_new_pp = processNewData(existing_explore_ids, existing_pp_ids)
    existing_count_explore = num_new_explore + len(existing_explore_ids)
    existing_count_pp = num_new_pp + len(existing_pp_ids)

    logging.info('Total rows in daily table: {}, New: {}, Max: {}'.format(
        existing_count_explore, num_new_explore, MAXROWS_EXPLORE))

    logging.info('Total rows in daily table: {}, New: {}, Max: {}'.format(
        existing_count_pp, num_new_pp, MAXROWS_PLANETPULSE))

    # 3. Remove old observations
    deleteExcessRows(CARTO_TABLE_EXPLORE, MAXROWS_EXPLORE, TIME_FIELD, MAXAGE_EXPLORE)
    deleteExcessRows(CARTO_TABLE_PLANETPULSE, MAXROWS_PLANETPULSE, TIME_FIELD, MAXAGE_PLANETPULSE)

    logging.info('SUCCESS')
