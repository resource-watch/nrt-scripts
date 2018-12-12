import os
import logging
import sys
import requests
from collections import OrderedDict
import datetime
import cartosql

LOG_LEVEL = logging.INFO

# Constants
URL_3HR = 'https://pmmpublisher.pps.eosdis.nasa.gov/opensearch?q=global_landslide_nowcast_3hr&limit=100000000&startTime={startTime}&endTime={endTime}'
URL_DAILY = 'https://pmmpublisher.pps.eosdis.nasa.gov/opensearch?q=global_landslide_nowcast&limit=100000000&startTime={startTime}&endTime={endTime}'


TABLE_DAILY = 'dis_012a_landslide_hazard_alerts_daily'
TABLE_3HR = 'dis_012b_landslide_hazard_alerts_3hr'

# legacy dataset table // use either TABLE_DAILY or TABLE_3HR in future
TABLE_LEGACY = 'dis_012_landslide_hazard_alerts_explore'

TABLES = {
    TABLE_DAILY: URL_DAILY,
    TABLE_3HR: URL_3HR,
    TABLE_LEGACY: URL_3HR
}

CARTO_SCHEMA = OrderedDict([
    ('_UID', 'text'),
    ('datetime', 'timestamp'),
    ('nowcast', 'numeric'),
    ('the_geom', 'geometry')
])
UID_FIELD = '_UID'
TIME_FIELD = 'datetime'

# Limit 100k rows, drop older than 1 yr
MAX_ROWS = 100000
MAX_AGE = datetime.datetime.utcnow() - datetime.timedelta(days=365)
DATASET_ID = '444138cd-8ef4-48b3-b197-73e324175ad0'
def lastUpdateDate(dataset, date):
   apiUrl = 'http://api.resourcewatch.org/v1/dataset/{0}'.format(dataset)
   headers = {
   'Content-Type': 'application/json',
   'Authorization': os.getenv('apiToken')
   }
   body = {
       "dataLastUpdated": date.isoformat()
   }
   try:
       r = requests.patch(url = apiUrl, json = body, headers = headers)
       logging.info('[lastUpdated]: SUCCESS, '+ date.isoformat() +' status code '+str(r.status_code))
       return 0
   except Exception as e:
       logging.error('[lastUpdated]: '+str(e))

def genUID(datetime, position_in_geojson):
    '''Generate unique id'''
    return '{}_{}'.format(datetime, position_in_geojson)

def processData(exclude_ids, table, src_url):
    new_ids = []
    num_new = 1

    start_time = MAX_AGE.isoformat()
    end_time = datetime.datetime.utcnow().isoformat()

    # OpenSearch endpoint returns json object of URLs with data
    r = requests.get(src_url.format(startTime=start_time, endTime=end_time))
    results = r.json()

    # loop until no new observations
    for item in results['items']:
        new_rows = []

        # better to break if JSON response for format changes
        # than to try to search for the right values
        date = item['properties']['date']['@value']
        url = item['action'][5]['using'][0]['url']

        logging.info('Fetching data for {}'.format(date))
        data = requests.get(url).json()
        # loop through geojson features
        for i in range(len(data['features'])):
            uid = genUID(date, i)
            if uid not in exclude_ids and uid not in new_ids:
                new_ids.append(uid)
                obs = data['features'][i]
                row = []
                for field in CARTO_SCHEMA:
                    if field == UID_FIELD:
                        row.append(uid)
                    if field == TIME_FIELD:
                        row.append(date)
                    if field == 'nowcast':
                        row.append(obs['properties']['nowcast'])
                    if field == 'the_geom':
                        row.append(obs['geometry'])
                new_rows.append(row)

        num_new = len(new_rows)
        if num_new and len(new_ids) < MAX_ROWS:
            logging.info("Inserting {} new rows".format(num_new))
            cartosql.insertRows(table, CARTO_SCHEMA.keys(),
                                CARTO_SCHEMA.values(), new_rows)
        else:
            break

    return new_ids

##############################################################
# General logic for Carto
# should be the same for most tabular datasets
##############################################################

def getFieldAsList(field, table, **args):
    r = cartosql.getFields(field, table, f='csv', **args)
    return r.text.splitlines()[1:]


def checkCreateTable(table, schema, id_field, time_field):
    '''Get existing ids or create table'''
    if cartosql.tableExists(table):
        logging.info('Fetching existing IDs')
        return getFieldAsList(id_field, table)
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
    ids = getFieldAsList('cartodb_id', table, order='{}'.format(time_field))

    # 3. delete excess
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[:-max_rows])
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))


def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    for table, url in TABLES.items():
        logging.info('Processing data for {}'.format(table))
        # 1. Check if table exists and create table
        existing_ids = checkCreateTable(table, CARTO_SCHEMA, UID_FIELD,
                                        TIME_FIELD)

        # 2. Iterively fetch, parse and post new data
        new_ids = processData(existing_ids, table, url)

        new_count = len(new_ids)
        existing_count = new_count + len(existing_ids)
        logging.info('Total rows: {}, New: {}, Max: {}'.format(
            existing_count, new_count, MAX_ROWS))

        # 3. Remove old observations
        deleteExcessRows(table, MAX_ROWS, TIME_FIELD, MAX_AGE)

    logging.info('SUCCESS')
