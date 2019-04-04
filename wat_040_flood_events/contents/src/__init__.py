from __future__ import unicode_literals

import fiona
import os
import logging
import sys
import urllib
import datetime
from collections import OrderedDict
import cartosql
import requests
import zipfile

# Constants
DATA_DIR = 'data'
SOURCE_URLS = {
    'f.dat':'http://floodobservatory.colorado.edu/Version3/FloodArchive.DAT',
    'f.id':'http://floodobservatory.colorado.edu/Version3/FloodArchive.ID',
    'f.map':'http://floodobservatory.colorado.edu/Version3/FloodArchive.MAP',
    'f.ind':'http://floodobservatory.colorado.edu/Version3/FloodArchive.IND',
    'f.tab':'http://floodobservatory.colorado.edu/Version3/FloodArchive.TAB',
    'f_shp.zip': 'http://floodobservatory.colorado.edu/Version3/FloodsArchived_shp.zip'
}
TABFILE = 'f.tab'
ENCODING = 'latin-1'
SHPFILE = 'FloodsArchived_shape.shp'
# asserting table structure rather than reading from input
CARTO_TABLE = 'wat_040_flood_events'
CARTO_TABLE_SHP = 'wat_040_flood_events_shp'

CARTO_SCHEMA = OrderedDict([
    ('the_geom', 'geometry'),
    ('_UID', 'text'),
    ('ID', 'int'),
    ('GlideNumber', 'text'),
    ('Country', 'text'),
    ('OtherCountry', 'text'),
    ('long', 'numeric'),
    ('lat', 'numeric'),
    ('Area', 'numeric'),
    ('Began', 'timestamp'),
    ('Ended', 'timestamp'),
    ('Validation', 'text'),
    ('Dead', 'int'),
    ('Displaced', 'int'),
    ('MainCause', 'text'),
    ('Severity', 'numeric')
])
UID_FIELD = '_UID'
TIME_FIELD = 'Began'

MAXROWS = 1000000
LOG_LEVEL = logging.INFO
MAXAGE = None
DATASET_ID = '1616a329-1bf0-4a45-992f-3087b76c232e'
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

DATASET_ID = '1616a329-1bf0-4a45-992f-3087b76c232e'


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

# Generate UID
def genUID(obs):
    return str(obs['properties']['ID'])


# Reads flood shp and returnse list of insertable rows
def processNewData(exclude_ids):
    # 1. Fetch data from source
    logging.info('Fetching latest data')
    for dest, url in SOURCE_URLS.items():
        urllib.request.urlretrieve(url, os.path.join(DATA_DIR, dest))
        if os.path.splitext(url)[1]=='.zip':
            zip_ref=zipfile.ZipFile(os.path.join(DATA_DIR, dest), 'r')
            zip_ref.extractall(os.path.join(DATA_DIR))
            zip_ref.close()
    # 2. Parse fetched point data and generate unique ids
    logging.info('Parsing point data')
    new_ids = []
    rows = []
    with fiona.open(os.path.join(DATA_DIR, TABFILE), 'r',
                    encoding=ENCODING) as shp:
        logging.debug(shp.schema)
        for obs in shp:
            uid = genUID(obs)
            # Only add new observations unless overwrite
            if uid not in exclude_ids and uid not in new_ids:
                new_ids.append(uid)
                row = []
                for field in CARTO_SCHEMA.keys():
                    if field == 'the_geom':
                        geom = {
                            'type': 'Point',
                            'coordinates': [obs['properties']['long'], obs['properties']['lat']]
                        }
                        row.append(geom)
                    elif field == UID_FIELD:
                        row.append(uid)
                    else:
                        row.append(obs['properties'][field])
                rows.append(row)

    # 3. Insert new point observations
    new_count = len(rows)
    if new_count:
        logging.info('Pushing new rows')
        cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                            CARTO_SCHEMA.values(), rows)

    # 4. Parse fetched shp data and generate unique ids
    logging.info('Parsing shapefile data')
    new_ids = []
    rows = []
    with fiona.open(os.path.join(DATA_DIR, SHPFILE), 'r') as shp:
        logging.debug(shp.schema)
        for obs in shp:
            uid = genUID(obs)
            # Only add new observations unless overwrite
            if uid not in exclude_ids and uid not in new_ids:
                new_ids.append(uid)
                row = []
                for field in CARTO_SCHEMA.keys():
                    if field == 'the_geom':
                        geom = obs['geometry']
                        row.append(geom)
                    elif field == UID_FIELD:
                        row.append(uid)
                    else:
                        row.append(obs['properties'][field[:10]])
                rows.append(row)

    # 5. Insert new shp observations
    new_count = len(rows)
    if new_count:
        logging.info('Pushing new rows')
        cartosql.insertRows(CARTO_TABLE_SHP, CARTO_SCHEMA.keys(),
                            CARTO_SCHEMA.values(), rows)

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

def get_most_recent_date(table):
    r = cartosql.getFields('ended', table, f='csv', post=True)
    dates = r.text.split('\r\n')[1:-1]
    dates.sort()
    most_recent_date = datetime.datetime.strptime(dates[-1], '%Y-%m-%d %H:%M:%S')
    return most_recent_date

def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    # 1. Check if table exists and create table
    existing_ids = checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)

    # 2. Iterively fetch, parse and post new data
    new_ids = processNewData(existing_ids)

    new_count = len(new_ids)
    existing_count = new_count + len(existing_ids)
    logging.info('Total rows: {}, New: {}, Max: {}'.format(
        existing_count, new_count, MAXROWS))

    # 3. Remove old observations
    deleteExcessRows(CARTO_TABLE, MAXROWS, TIME_FIELD, MAXAGE)
    deleteExcessRows(CARTO_TABLE_SHP, MAXROWS, TIME_FIELD, MAXAGE)

    # Get most recent update date
    if new_count > 0:
        most_recent_date = datetime.datetime.utcnow()
        lastUpdateDate(DATASET_ID, most_recent_date)

    logging.info('SUCCESS')
