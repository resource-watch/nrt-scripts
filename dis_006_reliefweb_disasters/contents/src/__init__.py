import logging
import sys
import os
from collections import OrderedDict
import cartosql
import requests
import datetime
import json
import hashlib

### Constants
LIMIT = 1000
SOURCE_URL = "https://api.reliefweb.int/v1/disasters?preset=latest&limit={}&profile=full".format(LIMIT)

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S+00:00'
UID_FIELD='uid'

LOG_LEVEL = logging.INFO
# Clear table first each time we retrieve data because the status of events may change. This way we can easily get the latest status in the Carto table
CLEAR_TABLE_FIRST = True

### Table name and structure
CARTO_TABLE = 'dis_006_reliefweb_disasters'
CARTO_TABLE_INTERACTION = 'dis_006_reliefweb_disasters_interaction'

CARTO_SCHEMA = OrderedDict([
    ("the_geom", "geometry"),
    ("uid", "text"),
    ("event_id", "numeric"),
    ("event_name", "text"),
    ("description", "text"),
    ("status", "text"),
    ("date", "timestamp"),
    ("glide", "text"),
    ("related_glide", "text"),
    ("featured", "text"),
    ("primary_country", "text"),
    ("country_name", "text"),
    ("country_shortname", "text"),
    ("country_iso3", "text"),
    ("current", "text"),
    ("event_type_ids", "text"),
    ("event_types", "text"),
    ("url", "text"),
    ("lon", "numeric"),
    ("lat", "numeric")
])

CARTO_SCHEMA_INTERACTION = OrderedDict([
    ("the_geom", "geometry"),
    ("uid", "text"),
    ("country_name", "text"),
    ("country_shortname", "text"),
    ("country_iso3", "text"),
    ("interaction", "text")
])
AGE_FIELD = 'date'
# Table limits
MAX_ROWS = 1000000

DATASET_ID = '4919be3a-c543-4964-a224-83ef801370de'
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

###
## Carto code
###

def gen_uid(event_id, country_id):
    '''Generate unique id'''
    id_str = '{}_{}'.format(event_id, country_id)
    return hashlib.md5(id_str.encode('utf8')).hexdigest()

def gen_interaction_uid(ctry_iso3):
    '''Generate unique id based on country code'''
    return hashlib.md5(ctry_iso3.encode('utf8')).hexdigest()

def checkCreateTable(table, schema, id_field, time_field=''):
    '''Get existing ids or create table'''
    if cartosql.tableExists(table):
        logging.info('Fetching existing IDs')
        r = cartosql.getFields(id_field, table, f='csv', post=True)
        return r.text.split('\r\n')[1:-1]
    else:
        logging.info('Table {} does not exist, creating'.format(table))
        cartosql.createTable(table, schema)
        cartosql.createIndex(table, id_field, unique=True)
        if time_field:
            cartosql.createIndex(table, time_field)
    return []


def deleteExcessRows(table, max_rows, age_field):
    '''Delete excess rows by age or count'''
    num_dropped = 0

    # get sorted ids (old->new)
    r = cartosql.getFields('cartodb_id', table, order='{}'.format(age_field.lower()),
                           f='csv')
    ids = r.text.split('\r\n')[1:-1]

    #  delete excess
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[:-max_rows])
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))
    return num_dropped

def processData(existing_ids):
    new_data = []
    new_ids = []

    r = requests.get(SOURCE_URL)
    data_bytes=r.content
    decoded=data_bytes.decode('utf8')
    json_data = json.loads(decoded)
    data_dict = json_data['data']

    for entry in data_dict:
        event_id =entry['id']
        ids = []
        names = []
        for t in entry['fields']['type']:
            ids.append(t['id'])
            names.append(t['name'])
        ids=', '.join(map(str, ids))
        names=', '.join(map(str, names))
        for country in entry['fields']['country']:
            country_id = country['id']
            uid = gen_uid(event_id, country_id)
            if uid not in existing_ids + new_ids:
                new_ids.append(uid)
                row = []
                for key in CARTO_SCHEMA.keys():
                    try:
                        if key == 'the_geom':
                            lon = country['location']['lon']
                            lat = country['location']['lat']
                            item = {
                                'type': 'Point',
                                'coordinates': [lon, lat]
                            }
                        elif key=='uid':
                            item = uid
                        elif key=='event_id':
                            item = int(event_id)
                        elif key=='event_name':
                            item = entry['fields']['name']
                        elif key=='description':
                            item = entry['fields']['description']
                        elif key=='status':
                            item = entry['fields']['status']
                        elif key=='date':
                            item = datetime.datetime.strptime(entry['fields']['date']['created'],DATETIME_FORMAT)
                        elif key=='glide':
                            item = entry['fields']['glide']
                        elif key=='related_glide':
                            item = ', '.join(map(str, entry['fields']['related_glide']))
                        elif key=='featured':
                            item = str(entry['fields']['featured'])
                        elif key=='primary_country':
                            item = entry['fields']['primary_country']['iso3']
                        elif key=='country_name':
                            item = country['name']
                        elif key=='country_shortname':
                            item = country['shortname']
                        elif key=='country_iso3':
                            item = country['iso3']
                        elif key== 'current':
                            item = str(entry['fields']['current'])
                        elif key == 'event_type_ids':
                            item = ids
                        elif key == 'event_types':
                            item = names
                        elif key == 'url':
                            item = entry['fields']['url']
                        elif key == 'lon':
                            item = country['location']['lon']
                        elif key == 'lat':
                            item = country['location']['lat']
                    except KeyError:
                        item=None
                    row.append(item)
                new_data.append(row)
    num_new = len(new_ids)
    if num_new:
        logging.info('Adding {} new records'.format(num_new))
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), new_data, user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))
    return(num_new)

def processInteractions():
    r = cartosql.get("SELECT * FROM {} WHERE current='True'".format(CARTO_TABLE),
                     user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))
    interaction_data = r.json()['rows']
    countries_with_interaction=[]
    for interaction in interaction_data:
        ctry = interaction['country_iso3']
        if ctry not in countries_with_interaction:
            countries_with_interaction.append(ctry)
    if cartosql.tableExists(CARTO_TABLE_INTERACTION):
        cartosql.dropTable(CARTO_TABLE_INTERACTION)
    #run to create new table
    existing_interaction_ids = checkCreateTable(CARTO_TABLE_INTERACTION, CARTO_SCHEMA_INTERACTION, UID_FIELD)
    new_interactions=[]
    for ctry in countries_with_interaction:
        r = cartosql.get("SELECT * FROM {} WHERE current='True' AND country_iso3='{}'".format(CARTO_TABLE, ctry),
                         user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))
        ctry_interaction_data = r.json()['rows']
        event_num=1
        for interaction in ctry_interaction_data:
            event = interaction['event_name'].split(": ",1)
            if event_num == 1:
                if len(event)==1:
                    interaction_str = '{} ({})'.format(event[0], interaction['url'])
                else:
                    interaction_str = '{} ({})'.format(event[1], interaction['url'])
            else:
                if len(event)==1:
                    interaction_str = interaction_str + '; ' + '{} ({})'.format(event[0], interaction['url'])
                else:
                    interaction_str = interaction_str + '; ' + '{} ({})'.format(event[1], interaction['url'])
            event_num+=1
        uid = gen_interaction_uid(ctry)
        row = []
        for key in CARTO_SCHEMA_INTERACTION.keys():
            try:
                if key == 'the_geom':
                    lon = ctry_interaction_data[0]['lon']
                    lat = ctry_interaction_data[0]['lat']
                    item = {
                        'type': 'Point',
                        'coordinates': [lon, lat]
                    }
                elif key=='interaction':
                    item=interaction_str
                else:
                    item = ctry_interaction_data[0][key]
            except KeyError:
                item=None
            row.append(item)
        new_interactions.append(row)
    logging.info('Adding {} new interactions'.format(len(new_interactions)))
    cartosql.blockInsertRows(CARTO_TABLE_INTERACTION, CARTO_SCHEMA_INTERACTION.keys(), CARTO_SCHEMA_INTERACTION.values(), new_interactions, user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))


def get_most_recent_date(table):
    r = cartosql.getFields(AGE_FIELD, table, f='csv', post=True)
    dates = r.text.split('\r\n')[1:-1]
    dates.sort()
    most_recent_date = datetime.datetime.strptime(dates[-1], DATETIME_FORMAT)
    return most_recent_date


def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    if CLEAR_TABLE_FIRST:
        logging.info('Clearing Table')
        if cartosql.tableExists(CARTO_TABLE):
            cartosql.dropTable(CARTO_TABLE)

    ### 1. Check if table exists, if not, create it
    logging.info('Checking if table exists and getting existing IDs.')
    existing_ids = checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD)
    num_existing = len(existing_ids)

    ### 2. Fetch data from FTP, dedupe, process
    logging.info('Fetching new data')
    num_new = processData(existing_ids)
    logging.info('Processing interactions')
    processInteractions()

    ### 3. Delete data to get back to MAX_ROWS
    logging.info('Deleting excess rows')
    num_dropped = deleteExcessRows(CARTO_TABLE, MAX_ROWS, AGE_FIELD)

    ### 4. Notify results
    total = num_existing + num_new - num_dropped

    # If updates, change update date on RW
    if num_new>0:
        lastUpdateDate(DATASET_ID, datetime.datetime.utcnow())

    logging.info('Existing rows: {},  New rows: {}, Max: {}'.format(total, num_new, MAX_ROWS))
    logging.info("SUCCESS")
