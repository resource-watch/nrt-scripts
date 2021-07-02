import os
import logging
import sys
from collections import OrderedDict
import cartosql
import datetime
import hashlib
import requests
import time 
import json


logging.basicConfig(stream=sys.stderr, level=logging.INFO)

### Constants
DATA_DIR = 'data'
# max page size = 10000
DATA_URL = 'https://u50g7n0cbj.execute-api.us-east-1.amazonaws.com/v2/measurements?date_from={date_from}T{hour}%3A{minute}%3A{second}&date_to={date_to}T{hour}%3A{minute}%3A{second}&limit=3000&page={page}&sort=desc&has_geo=true&parameter={parameter}&order_by=datetime&sensorType=reference%20grade'
# always check first 15 pages
MIN_PAGES = 15
MAX_PAGES = 100

# how long to wait before trying to get data again incase of failure
WAIT_TIME = 60

# asserting table structure rather than reading from input
PARAMS = ('pm25', 'pm10', 'so2', 'no2', 'o3', 'co', 'bc')
CARTO_TABLES = {
    'pm25':'cit_003a_air_quality_pm25',
    'pm10':'cit_003b_air_quality_pm10',
    'so2':'cit_003c_air_quality_so2',
    'no2':'cit_003d_air_quality_no2',
    'o3':'cit_003e_air_quality_o3',
    'co':'cit_003f_air_quality_co',
    'bc':'cit_003g_air_quality_bc'
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
    ("attribution", "text"),
    ("ppm", "numeric")
])
CARTO_GEOM_TABLE = 'cit_003loc_air_quality'
CARTO_GEOM_SCHEMA = OrderedDict([
    ("the_geom", "geometry"),
    ("_UID", "text"),
    ("location", "text"),
    ("city", "text"),
    ("country", "text")
])


UID_FIELD = '_UID'
TIME_FIELD = 'utc'

CARTO_USER = os.environ.get('CARTO_USER')
CARTO_KEY = os.environ.get('CARTO_KEY')

# Limit to 500000 rows / 30 days
MAXROWS = 500000
MAXAGE = datetime.datetime.now() - datetime.timedelta(days=30)

# conversions
UGM3 = ["\u00b5g/m\u00b3", "ug/m3"]
MOL_WEIGHTS = {
    'so2': 64,
    'no2': 46,
    'o3': 48,
    'co': 28
}

DATASET_ID = {
    'pm25':'ae7227d1-8779-4ca4-a2ce-3c87d53c63f6',
    'pm10':'7c36dbb7-6685-4dc7-b285-7476db05cd5e',
    'so2':'764318db-bb4b-442c-b533-8a3c38768a0c',
    'no2':'5b5c7d9b-baf3-4fdf-a41c-e10506b72770',
    'o3':'9d17e2eb-cc26-4743-a2d6-abf1ebc56376',
    'co':'51861c34-f67a-4662-b0b6-1b7f265c6d23',
    'bc':'0c3ed5b9-94b4-4fc5-9208-bf749f0a5052'
}

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

def convert(param, unit, value):
    if param in MOL_WEIGHTS.keys() and unit in UGM3:
        return convert_ugm3_ppm(value, MOL_WEIGHTS[param])
    return value


def convert_ugm3_ppm(ugm3, mol, T=0, P=101.325):
    # ideal gas conversion
    K = 273.15    # 0C
    Atm = 101.325 # kPa
    return float(ugm3)/mol * 22.414 * (T+K)/K * Atm/P / 1000


# Generate UID
def genUID(obs):
    # location should be unique, plus measurement timestamp
    id_str = '{}_{}'.format(obs['location'], obs['date']['utc'])
    return hashlib.md5(id_str.encode('utf8')).hexdigest()


# Generate UID for location
def genLocID(obs):
    return hashlib.md5(obs['location'].encode('utf8')).hexdigest()


# Parse OpenAQ fields
def parseFields(obs, uid, fields):
    row = []
    for field in fields:
        if field == 'the_geom':
            # construct geojson
            if 'coordinates' in obs and obs['coordinates']:
                geom = {
                    "type": "Point",
                    "coordinates": [
                        obs['coordinates']['longitude'],
                        obs['coordinates']['latitude']
                    ]
                }
                row.append(geom)
            else:
                row.append(None)
        elif field == UID_FIELD:
            row.append(uid)
        elif field == TIME_FIELD:
            row.append(obs['date'][TIME_FIELD])
        elif field == 'attribution':
            try:
                obs['attribution']
            except KeyError:
                row_value='NA'
            else:
                row_value = str(obs['attribution'])
            row.append(row_value)
        elif field == 'ppm':
            ppm = convert(obs['parameter'], obs['unit'], obs['value'])
            row.append(ppm)
        else:
            row.append(obs[field])
    return row


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

def get_most_recent_date(param):
    r = cartosql.getFields(TIME_FIELD, CARTO_TABLES[param], f='csv', post=True)
    dates = r.text.split('\r\n')[1:-1]
    dates.sort()
    most_recent_date = datetime.datetime.strptime(dates[-1], '%Y-%m-%d %H:%M:%S')
    now = datetime.datetime.utcnow()
    if most_recent_date > now:
        most_recent_date = now
    return most_recent_date

def create_headers():
    '''
    Create headers to perform authorized actions on API

    '''
    return {
        'Content-Type': "application/json",
        'Authorization': "{}".format(os.getenv('apiToken')),
    }

def pull_layers_from_API(dataset_id):
    '''
    Pull dictionary of current layers from API
    INPUT   dataset_id: Resource Watch API dataset ID (string)
    RETURN  layer_dict: dictionary of layers (dictionary of strings)
    '''
    # generate url to access layer configs for this dataset in back office
    rw_api_url = 'https://api.resourcewatch.org/v1/dataset/{}/layer?page[size]=100'.format(dataset_id)
    # request data
    r = requests.get(rw_api_url)
    try_num = 1
    while try_num <= 3:
        try: 
            # convert response into json and make dictionary of layers
            layer_dict = json.loads(r.content.decode('utf-8'))['data']
            break
        except:
            logging.info("Failed to fetch layers. Trying again after 30 seconds.")
            time.sleep(30)
            try_num += 1
    return layer_dict

def update_layer(layer):
    '''
    Update layers in Resource Watch back office.
    INPUT   layer: layer that will be updated (string)
    '''
    # get current layer description
    lyr_description = layer['attributes']['description']
    
    # get current date being used from description by string manupulation
    old_date_text =lyr_description.split('between ')[1].split('.')[0]

    # get current date in utc
    current_date = datetime.datetime.utcnow()
    # get text for new date end which will be the current date
    new_date_end = current_date.strftime("%B %d, %Y, %H%M")
    # get most recent starting date, 24 hours ago
    new_date_start = (current_date - datetime.timedelta(hours=24))
    new_date_start = datetime.datetime.strftime(new_date_start, "%B %d, %Y, %H%M")
    # construct new date range by joining new start date and new end date
    new_date_text = new_date_start + ' UTC' + ' and ' + new_date_end + ' UTC'

    # replace date in layer's description with new date
    layer['attributes']['description'] = layer['attributes']['description'].replace(old_date_text, new_date_text)

    # send patch to API to replace layers
    # generate url to patch layer
    rw_api_url_layer = "https://api.resourcewatch.org/v1/dataset/{dataset_id}/layer/{layer_id}".format(
        dataset_id=layer['attributes']['dataset'], layer_id=layer['id'])
    # create payload with new description and layer configuration
    payload = {
        'application': ['rw'],
        'description': layer['attributes']['description']
    }
    # patch API with updates
    r = requests.request('PATCH', rw_api_url_layer, data=json.dumps(payload), headers=create_headers())
    # check response
    # if we get a 200, the layers have been replaced
    # if we get a 504 (gateway timeout) - the layers are still being replaced, but it worked
    if r.ok or r.status_code==504:
        logging.info('Layer replaced: {}'.format(layer['id']))
    else:
        logging.error('Error replacing layer: {} ({})'.format(layer['id'], r.status_code))
        
def main():
    logging.info('BEGIN')

    # 1. Get existing uids, if none create tables
    existing_ids = {}
    for param in PARAMS:
        existing_ids[param] = checkCreateTable(CARTO_TABLES[param],
                                               CARTO_SCHEMA, UID_FIELD,
                                               TIME_FIELD)
    # 1.1 Get separate location table uids
    loc_ids = checkCreateTable(CARTO_GEOM_TABLE, CARTO_GEOM_SCHEMA, UID_FIELD)

    # 2. Iterively fetch, parse and post new data
    # this is done all together because OpenAQ endpoint filter by parameter
    # doesn't work
    new_counts = dict(((param, 0) for param in PARAMS))

    for param in PARAMS:
        page = 1
        retries = 0
        date_from = (datetime.datetime.utcnow()-datetime.timedelta(hours=24)).strftime("%Y-%m-%d")
        date_to = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        hour = datetime.datetime.utcnow().hour
        minute = datetime.datetime.utcnow().minute
        second = datetime.datetime.utcnow().second

        while page <= MIN_PAGES or new_count and page < MAX_PAGES:
            logging.info("Fetching page {}".format(page))
            url = (DATA_URL.format(page = page, date_from = date_from, date_to = date_to, parameter = param, hour = hour, minute = minute, second = second))
            page += 1
            new_count = 0
            
            # separate row lists per param
            rows = dict(((param, []) for param in PARAMS))
            loc_rows = []

            # 2.1 parse data excluding existing observations
            try:
                r = requests.get(url)
                logging.info(r.url)
                results = r.json()['results']
                for obs in results:
                    param = obs['parameter']
                    uid = genUID(obs)
                    if uid not in existing_ids[param]:
                        existing_ids[param].append(uid)
                        rows[param].append(parseFields(obs, uid, CARTO_SCHEMA.keys()))

                        # 2.2 Check if new locations
                        loc_id = genLocID(obs)
                        if loc_id not in loc_ids and 'coordinates' in obs:
                            loc_ids.append(loc_id)
                            loc_rows.append(parseFields(obs, loc_id,
                                                        CARTO_GEOM_SCHEMA.keys()))

                # 2.3 insert new locations
                if len(loc_rows):
                    logging.info('Pushing {} new locations'.format(len(loc_rows)))
                    cartosql.insertRows(CARTO_GEOM_TABLE, CARTO_GEOM_SCHEMA.keys(),
                                        CARTO_GEOM_SCHEMA.values(), loc_rows)

                # 2.4 insert new rows
                count = len(rows[param])
                if count:
                    try_num = 1
                    while try_num <= 3:
                        try:
                            logging.info('Try {}: Pushing {} new {} rows'.format(try_num, count, param))
                            cartosql.insertRows(CARTO_TABLES[param], CARTO_SCHEMA.keys(),
                                                CARTO_SCHEMA.values(), rows[param], blocksize=500)
                            logging.info('Successfully pushed {} new {} rows.'.format(count, param))
                            break
                        except:
                            logging.info('Waiting for {} seconds before trying again.'.format(WAIT_TIME))
                            time.sleep(WAIT_TIME)
                            try_num += 1
                    new_count += count
                new_counts[param] += count

                retries = 0
            # failed to read ['results']
            except Exception as e:
                logging.info('Failed to read results. Waiting for {} seconds before trying again.'.format(WAIT_TIME))
                time.sleep(30)
                retries += 1
                page -= 1
                if retries > 5:
                    raise(e)

        # 3. Remove old observations
        logging.info('Total rows: {}, New: {}, Max: {}'.format(
            len(existing_ids[param]), new_counts[param], MAXROWS))
        deleteExcessRows(CARTO_TABLES[param], MAXROWS, TIME_FIELD, MAXAGE)

        # update layers
        dataset = DATASET_ID[param]
        most_recent_date = get_most_recent_date(param)
        lastUpdateDate(dataset, most_recent_date)
        # Update the dates on layer description
        logging.info('Updating {}'.format(param))
        # pull dictionary of current layers from API
        layer_dict = pull_layers_from_API(dataset)
        # go through each layer, pull the definition and update
        for layer in layer_dict:
            # replace layer description with new dates
            update_layer(layer)

    logging.info('SUCCESS')
