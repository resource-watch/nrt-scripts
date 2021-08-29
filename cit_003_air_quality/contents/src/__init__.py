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
import boto3
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
import ndjson


logging.basicConfig(stream=sys.stderr, level=logging.INFO)

### Constants
DATA_DIR = 'data'
# how long to wait before trying to get data again incase of failure
WAIT_TIME = 30
# asserting table structure rather than reading from input
PARAMS = ('pm25', 'pm10', 'so2', 'no2', 'o3', 'co', 'bc')
# the name of the seven carto tables to store the data 
CARTO_TABLES = {
    'pm25':'cit_003a_air_quality_pm25',
    'pm10':'cit_003b_air_quality_pm10',
    'so2':'cit_003c_air_quality_so2',
    'no2':'cit_003d_air_quality_no2',
    'o3':'cit_003e_air_quality_o3',
    'co':'cit_003f_air_quality_co',
    'bc':'cit_003g_air_quality_bc'
}
# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
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
# the name of the seven carto tables to store the geometry data 
CARTO_GEOM_TABLE = 'cit_003loc_air_quality'
# column names and types for the geometry data table
CARTO_GEOM_SCHEMA = OrderedDict([
    ("the_geom", "geometry"),
    ("_UID", "text"),
    ("location", "text"),
    ("city", "text"),
    ("country", "text")
])

# column of table that can be used as a unique ID (UID)
UID_FIELD = '_UID'
# column of table that can be used as a timestamp
TIME_FIELD = 'utc'

# carto username and API key for account where we will store the data
CARTO_USER = os.environ.get('CARTO_USER')
CARTO_KEY = os.environ.get('CARTO_KEY')

# AWS S3 access key and secret key
AWS_ACCESS_KEY_ID = os.environ.get('S3_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = os.environ.get('S3_SECRET_KEY')

# limit to 350000 rows / 7 days
MAXROWS = 350000
MAXAGE = datetime.datetime.utcnow() - datetime.timedelta(days=7)

# conversion units and parameters
UGM3 = ["\u00b5g/m\u00b3", "ug/m3"]
MOL_WEIGHTS = {
    'so2': 64,
    'no2': 46,
    'o3': 48,
    'co': 28
}

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = {
    'pm25':'ae7227d1-8779-4ca4-a2ce-3c87d53c63f6',
    'pm10':'7c36dbb7-6685-4dc7-b285-7476db05cd5e',
    'so2':'764318db-bb4b-442c-b533-8a3c38768a0c',
    'no2':'5b5c7d9b-baf3-4fdf-a41c-e10506b72770',
    'o3':'9d17e2eb-cc26-4743-a2d6-abf1ebc56376',
    'co':'51861c34-f67a-4662-b0b6-1b7f265c6d23',
    'bc':'0c3ed5b9-94b4-4fc5-9208-bf749f0a5052'
}

'''
FUNCTIONS FOR ALL DATASETS

The functions below must go in every near real-time script.
Their format should not need to be changed.
'''
def lastUpdateDate(dataset, date):
    '''
    Given a Resource Watch dataset's API ID and a datetime,
    this function will update the dataset's 'last update date' on the API with the given datetime
    INPUT   dataset: Resource Watch API dataset ID (string)
            date: date to set as the 'last update date' for the input dataset (datetime)
    '''
    # generate the API url for this dataset
    apiUrl = 'http://api.resourcewatch.org/v1/dataset/{0}'.format(dataset)
    # create headers to send with the request to update the 'last update date'
    headers = {
    'Content-Type': 'application/json',
    'Authorization': os.getenv('apiToken')
    }
    # create the json data to send in the request
    body = {
        "dataLastUpdated": date.isoformat()
    }
    # send the request
    try:
        r = requests.patch(url = apiUrl, json = body, headers = headers)
        logging.info('[lastUpdated]: SUCCESS, '+ date.isoformat() +' status code '+str(r.status_code))
        return 0
    except Exception as e:
        logging.error('[lastUpdated]: '+str(e))

def delete_local():
    '''
    Delete all files and folders in Docker container's data directory
    '''
    try:
        # for each object in the data directory
        for f in os.listdir(DATA_DIR):
            # try to remove it as a file
            try:
                logging.info('Removing {}'.format(f))
                os.remove(DATA_DIR+'/'+f)
            # if it is not a file, remove it as a folder
            except:
                shutil.rmtree(f, ignore_errors=True)
    except NameError:
        logging.info('No local files to clean.')

'''
FUNCTIONS FOR CARTO DATASETS

The functions below must go in every near real-time script for a Carto dataset.
Their format should not need to be changed.
'''
def checkCreateTable(table, schema, id_field, time_field=''):
    '''
    Create the table if it does not exist, and pull list of IDs already in the table if it does
    INPUT   table: Carto table to check or create (string)
            schema: dictionary of column names and types, used if we are creating the table for the first time (dictionary of strings)
            id_field: name of column that we want to use as a unique ID for this table; this will be used to compare the
                      source data to the our table each time we run the script so that we only have to pull data we
                      haven't previously uploaded (string)
            time_field: optional, name of column that will store datetime information (string)
    RETURN  list of existing IDs in the table, pulled from the id_field column (list of strings)
    '''
    # check it the table already exists in Carto
    if cartosql.tableExists(table):
        # if the table does exist, get a list of all the values in the id_field column
        logging.info('Fetching existing IDs')
        r = cartosql.getFields(id_field, table, f='csv', post=True)
        # turn the response into a list of strings, removing the first and last entries (header and an empty space at end)
        return r.text.split('\r\n')[1:-1]
    else:
        # if the table does not exist, create it with columns based on the schema input
        logging.info('Table {} does not exist, creating'.format(table))
        cartosql.createTable(table, schema)
        # if a unique ID field is specified, set it as a unique index in the Carto table; when you upload data, Carto
        # will ensure no two rows have the same entry in this column and return an error if you try to upload a row with
        # a duplicate unique ID
        cartosql.createIndex(table, id_field, unique=True)
        # if a time_field is specified, set it as an index in the Carto table; this is not a unique index
        if time_field:
            cartosql.createIndex(table, time_field)
        # return an empty list because there are no IDs in the new table yet
        return []

'''
FUNCTIONS FOR THIS DATASET

The functions below have been tailored to this specific dataset.
They should all be checked because their format likely will need to be changed.
'''
def convert(param, unit, value):
    '''
    Unit conversion
    INPUT  param: observation parameter (string)
           unit: observation unit (string)
           value: observation value for conversion (number)
    OUTPUT new value in ppm (number)
    '''
    if param in MOL_WEIGHTS.keys() and unit in UGM3:
        return convert_ugm3_ppm(value, MOL_WEIGHTS[param])
    return value

def convert_ugm3_ppm(ugm3, mol, T=0, P=101.325):
    '''
    Ideal gas conversion
    INPUT  ugm3: value in ug/m3 (number)
           mol: molar mass parameter (number)
           T: temperature constant (number)
           P: pressure constant (number)
    OUTPUT new value in ppm (number)
    '''
    K = 273.15    # 0C
    Atm = 101.325 # kPa
    return float(ugm3)/mol * 22.414 * (T+K)/K * Atm/P / 1000

def genUID(obs):
    '''
    Generate unique ID (UID)
    INPUT  obs: observation in requested json (dictionary of strings)
    OUTPUT unique ID for observation (string)   
    '''
    # location should be unique, plus measurement timestamp
    id_str = '{}_{}'.format(obs['location'], obs['date']['utc'])
    return hashlib.md5(id_str.encode('utf8')).hexdigest()

def genLocID(obs):
    '''
    Generate unique ID (UID) for location
    INPUT  obs: observation in requested json (dictionary of strings)
    OUTPUT unique ID for location (string)
    '''
    return hashlib.md5(obs['location'].encode('utf8')).hexdigest()

def parseFields(obs, uid, fields):
    '''
    Parse OpenAQ fields
    INPUT   obs: observation in requested json (dictionary of strings)
            uid: Unique ID (UID) (string)
            fields: column names for data table (list of strings)
    OUTPUT  data saved as Carto table rows (list of strings)
    '''
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

def deleteExcessRows(table, max_rows, time_field, max_age=''):
    '''
    Delete excess rows by age or count
    INPUT   table: Carto table to check (string)
            max_rows: row limitation (number)
            time_field: column of table that can be used as a timestamp (string)
            max_age: age limitation (datetime)
    '''
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
    '''
    Get the most recent date of records in the Carto table
    INPUT   param: parameter to choose the Carto table (string)
    OUTPUT  most_recent_date: get the most recent date (datetime)
    '''
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
    OUTPUT headers (dictionary of strings)
    '''
    return {
        'Content-Type': "application/json",
        'Authorization': "{}".format(os.getenv('apiToken')),
    }

def pull_layers_from_API(dataset_id):
    '''
    Pull dictionary of current layers from API
    INPUT   dataset_id: Resource Watch API dataset ID (string)
    OUTPUT  layer_dict: dictionary of layers (dictionary of strings)
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

def update_layer(layer, param):
    '''
    Update layers in Resource Watch back office.
    INPUT   layer: layer that will be updated (string)
            param: observation parameter (string)
    '''
    # get current layer description
    lyr_description = layer['attributes']['description']
    
    # get current date being used from description by string manupulation
    old_date_text =lyr_description.split('between ')[1].split('.')[0]

    # get most recent date in Carto table
    most_recent_date = get_most_recent_date(param)
    # get text for new date start which will be the most recent date
    new_date_start = most_recent_date.strftime("%B %d, %Y")
    # get new end date, 24 hours after the start date
    new_date_end = (most_recent_date + datetime.timedelta(hours=24)).strftime("%B %d, %Y")
    # construct new date range by joining new start date and new end date
    new_date_text = new_date_start + ' 00:00 UTC' + ' and ' + new_date_end + ' 00:00 UTC'

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
    # 1.1 Get most recent date from each table 
    existing_ids = {}
    most_recent_dates = {}
    for param in PARAMS:
        existing_ids[param] = checkCreateTable(CARTO_TABLES[param],
                                               CARTO_SCHEMA, UID_FIELD,
                                               TIME_FIELD)
        most_recent_dates[param] = get_most_recent_date(param)
    # 1.2 Get separate location table uids
    loc_ids = checkCreateTable(CARTO_GEOM_TABLE, CARTO_GEOM_SCHEMA, UID_FIELD)

    # 2. Iterively fetch, parse and post new data
    new_counts = dict(((param, 0) for param in PARAMS))

    # set date_from to 24 hours ago
    date_from = (datetime.datetime.utcnow()-datetime.timedelta(hours=24)).strftime("%Y-%m-%d")
    # date_from in datetime format
    date_from_datetime = datetime.datetime.strptime(date_from, '%Y-%m-%d')
    # date_to in datetime format
    date_to_datetime = date_from_datetime + datetime.timedelta(hours=24)

    # set up AWS S3 for downloading
    s3 = boto3.client('s3', 'us-east-1', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
    
    raw_data_files=[]
    # download all the json file in the date folder
    for key in s3.list_objects(Bucket='openaq-fetches', Prefix = f'realtime/{date_from}')['Contents']:
        # download to local location
        dest_pathname = os.path.join(DATA_DIR, "".join((key['Key'].split("/")[2]).partition("ndjson")[0:2]))
        # save raw file locations
        raw_data_files.append(dest_pathname)
        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)
        # download the file
        s3.download_file('openaq-fetches', key['Key'], dest_pathname)
    
    results=[]
    # loop through all the json files
    for idx, raw_data_file in enumerate(raw_data_files):
        logging.info("Fetching {}/{} data on {}".format((idx+1), len(raw_data_files), date_from))

        with open(raw_data_file) as f:
            results = ndjson.load(f)
        
        # separate row lists per param
        rows = dict(((param, []) for param in PARAMS))
        loc_rows = []

        with ThreadPoolExecutor() as executor:
            for obs in results:
                if datetime.datetime.strptime(obs['date']['utc'], '%Y-%m-%dT%H:%M:%S.000Z') >= date_from_datetime and datetime.datetime.strptime(obs['date']['utc'], '%Y-%m-%dT%H:%M:%S.000Z') < date_to_datetime and 'coordinates' in obs and obs['value'] >= 0:
                    uid = genUID(obs)
                    param = obs['parameter']
                    # 2.1 parse data excluding existing observations
                    if datetime.datetime.strptime(obs['date']['utc'], '%Y-%m-%dT%H:%M:%S.000Z') > most_recent_dates[param]:
                        existing_ids[param].append(uid)
                        rows[param].append(executor.submit(parseFields,obs, uid, CARTO_SCHEMA.keys()).result())
                        # 2.2 Check if new locations
                        loc_id = genLocID(obs)
                        if loc_id not in loc_ids and 'coordinates' in obs:
                            loc_ids.append(loc_id)
                            loc_rows.append(executor.submit(parseFields,obs, loc_id, CARTO_GEOM_SCHEMA.keys()).result())
                    
                    elif uid not in existing_ids[param]: 
                        existing_ids[param].append(uid)
                        rows[param].append(executor.submit(parseFields,obs, uid, CARTO_SCHEMA.keys()).result())
                        # 2.2 Check if new locations
                        loc_id = genLocID(obs)
                        if loc_id not in loc_ids and 'coordinates' in obs:
                            loc_ids.append(loc_id)
                            loc_rows.append(executor.submit(parseFields,obs, loc_id, CARTO_GEOM_SCHEMA.keys()).result())

        # 2.3 insert new locations
        if len(loc_rows):
            logging.info('Pushing {} new locations'.format(len(loc_rows)))
            with ThreadPoolExecutor(max_workers=10) as executor:
                executor.submit(cartosql.insertRows, CARTO_GEOM_TABLE, CARTO_GEOM_SCHEMA.keys(),
                                CARTO_GEOM_SCHEMA.values(), loc_rows)

        for param in PARAMS: 
            # 2.4 insert new rows
            count = len(rows[param])
            if count:
                try_num = 1
                while try_num <= 3:
                    try:
                        logging.info('Try {}: Pushing {} new {} rows'.format(try_num, count, param))
                        with ThreadPoolExecutor(max_workers=10) as executor:
                            executor.submit(cartosql.insertRows, CARTO_TABLES[param], CARTO_SCHEMA.keys(),
                                            CARTO_SCHEMA.values(), rows[param])
                        logging.info('Successfully pushed {} new {} rows.'.format(count, param))
                        break
                    except:
                        logging.info('Waiting for {} seconds before trying again.'.format(WAIT_TIME))
                        time.sleep(WAIT_TIME)
                        try_num += 1
            new_counts[param] += count

    for param in PARAMS:            
        # remove old observations
        logging.info('Total rows: {}, New: {}, Max: {}'.format(len(existing_ids[param]), new_counts[param], MAXROWS))
        deleteExcessRows(CARTO_TABLES[param], MAXROWS, TIME_FIELD, MAXAGE)

        # update layers
        dataset = DATASET_ID[param]
        # get most recent date in Carto table
        most_recent_date = get_most_recent_date(param)
        lastUpdateDate(dataset, most_recent_date)
        # Update the dates on layer description
        logging.info('Updating {}'.format(param))
        # pull dictionary of current layers from API
        layer_dict = pull_layers_from_API(dataset)
        # go through each layer, pull the definition and update
        for layer in layer_dict:
            # replace layer description with new dates
            update_layer(layer, param)
    
    # Delete local files in Docker container
    delete_local()

    logging.info('SUCCESS')