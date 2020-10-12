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
import json

# name of data directory in Docker container
DATA_DIR = 'data'

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# urls for source data
SOURCE_URLS = {
    'f.dat':'http://floodobservatory.colorado.edu/Version3/FloodArchive.DAT',
    'f.id':'http://floodobservatory.colorado.edu/Version3/FloodArchive.ID',
    'f.map':'http://floodobservatory.colorado.edu/Version3/FloodArchive.MAP',
    'f.ind':'http://floodobservatory.colorado.edu/Version3/FloodArchive.IND',
    'f.tab':'http://floodobservatory.colorado.edu/Version3/FloodArchive.TAB',
    'f_shp.zip': 'http://floodobservatory.colorado.edu/Version3/FloodsArchived_shp.zip'
}

# key for the tab-delimited file (FloodArchive.TAB) in SOURCE_URLS
TABFILE = 'f.tab'

# encoding used in the tab-delimited file (FloodArchive.TAB)
ENCODING = 'latin-1'

# name of shapefile retrieved from 'http://floodobservatory.colorado.edu/Version3/FloodsArchived_shp.zip'
SHPFILE = 'FloodsArchived_shape.shp'

# name of table in Carto where we will upload the data for flood events
CARTO_TABLE = 'wat_040_flood_events'

# name of table in Carto where we will upload the data for flood event areas
CARTO_TABLE_SHP = 'wat_040_flood_events_shp'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
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

# column of table that can be used as an unique ID 
UID_FIELD = '_UID'

# column that stores datetime information
TIME_FIELD = 'Began'

# how many rows can be stored in the Carto table before the oldest ones are deleted?
MAXROWS = 1000000

# oldest date that can be stored in the Carto table before we start deleting
MAXAGE = None

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = '1616a329-1bf0-4a45-992f-3087b76c232e'

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
    apiUrl = f'http://api.resourcewatch.org/v1/dataset/{dataset}'
    # create headers to send with the request to update the 'last update date'
    headers = {
    'Content-Type': 'application/json',
    'Authorization': os.getenv('apiToken')
    }
    # create the json data to send in the request
    body = {
        "dataLastUpdated": date.isoformat() # date should be a string in the format 'YYYY-MM-DDTHH:MM:SS'
    }
    # send the request
    try:
        r = requests.patch(url = apiUrl, json = body, headers = headers)
        logging.info('[lastUpdated]: SUCCESS, '+ date.isoformat() +' status code '+str(r.status_code))
        return 0
    except Exception as e:
        logging.error('[lastUpdated]: '+str(e))

'''
FUNCTIONS FOR CARTO DATASETS

The functions below must go in every near real-time script for a Carto dataset.
Their format should not need to be changed.
'''

def checkCreateTable(table, schema, id_field, time_field=''):
    '''
    Create the table if it does not exist, and pull list of IDs already in the table if it does
    INPUT   table: Carto table to check or create (string)
            schema: dictionary of column names and types, used if we are creating the table for the first time (dictionary)
            id_field: name of column that we want to use as a unique ID for this table; this will be used to compare the
                    source data to the our table each time we run the script so that we only have to pull data we
                    haven't previously uploaded (string)
            time_field:  optional, name of column that will store datetime information (string)
    RETURN  list of existing IDs in the table, pulled from the id_field column (list of strings)
    '''
    # check it the table already exists in Carto
    if cartosql.tableExists(table, user=CARTO_USER, key=CARTO_KEY):
        # if the table does exist, get a list of all the values in the id_field column
        logging.info('Fetching existing IDs')
        r = cartosql.getFields(id_field, table, f='csv', post=True, user=CARTO_USER, key=CARTO_KEY)
        # turn the response into a list of strings, removing the first and last entries (header and an empty space at end)
        return r.text.split('\r\n')[1:-1]
    else:
        # if the table does not exist, create it with columns based on the schema input
        logging.info('Table {} does not exist, creating'.format(table))
        cartosql.createTable(table, schema, user=CARTO_USER, key=CARTO_KEY)
        # if a unique ID field is specified, set it as a unique index in the Carto table; when you upload data, Carto
        # will ensure no two rows have the same entry in this column and return an error if you try to upload a row with
        # a duplicate unique ID
        if id_field:
            cartosql.createIndex(table, id_field, unique=True, user=CARTO_USER, key=CARTO_KEY)
        # if a time_field is specified, set it as an index in the Carto table; this is not a unique index
        if time_field:
            cartosql.createIndex(table, time_field, user=CARTO_USER, key=CARTO_KEY)
        # return an empty list because there are no IDs in the new table yet
        return []

'''
FUNCTIONS FOR THIS DATASET

The functions below have been tailored to this specific dataset.
They should all be checked because their format likely will need to be changed.
'''

def genUID(obs):
    '''
    Generate unique id using 'ID' variable from GeoJSON
    INPUT   obs: features from the GeoJSON (GeoJSON feature)
    RETURN  unique id for row (string)
    '''
    return str(obs['properties']['ID'])

def updateEndDate(source_file, table, num_obs_to_update, encoding=None):
    '''
    Update end dates in Carto table for most recent floods
    INPUT   source_file: file that we want to process to get the end dates and geometries (string)
            table: name of table in Carto we want to update (string)
            num_obs_to_update: number of flood events that we want to update (integer)
            encoding: optional, encoding used in the input file (string)
    '''

    # open and read the source_file as GeoJSON
    with fiona.open(os.path.join(DATA_DIR, source_file), 'r', encoding=encoding) as shp:
        # for each flood event, get the current listed end date
        for obs in shp[-num_obs_to_update:]:
            # generate unique id using feature information
            uid = genUID(obs)
            # get end date from 'Ended' feature of the GeoJSON
            end_date = obs['properties']['Ended']
            # construct geometry using the 'long', 'lat' features in GeoJSON
            geom = {
                'type': 'Point',
                'coordinates': [obs['properties']['long'], obs['properties']['lat']]
            }

            # update 'ended' column in Carto table with latest end dates
            requests.get(
                "https://{username}.carto.com/api/v2/sql?q=UPDATE {table} SET {column} = '{value}' WHERE _uid = '{id}' &api_key={api_key}".format(
                    username=os.getenv('CARTO_USER'), table=table, column='ended', value=end_date, id=uid,
                    api_key=os.getenv('CARTO_KEY')))

            # update 'the_geom' column in Carto table with latest geometry
            requests.get("https://{username}.carto.com/api/v2/sql?q=UPDATE {table} SET {column} = '{value}' WHERE _uid = '{id}' &api_key={api_key}".format(
                username=os.getenv('CARTO_USER'), table=table, column='the_geom', value=geom, id=uid,
                api_key=os.getenv('CARTO_KEY')))

def processNewData(existing_ids):
    '''
    Fetch, process and upload new data
    INPUT   existing_ids: list of unique IDs that we already have in our Carto table (list of strings)
    RETURN  num_new: number of rows of new data sent to Carto table (integer)
    '''

    logging.info('Fetching latest data')
    # loop through each url
    for dest, url in SOURCE_URLS.items():
        # pull data from url and save to temporary files
        # The name of the temporary files are constructed using the key of SOURCE_URLS
        urllib.request.urlretrieve(url, os.path.join(DATA_DIR, dest))
        # check if the url is a zip file
        if os.path.splitext(url)[1]=='.zip':
            # open and read temporary files that are in zip format
            zip_ref=zipfile.ZipFile(os.path.join(DATA_DIR, dest), 'r')
            # extract the zip file
            zip_ref.extractall(os.path.join(DATA_DIR))
            # close the zip file
            zip_ref.close()

    # parse fetched point data and generate unique ids
    logging.info('Parsing point data')
    # create an empty list to store unique ids of new data we will be sending to Carto table
    new_ids = []
    # create an empty list to store each row of new data
    rows = []
    # open and read the file specified by the variable TABFILE as GeoJSON
    with fiona.open(os.path.join(DATA_DIR, TABFILE), 'r',
                    encoding=ENCODING) as shp:
        logging.debug(shp.schema)
        # loop through each feature in the GeoJSON
        for obs in shp:
            # generate unique id using feature information
            uid = genUID(obs)
            # if the unique id doesn't already exist in Carto table and not included to 
            # new_ids list yet
            if uid not in existing_ids and uid not in new_ids:
                # append the id to the list for sending to Carto 
                new_ids.append(uid)
                # create an empty list to store data from this row
                row = []
                # go through each column in the Carto table
                for field in CARTO_SCHEMA.keys():
                    # if we are fetching data for geometry column
                    if field == 'the_geom':
                        # construct geometry using the 'long', 'lat' features in GeoJSON
                        geom = {
                            'type': 'Point',
                            'coordinates': [obs['properties']['long'], obs['properties']['lat']]
                        }
                        # add geometry to the list of data from this row
                        row.append(geom)
                    # if we are fetching data for unique id column
                    elif field == UID_FIELD:
                        # add the unique id to the list of data from this row
                        row.append(uid)
                    else:
                        # add data for remaining fields to the list of data from this row
                        row.append(obs['properties'][field])
                # add the list of values from this row to the list of new data
                rows.append(row)

    # find the length (number of rows) of new_data 
    new_count = len(rows)
    if new_count:
        logging.info('Pushing new rows')
        # insert new data into the carto table
        cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                            CARTO_SCHEMA.values(), rows, user=os.getenv('CARTO_USER'),key=os.getenv('CARTO_KEY'))

    # parse fetched shp data and generate unique ids
    logging.info('Parsing shapefile data')
    # create an empty list to store unique ids of new data we will be sending to Carto table
    new_ids = []
    # create an empty list to store each row of new data
    rows = []
    # open and read the shapefile specified by the variable SHPFILE as GeoJSON
    with fiona.open(os.path.join(DATA_DIR, SHPFILE), 'r') as shp:
        logging.debug(shp.schema)
        # loop through each feature in the GeoJSON
        for obs in shp:
            # generate unique id using feature information
            uid = genUID(obs)
            # if the unique id doesn't already exist in Carto table and not included to 
            # new_ids list yet
            if uid not in existing_ids and uid not in new_ids:
                # append the id to the list for sending to Carto 
                new_ids.append(uid)
                # create an empty list to store data from this row
                row = []
                # go through each column in the Carto table
                for field in CARTO_SCHEMA.keys():
                    # if we are fetching data for geometry column
                    if field == 'the_geom':
                        # get geometry from the 'geometry' feature in GeoJSON
                        geom = obs['geometry']
                        # add geometry to the list of data from this row
                        row.append(geom)
                    # if we are fetching data for unique id column
                    elif field == UID_FIELD:
                        # add the unique id to the list of data from this row
                        row.append(uid)
                    else:
                        # add data for remaining fields to the list of data from this row
                        # field names get gut off after 10 characters in shapefile, so the names should be the
                        # first 10 characters of the specified field name
                        row.append(obs['properties'][field[:10]])
                # add the list of values from this row to the list of new data
                rows.append(row)

    # find the length (number of rows) of new_data 
    new_count = len(rows)
    # check if new data is available
    if new_count:
        logging.info('Pushing new rows')
        # insert new data into the carto table
        cartosql.insertRows(CARTO_TABLE_SHP, CARTO_SCHEMA.keys(),
                            CARTO_SCHEMA.values(), rows, user=os.getenv('CARTO_USER'),key=os.getenv('CARTO_KEY'))

    # Update end dates for most recent floods because they are updated if the flood is still happening
    # Update end dates and geometry for point data
    logging.info('Updating end dates for point data')
    updateEndDate(TABFILE, CARTO_TABLE, 20, encoding=ENCODING)
    # Update end dates and geometry for shapefile data
    logging.info('Updating end dates for shapefile data')
    updateEndDate(SHPFILE, CARTO_TABLE_SHP, 20)

    # length (number of rows) of new_data 
    num_new = len(new_ids)

    return num_new


def deleteExcessRows(table, max_rows, time_field, max_age=''):
    ''' 
    Delete rows that are older than a certain threshold and also bring count down to max_rows
    INPUT   table: name of table in Carto from which we will delete excess rows (string)
            max_rows: maximum rows that can be stored in the Carto table (integer)
            time_field: column that stores datetime information (string) 
            max_age(optional): oldest date that can be stored in the Carto table (datetime object)
    ''' 

    # initialize number of rows that will be dropped as 0
    num_dropped = 0
    # check if max_age is a datetime object
    if isinstance(max_age, datetime.datetime):
        # convert max_age to a string 
        max_age = max_age.isoformat()

    # if the max_age variable exists
    if max_age:
        # delete rows from table which are older than the max_age
        r = cartosql.deleteRows(table, "{} < '{}'".format(time_field, max_age), user=CARTO_USER, key=CARTO_KEY)
        # get the number of rows that were dropped from the table
        num_dropped = r.json()['total_rows']

    # get cartodb_ids from carto table sorted by date (new->old)
    r = cartosql.getFields('cartodb_id', table, order='{} desc'.format(time_field),
                           f='csv', user=CARTO_USER, key=CARTO_KEY)
    # turn response into a list of strings of the ids
    ids = r.text.split('\r\n')[1:-1]

    # if number of rows is greater than max_rows, delete excess rows
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[max_rows:], user=CARTO_USER, key=CARTO_KEY)
        # get the number of rows that have been dropped from the table
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))

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
    # convert response into json and make dictionary of layers
    layer_dict = json.loads(r.content.decode('utf-8'))['data']
    return layer_dict

def update_layer(layer, new_date):
    '''
    Update layers in Resource Watch back office.
    INPUT   layer: layer that will be updated (string)
            new_date: date of asset to be shown in this layer (datetime)
    '''
    # get current layer titile
    title = layer['attributes']['name']    
    # get current date being used from title by string manupulation
    old_date_text = title.split(' Flood')[0]
    # get text for new date end which will be the current date
    new_date_end = new_date.strftime("%B %d, %Y")
    # get most recent starting date, 30 days ago
    new_date_start = (new_date - datetime.timedelta(days=29))
    new_date_start = datetime.datetime.strftime(new_date_start, "%B %d, %Y")
    # construct new date range by joining new start date and new end date
    new_date_text = new_date_start + ' - ' + new_date_end
    
    # replace date in layer's title with new date
    layer['attributes']['name'] = layer['attributes']['name'].replace(old_date_text, new_date_text)

    # send patch to API to replace layers
    # generate url to patch layer
    rw_api_url_layer = "https://api.resourcewatch.org/v1/dataset/{dataset_id}/layer/{layer_id}".format(
        dataset_id=layer['attributes']['dataset'], layer_id=layer['id'])
    # create payload with new title and layer configuration
    payload = {
        'application': ['rw'],
        'name': layer['attributes']['name']
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
        
def updateResourceWatch(num_new):
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date' and updating any dates on layers
    INPUT   num_new: number of new rows in Carto table (integer)
    '''
    # If there are new entries in the Carto table
    if num_new>0:
        # Update dataset's last update date on Resource Watch
        most_recent_date = datetime.datetime.utcnow()
        lastUpdateDate(DATASET_ID, most_recent_date)
        # Update the dates on layer legends
        logging.info('Updating {}'.format(CARTO_TABLE))
        # pull dictionary of current layers from API
        layer_dict = pull_layers_from_API(DATASET_ID)
        # go through each layer, pull the definition and update
        for layer in layer_dict:
            # replace layer title with new dates
            update_layer(layer, most_recent_date)

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Check if table exists, create it if it does not
    logging.info('Checking if table exists and getting existing IDs.')
    existing_ids = checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)

    # Fetch, process, and upload new data
    num_new = processNewData(existing_ids)
    
    logging.info('Previous rows: {},  New rows: {}, Max: {}'.format(len(existing_ids), num_new, MAXROWS))

    # Remove old observations
    deleteExcessRows(CARTO_TABLE, MAXROWS, TIME_FIELD, MAXAGE)
    deleteExcessRows(CARTO_TABLE_SHP, MAXROWS, TIME_FIELD, MAXAGE)

    # Update Resource Watch
    updateResourceWatch(num_new)

    logging.info('SUCCESS')
