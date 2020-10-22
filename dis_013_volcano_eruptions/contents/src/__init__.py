import logging
import sys
import os
from collections import OrderedDict
import cartosql
import lxml
from xmljson import parker as xml2json
import requests
import datetime
import json

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = False

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'dis_013_volcano_eruptions'

# column of table that can be used as a unique ID (UID)
UID_FIELD = 'Eruption_Number'

# column that stores datetime information
AGE_FIELD = 'StartDateYear'

# column names and types for source data table
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA_UPPER = OrderedDict([
    ("the_geom", "geometry"),
    ("Volcano_Number", "numeric"),
    ("Volcano_Name", "text"),
    ("Eruption_Number", "numeric"),
    ("Activity_Type", "text"),
    ("ActivityArea", "text"),
    ("ActivityUnit", "text"),
    ("ExplosivityIndexMax", "numeric"),
    ("ExplosivityIndexModifier", "text"),
    ("StartEvidenceMethod", "text"),
    ("StartDateYearModifier", "text"),
    ("StartDateYear", "numeric"),
    ("StartDateYearUncertainty", "numeric"),
    ("StartDateMonth", "numeric"),
    ("StartDateDayModifier", "text"),
    ("StartDateDay", "numeric"),
    ("StartDateDayUncertainty", "numeric"),
    ("EndDateYearModifier", "text"),
    ("EndDateYear", "numeric"),
    ("EndDateYearUncertainty", "numeric"),
    ("EndDateMonth", "numeric"),
    ("EndDateDayModifier", "text"),
    ("EndDateDay", "numeric"),
    ("EndDateDayUncertainty", "numeric")
])
# column names for Carto should be lowercase, so we will make the source column names lowercase
CARTO_SCHEMA = OrderedDict([(key.lower(), value) for key,value in CARTO_SCHEMA_UPPER.items()])

# how many rows can be stored in the Carto table before the oldest ones are deleted?
MAX_ROWS = 1000000

# url for volcano eruptions data
SOURCE_URL = "http://webservices.volcano.si.edu/geoserver/GVP-VOTW/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=GVP-VOTW:Smithsonian_VOTW_Holocene_Eruptions"

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = 'f2016c79-82f7-466e-b4db-2c734dd5706d'

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

def deleteExcessRows(table, max_rows, age_field):
    ''' 
    Delete rows to bring count down to max_rows
    INPUT   table: name of table in Carto from which we will delete excess rows (string)
            max_rows: maximum rows that can be stored in the Carto table (integer)
            age_field: column that stores datetime information (string) 
    RETURN  num_dropped: number of rows that have been dropped from the table (integer)
    ''' 
    # initialize number of rows that will be dropped as 0
    num_dropped = 0

    # get cartodb_ids from carto table sorted by date (new->old)
    r = cartosql.getFields('cartodb_id', table, order='{} desc'.format(age_field),
                           f='csv', user=CARTO_USER, key=CARTO_KEY)
    # turn response into a list of strings of the ids
    ids = r.text.split('\r\n')[1:-1]

    # if number of rows is greater than max_rows, delete excess rows
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[max_rows:], CARTO_USER, CARTO_KEY)
        # get the number of rows that have been dropped from the table
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))

    return(num_dropped)

def processData(url, existing_ids):
    '''
    Fetch, process and upload new data
    INPUT   url: url where you can find the source data (string)
            existing_ids: list of date IDs that we already have in our Carto table (list of strings)
    RETURN  num_new: number of rows of new data sent to Carto table (integer)
    '''
    # create an empty list to store new data
    new_data = []
    # create an empty list to store unique ids of new data we will be sending to Carto table
    new_ids = []
    # pull data from the url 
    r = requests.get(url)
    # break down the content retrieved from the url
    xml = lxml.etree.fromstring(r.content)
    # convert xml into Python dictionary structure (json in this case)
    json = xml2json.data(xml)
    # create a list from retrieved json features
    data_dict = list(json.values())[1]
    # loop through to process each record
    for entry in data_dict:
        # get the full volcano eruption data for current index
        data = entry['{volcano.si.edu}Smithsonian_VOTW_Holocene_Eruptions']
        # generate unique id by retrieving eruption number from data
        uid = data['{volcano.si.edu}'+UID_FIELD]
        # if the id doesn't already exist in Carto table or 
        # isn't added to the list for sending to Carto yet
        if str(uid) not in existing_ids + new_ids:
            # append the id to the list for sending to Carto 
            new_ids.append(uid)
            # create an empty list to store data from this row
            row = []
            # go through each column from the source data that we want to include in the Carto table
            for key in CARTO_SCHEMA_UPPER.keys():
                # generate string to retrieve for values each column from data
                source_key = '{volcano.si.edu}'+key
                try:
                    # if we are fetching data for geometry column, we have to construct the geojson from the source coordinates
                    if key == 'the_geom':
                        # generate the source column name where we can find the geometry
                        source_key = '{volcano.si.edu}GeoLocation'
                        # get the coordinates and split it to separate out latitude and longitude
                        coords=data[source_key]['{http://www.opengis.net/gml}Point']['{http://www.opengis.net/gml}coordinates'].split(',')
                        # get the longitude from first index of coords
                        lon = coords[0]
                        # get the latitude from second index of coords
                        lat = coords[1]
                        # create a geojson from the coordinates
                        item = {
                            'type': 'Point',
                            'coordinates': [lon, lat]
                        }
                    else:
                        # for all other columns, we can fetch the data using our column name in Carto
                        item = data[source_key]
                except KeyError:
                    # if the column we are trying to retrieve doesn't exist in the source data, store None
                    item=None
                # add the retrieved value for this column to the list of data for this row
                row.append(item)
            # add the list of values from this row to the list of new data
            new_data.append(row)
    # find the length (number of rows) of new_data        
    num_new = len(new_ids)
    # if we have found new dates to process
    if num_new:
        # insert new data into the carto table
        logging.info('Adding {} new records'.format(num_new))
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), new_data, user=CARTO_USER, key=CARTO_KEY)
    
    return(num_new)

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

def get_date_1y(title, new_date):
    '''
    Get current date from layer title and construct new date from most recent date
    INPUT   title: current layer titile (string)
            new_date: latest date of data to be shown in this layer (datetime)
    RETURN  old_date_text: current date being used in the title (string)
            new_date_text: new date to be show in the title (string)
    '''
    # get current end date being used from title by string manupulation
    old_date_text = title.split(' Volcano')[0]
    # get text for new date
    new_date_end = datetime.datetime.strftime(new_date, "%B %d, %Y")
    # get most recent starting date, 1 year ago
    new_date_start = (new_date - datetime.timedelta(days=365))
    new_date_start = datetime.datetime.strftime(new_date_start, "%B %d, %Y")
    # construct new date range by joining new start date and new end date
    new_date_text = new_date_start + ' - ' + new_date_end

    return old_date_text, new_date_text

def get_date(title, new_date):
    '''
    Get current date from layer title and construct new date from most recent date
    INPUT   title: current layer titile (string)
            new_date: latest date of data to be shown in this layer (datetime)
    RETURN  old_date_text: current date being used in the title (string)
            new_date_text: new date to be show in the title (string)
    '''
    # get current end date being used from title by string manupulation
    old_date_text = title.split(' Volcano')[0].split(' - ')[1]
    # get text for new date
    new_date_text = datetime.datetime.strftime(new_date, "%B %d, %Y")

    return old_date_text, new_date_text

def update_layer(layer, new_date):
    '''
    Update layers in Resource Watch back office.
    INPUT   layer: layer that will be updated (string)
            new_date: date of latest data to be shown in this layer (datetime)
    '''
    # get current layer titile
    cur_title = layer['attributes']['name']
    
    # get layer description
    lyr_dscrptn = layer['attributes']['description']
    
    # if we are processing the layer that shows volcano eruptions for past year
    if lyr_dscrptn.endswith('past year.'):
        old_date_text, new_date_text = get_date_1y(cur_title, new_date)
    # if we are processing the other layers
    else:
        old_date_text, new_date_text = get_date(cur_title, new_date)

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
        # Update the dates on layer legends
        logging.info('Updating {}'.format(CARTO_TABLE))
        # pull dictionary of current layers from API
        layer_dict = pull_layers_from_API(DATASET_ID)
        # go through each layer, pull the definition and update
        for layer in layer_dict:
            # replace layer title with new dates
            update_layer(layer, most_recent_date)
        lastUpdateDate(DATASET_ID, most_recent_date)

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # clear the table before starting, if specified
    if CLEAR_TABLE_FIRST:
        logging.info("clearing table")
        # if the table exists
        if cartosql.tableExists(CARTO_TABLE, user=CARTO_USER, key=CARTO_KEY):
            # delete all the rows
            cartosql.deleteRows(CARTO_TABLE, 'cartodb_id IS NOT NULL', user=CARTO_USER, key=CARTO_KEY)
            # note: we do not delete the entire table because this will cause the dataset visualization on Resource Watch
            # to disappear until we log into Carto and open the table again. If we simply delete all the rows, this
            # problem does not occur

    # Check if table exists, create it if it does not
    logging.info('Checking if table exists and getting existing IDs.')
    existing_ids = checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD.lower())

    # Fetch, process, and upload new data
    logging.info('Fetching new data')
    num_new = processData(SOURCE_URL, existing_ids)
    logging.info('Previous rows: {},  New rows: {}'.format(len(existing_ids), num_new))

    # Delete data to get back to MAX_ROWS
    logging.info('Deleting excess rows')
    num_dropped = deleteExcessRows(CARTO_TABLE, MAX_ROWS, AGE_FIELD)

    # Update Resource Watch
    updateResourceWatch(num_new)

    logging.info("SUCCESS")
