import logging
import sys
import os
import requests
from collections import OrderedDict
import cartosql
import datetime
import json

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = False

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'dis_001_significant_earthquakes'

# column of table that can be used as a unique ID (UID)
UID_FIELD = 'uid'

# column that stores datetime information
TIME_FIELD = 'datetime'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA = OrderedDict([
    ('uid', 'text'),
    ('the_geom', 'geometry'),
    ('depth_in_km', 'numeric'),
    ('datetime', 'timestamp'),
    ('mag', 'numeric'),
    ('place', 'text'),
    ('sig', 'numeric'),
    ('magType', 'text'),
    ('nst', 'numeric'),
    ('dmin', 'numeric'),
    ('rms', 'numeric'),
    ('gap', 'numeric'),
    ('tsunami', 'numeric'),
    ('felt', 'numeric'),
    ('cdi', 'numeric'),
    ('mmi', 'numeric'),
    ('net', 'text'),
    ('alert', 'text')
])

# how many rows can be stored in the Carto table before the oldest ones are deleted?
MAX_ROWS = 500000

# oldest date that can be stored in the Carto table before we start deleting
MAX_AGE = datetime.datetime.today() - datetime.timedelta(days=365*2)

# url for recent earthquake data
SOURCE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={startTime}&endtime={endTime}&minsig={minSig}"

# Do you want to process data back through the MAX_AGE?
# If false, data will be processed back until we pull a full week of data in which all data is already in the Carto table
PROCESS_HISTORY = False

# format of dates in Carto table
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

# specify the minimum significance the earthquake must have to be included in our dataset
# set it to 0 to include all earthquake events
SIGNIFICANT_THRESHOLD = 0

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = '1d7085f7-11c7-4eaf-a29a-5a4de57d010e'

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

def deleteExcessRows(table, max_rows, time_field, max_age=''):
    ''' 
    Delete rows that are older than a certain threshold and also bring count down to max_rows
    INPUT   table: name of table in Carto from which we will delete excess rows (string)
            max_rows: maximum rows that can be stored in the Carto table (integer)
            time_field: column that stores datetime information (string) 
            max_age: oldest date that can be stored in the Carto table (datetime object)
    RETURN  num_dropped: number of rows that have been dropped from the table (integer)
    ''' 
    # initialize number of rows that will be dropped as 0
    num_dropped = 0

    # check if max_age is a datetime object
    if isinstance(max_age, datetime.datetime):
        # convert max_age to a string in the format (YYYY-MM-DD)
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

    return(num_dropped)

def genUID(lat, lon, depth, dt):
    '''Generate unique id using latitude, longitude, depth and date information
    INPUT   lat: latitude of the earthquake (float)
            lon: longitude of the earthquake (float)
            depth: depth of the earthquake (float)
            dt: date for which we want to generate id (string)
    RETURN unique id for row (string)
    '''
    return '{}_{}_{}_{}'.format(lat, lon, depth, dt)

def processData(url, existing_ids):
    '''
    Fetch, process and upload new data
    INPUT   url: unformatted url where you can find the source data (string)
            existing_ids: list of date IDs that we already have in our Carto table (list of strings)
    RETURN  num_new: number of rows of new data sent to Carto table (integer)
    '''
    # create a variable to store the number of new rows we have added to Carto
    num_new = 0
    # create an empty list to store unique ids of new data we will be sending to Carto table
    new_ids = []
    # create a datetime object with today's date
    startTime = datetime.datetime.today()

    # Retrieve and process data by iterating backwards 1-week at a time
    # continue until the current datetime is older than the oldest date allowed in the table, set by the MAX_AGE variable
    while startTime > MAX_AGE:
        # set the end date for collecting data
        endTime = startTime
        # set the start date for collecting data to 7 days before the end date
        startTime = startTime - datetime.timedelta(days=7)
        logging.info('Fetching data between {} and {}'.format(startTime, endTime))
        # generate the url and pull data for the selected interval
        res = requests.get(url.format(startTime=startTime, endTime=endTime, minSig=SIGNIFICANT_THRESHOLD))
        if not res.ok:
            logging.error(res.text)
        # pull data from request response json
        data = res.json()
        
        # create an empty list to store new data for this week
        new_data = []
        # loop until no new observations
        for feature in data['features']:
            # get the coordinates of the earthquake from geometry
            coords = feature['geometry']['coordinates']
            # get the latitude, longitude and depth from the coordinates
            lat = coords[1]
            lon = coords[0]
            depth = coords[2]
            # get properties of the earthquake
            props = feature['properties']
            # get date from properties feature
            # convert datetime object to string formatted according to DATETIME_FORMAT
            dt = datetime.datetime.utcfromtimestamp(props['time'] / 1000).strftime(DATETIME_FORMAT)
            # generate unique id by using the coordinates, depth, and datetime of the earthquake
            _uid = genUID(lat, lon, depth, dt)
            # if the id doesn't already exist in Carto table or 
            # isn't added to the list for sending to Carto yet            
            if _uid not in existing_ids and _uid not in new_ids:
                # append the id to the list for sending to Carto 
                new_ids.append(_uid)
                # create an empty list to store data from this row
                row = []
                # go through each column in the Carto table
                for field in CARTO_SCHEMA:
                    # if we are fetching data for unique id column
                    if field == UID_FIELD:
                        # add the unique id to the list of data from this row
                        row.append(_uid)
                    # if we are fetching data for geometry column
                    elif field == 'the_geom':
                        geom = {
                            'type': 'Point',
                            'coordinates': [lon, lat]
                        }
                        # add a geojson of the coordinates to the list of data from this row
                        row.append(geom)
                    # if we are fetching data for depth_in_km column
                    elif field == 'depth_in_km':
                        # add the depth information to the list of data from this row
                        row.append(depth)
                    # if we are fetching data for datetime column
                    elif field == 'datetime':
                         # add datetime information to the list of data from this row
                        row.append(dt)
                    else:
                        # for all other columns, we don't have to construct the pointer
                        # we can fetch the data using our column name in Carto
                        row.append(props[field])
                # add the list of values from this row to the list of new data
                new_data.append(row)
        # add the number of rows being added for this week to the current total of new rows added
        num_new += len(new_data)
        # if we have found new dates to process
        if len(new_data):
            # insert new data into the carto table
            logging.info('Adding {} new records'.format(num_new))
            cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                                     CARTO_SCHEMA.values(), new_data, user=CARTO_USER, key=CARTO_KEY)
        # if we are not processing the data back through the MAX_AGE
        elif not PROCESS_HISTORY:
            # break once we pull a week of data in which all data is already on Carto
            # otherwise, keep going until we reach the MAX_AGE
            break

    return(num_new)

def get_most_recent_date(table):
    '''
    Find the most recent date of data in the specified Carto table
    INPUT   table: name of table in Carto we want to find the most recent date for (string)
    RETURN  most_recent_date: most recent date of data in the Carto table, found in the TIME_FIELD column of the table (datetime object)
    '''
    # get dates in TIME_FIELD column
    r = cartosql.getFields(TIME_FIELD, table, f='csv', post=True, user=CARTO_USER, key=CARTO_KEY)
    # turn the response into a list of dates
    dates = r.text.split('\r\n')[1:-1]
    # sort the dates from oldest to newest
    dates.sort()
    # turn the last (newest) date into a datetime object
    most_recent_date = datetime.datetime.strptime(dates[-1], '%Y-%m-%d %H:%M:%S')
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
    # convert response into json and make dictionary of layers
    layer_dict = json.loads(r.content.decode('utf-8'))['data']
    return layer_dict

def get_date_30d(title, new_date):
    '''
    Get current date from layer title and construct new date from most recent date
    INPUT   title: current layer titile (string)
            new_date: latest date of data to be shown in this layer (datetime)
    RETURN  old_date_text: current date being used in the title (string)
            new_date_text: new date to be show in the title (string)
    '''
    # get current end date being used from title by string manupulation
    old_date = title.split()[0:7]
    # join each time variable to construct text of current date
    old_date_text = ' '.join(old_date)
    # get text for new date
    new_date_end = datetime.datetime.strftime(new_date, "%B %d, %Y")
    # get most recent starting date, 30 day ago
    new_date_start = (new_date - datetime.timedelta(days=29))
    new_date_start = datetime.datetime.strftime(new_date_start, "%B %d, %Y")
    # construct new date range by joining new start date and new end date
    new_date_text = new_date_start + ' - ' + new_date_end

    return old_date_text, new_date_text

def get_date_7d(title, new_date):
    '''
    Get current date from layer title and construct new date from most recent date
    INPUT   title: current layer titile (string)
            new_date: latest date of data to be shown in this layer (datetime)
    RETURN  old_date_text: current date being used in the title (string)
            new_date_text: new date to be show in the title (string)
    '''
    # get current end date being used from title by string manupulation
    old_date_text = title.split(' All')[0]
    # get text for new date
    new_date_end = datetime.datetime.strftime(new_date, "%B %d, %Y")
    # get most recent starting date, 7 day ago
    new_date_start = (new_date - datetime.timedelta(days=6))
    new_date_start = datetime.datetime.strftime(new_date_start, "%B %d, %Y")
    # construct new date range by joining new start date and new end date
    new_date_text = new_date_start + ' - ' + new_date_end

    return old_date_text, new_date_text

def update_layer(layer):
    '''
    Update layers in Resource Watch back office.
    INPUT   layer: layer that will be updated (string)
    '''
    # get current layer titile
    cur_title = layer['attributes']['name']
    
    # get new date end which will be the current date
    current_date = datetime.datetime.now()    
    
    # if we are processing the layer that shows earthquake for latest 7 days
    if cur_title.endswith('All Earthquakes (Magnitude)'):
        old_date_text, new_date_text = get_date_7d(cur_title, current_date)
    # if we are processing the layer that shows earthquake for latest 30 days
    else:
        old_date_text, new_date_text = get_date_30d(cur_title, current_date)

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
        most_recent_date = get_most_recent_date(CARTO_TABLE)
        # Update the dates on layer legends
        logging.info('Updating {}'.format(CARTO_TABLE))
        # pull dictionary of current layers from API
        layer_dict = pull_layers_from_API(DATASET_ID)
        # go through each layer, pull the definition and update
        for layer in layer_dict:
            # replace layer title with new dates
            update_layer(layer)
            
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
    existing_ids = checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)

    # Fetch, process, and upload new data
    logging.info('Fetching new data')
    num_new = processData(SOURCE_URL, existing_ids)
    logging.info('Previous rows: {},  New rows: {}'.format(len(existing_ids), num_new))

    # Delete data to get back to MAX_ROWS
    num_deleted = deleteExcessRows(CARTO_TABLE, MAX_ROWS, TIME_FIELD, MAX_AGE)

    # Update Resource Watch
    updateResourceWatch(num_new)

    logging.info("SUCCESS")
