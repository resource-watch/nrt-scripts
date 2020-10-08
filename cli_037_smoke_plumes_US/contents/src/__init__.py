from __future__ import unicode_literals

import fiona
import os
import logging
import sys
import urllib
import datetime
from collections import OrderedDict
import cartosql
import zipfile
import requests
import json

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = False

# name of data directory in Docker container
DATA_DIR = 'data'

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'cli_037_smoke_plumes'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA = OrderedDict([
    ('the_geom', 'geometry'),
    ('_UID', 'text'),
    ('date', 'timestamp'),
    ('Satellite', 'text'),
    ('_start', 'timestamp'),
    ('_end', 'timestamp'),
    ('duration', 'text'),
    ('Density', 'numeric')
])

# column of table that can be used as a unique ID 
UID_FIELD = '_UID'

# column that stores datetime information
TIME_FIELD = 'date'

# how many rows can be stored in the Carto table before the oldest ones are deleted?
MAXROWS = 100000

# url for latest Hazard Mapping System (HMS) data
SOURCE_URL = 'http://satepsanone.nesdis.noaa.gov/pub/FIRE/HMS/GIS/hms_smoke{date}.zip'

# url for archive Hazard Mapping System (HMS) data
SOURCE_URL_ARCHIVE = 'http://satepsanone.nesdis.noaa.gov/pub/FIRE/HMS/GIS/ARCHIVE/hms_smoke{date}.zip'

# file name to save data retrieved from source url
FILENAME = 'hms_smoke{date}'

# format of dates in Carto table
DATE_FORMAT = '%Y%m%d'

# format of dates in source shapefiles
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

# maximum days to go back while searching for new dates
MAXAGE_UPLOAD = datetime.datetime.today() - datetime.timedelta(days=360)

# use SOURCE_URL_ARCHIVE to access data if date is older than MAX_CHECK_CURRENT
# else use SOURCE_URL 
MAX_CHECK_CURRENT = datetime.datetime.today() - datetime.timedelta(days=7)

# oldest date that can be stored in the Carto table before we start deleting
MAXAGE = datetime.datetime.today() - datetime.timedelta(days=365*10)

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = 'c667617a-44e8-4181-b96d-f99bbe73c331'

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

# Generate UID
def genUID(date, pos_in_shp):
    '''
    Generate unique id using date and feature index in retrieved GeoJSON
    INPUT   date: date for which we want to generate id (string)
            pos_in_shp: index of the feature in GeoJSON (integer)
    RETURN  unique id of the feature in GeoJSON (string)
    '''
    return str('{}_{}'.format(date, pos_in_shp))

def getDate(uid):
    '''
    Split uid variable using '_' to get the first eight elements which represent the date 
    INPUT   uid: unique ID used in Carto table (string)
    RETURN  date in the format YYYYMMDD (string)
    '''
    return uid.split('_')[0]

def formatObservationDatetime(start, end, datetime_format=DATETIME_FORMAT):
    '''
    Reformat the start and end date according to DATETIME_FORMAT
    INPUT   start: start date of smoke plume observation (string)
            end: end date of smoke plume observation (string)
            datetime_format: format in which this function will return the datestrings (string)
    RETURN  start: start date of the observation, in the format specified by datetime_format (string)
                   end: end date of the observation, in the format specified by datetime_format (string)
                   duration: duration of the observation, in the format HH:MM:SS (string)
    '''
    # split the start date to separate out date and time
    date, time = start.split(' ')
    # get year of start date from first four characters of the date
    year = int(date[:4])
    # get fourth and the following characters from the date and subtract 1 to get day
    # 1 is subtracted because we will add the day number to January 1 to get the date. The source starts with January 1 having a day number of 1, so we would want to add 0 to January 1 to get the correct date.
    day = int(date[4:])-1 
    # get hour from the last two characters of the time string
    hour = int(time[:-2])
    # get minute from the time string (up until the last two characters)
    minute = int(time[-2:])
    # create a datetime object for the 1st of January of the year
    # generate a complete datetime object to include month, day and time
    start_dt = datetime.datetime(year=year,month=1,day=1) + datetime.timedelta(days=day, hours=hour, minutes=minute)
    # Use similar approach as above to reformat the end date
    date, time = end.split(' ')
    year = int(date[:4])
    day = int(date[4:])-1 
    hour = int(time[:-2])
    minute = int(time[-2:])
    end_dt = datetime.datetime(year=year,month=1,day=1) + datetime.timedelta(days=day, hours=hour, minutes=minute)
    # convert datetime object to string formatted according to datetime_format
    start = start_dt.strftime(datetime_format)
    # convert datetime object to string formatted according to datetime_format
    end = end_dt.strftime(datetime_format)
    # get duration of the event, in the format HH:MM:SS, by subtracting start date from end date
    duration = str((end_dt - start_dt))

    return(start,end,duration)

def findShp(zfile):
    '''
    Check if the zipfile contain the shapefile and return the shapefile name
    INPUT  zfile: zipfile containing retrieved data from source url (string)
    RETURN  f: filename for the shapefile in zipfile (string)
    '''
    # loop through all files in the zipped file
    with zipfile.ZipFile(zfile) as z:
        for f in z.namelist():
            # check if the file is a shapefile
            if os.path.splitext(f)[1] == '.shp':
                # return the shapefile name
                return f
    return False

def getNewDates(exclude_dates):
    '''
    Get new dates that we want to try to fetch data for
    INPUT  exclude_dates: list of dates that we already have in our Carto table (list of strings)
    RETURN  new_dates: new dates that we want to try to fetch data for, in the format of the DATE_FORMAT variable (list of strings)
    '''
    # create an empty list to store new dates to upload
    new_dates = []
    # create a datetime object with today's date
    date = datetime.datetime.today()
    # continue until the date is the same as the one set by the MAXAGE_UPLOAD variable
    while date > MAXAGE_UPLOAD:
        # go back 1 day at a time
        date -= datetime.timedelta(days=1)
        # convert datetime object to string formatted according to DATE_FORMAT
        datestr = date.strftime(DATE_FORMAT)
        logging.debug(datestr)
        # if the date doesn't exist in Carto
        if datestr not in exclude_dates:
            # append the date to list of new dates to try to fetch
            new_dates.append(datestr)
        else:
            logging.debug(datestr + "already in table")

    return new_dates

def processNewData(existing_ids):
    '''
    Fetch, process and upload new data
    INPUT  existing_ids: list of unique IDs that we already have in our Carto table (list of strings)
    RETURN  num_new: number of rows of new data sent to Carto table (integer)
    '''
    # create an empty list to store unique ids of new data we will be sending to Carto table
    new_ids = []

    # get dates that already exist in the Carto table
    dates = [getDate(uid) for uid in existing_ids]
    # get new dates to try to fetch
    new_dates = getNewDates(dates)
    # loop through each new dates to fetch
    for date in new_dates:
        # create a filename for the current date's data
        tmpfile = '{}.zip'.format(os.path.join(DATA_DIR,
                                               FILENAME.format(date=date)))
        logging.info('Fetching {}'.format(date))

        try:
            # First try the archive url
            # generate url using date
            url = SOURCE_URL_ARCHIVE.format(date=date)
            # pull data from url and save to tmpfile
            urllib.request.urlretrieve(url, tmpfile)
        except urllib.error.HTTPError as e:
            # if the data doesn't exist in archive url and is less than 1 week old
            # try current folder
            if datetime.datetime.strptime(date, DATE_FORMAT) > MAX_CHECK_CURRENT:
                try:
                    # generate url using date
                    url = SOURCE_URL.format(date=date)
                    # pull data from url and save to tmpfile
                    urllib.request.urlretrieve(url, tmpfile)
                except urllib.error.HTTPError as e:
                    logging.warning('Could not retrieve files for {}'.format(date))
                    continue
            else:
                logging.warning('Could not retrieve files for {}'.format(date))
                continue

        # Parse fetched data and generate unique ids
        logging.info('Parsing data')
        # find the shapefile from retrieved tmpfile
        # format path for the shapefile
        shpfile = '/{}'.format(findShp(tmpfile))
        # format path for the zipfile
        zfile = 'zip://{}'.format(tmpfile)
        # create an empty list to store each row of new data
        rows = []
        # open the shapefile as GeoJSON and process it
        with fiona.open(shpfile, 'r', vfs=zfile) as shp:
            logging.debug(shp.schema)
            # set the index for the feature position
            pos_in_shp = 0
            # loop through each features in the GeoJSON
            for obs in shp:
                # get the start date from the 'Start' variable in the 'properties feature'
                start = obs['properties']['Start']
                # get the end date from the 'End' variable in the 'properties feature'
                end = obs['properties']['End']
                # reformat the start and end date 
                # get the duration of smoke plume event
                start, end, duration = formatObservationDatetime(start, end)
                # create new variables in the 'properties' feature for storing start date, 
                # end date and duration of the events
                obs['properties']['_start'] = start
                obs['properties']['_end'] = end
                obs['properties']['duration'] = duration
                # generate unique id by using date and index of the feature
                uid = genUID(date, pos_in_shp)
                # append the unique id to the list for sending to Carto 
                new_ids.append(uid)
                # create an empty list to store data from this row
                row = []
                # go through each column in the Carto table
                for field in CARTO_SCHEMA.keys():
                    # if we are fetching data for geometry column
                    if field == 'the_geom':
                        # get geometry from the 'geometry' feature of the GeoJSON
                        # add geojson geometry to the list of data from this row
                        row.append(obs['geometry'])
                    # if we are fetching data for unique id
                    elif field == UID_FIELD:
                        # add the unique id to the list of data from this row
                        row.append(uid)
                    # if we are fetching data for datetime column
                    elif field == TIME_FIELD:
                        # add the datetime information to the list of data from this row
                        row.append(date)
                    else:
                        # add data for remaining fields to the list of data from this row
                        row.append(obs['properties'][field])
                # add the list of values from this row to the list of new data
                rows.append(row)
                # update the index before we move on to the next feature
                pos_in_shp += 1
        # Delete local file
        os.remove(tmpfile)

        # find the length (number of rows) of new_data
        new_count = len(rows)
        # check if new data is available
        if new_count:
            logging.info('Pushing {} new rows'.format(new_count))
            # insert new data into the carto table
            cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                                CARTO_SCHEMA.values(), rows, user=CARTO_USER, key=CARTO_KEY)
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

def get_date_7d(title, new_date):
    '''
    Get current date from layer title and construct new date from most recent date
    INPUT   title: current layer titile (string)
            new_date: latest date of data to be shown in this layer (datetime)
    RETURN  old_date_text: current date being used in the title (string)
            new_date_text: new date to be show in the title (string)
    '''
    # get current end date being used from title by string manupulation
    old_date_text = title.split(' Smoke')[0]
    # latest data is for one day ago, so subtracting a day
    new_date_end = (new_date - datetime.timedelta(days=1))
    # get text for new date
    new_date_end = datetime.datetime.strftime(new_date_end, "%B %d, %Y")
    # get most recent starting date, 8 day ago
    new_date_start = (new_date - datetime.timedelta(days=7))
    new_date_start = datetime.datetime.strftime(new_date_start, "%B %d, %Y")
    # construct new date range by joining new start date and new end date
    new_date_text = new_date_start + ' - ' + new_date_end

    return old_date_text, new_date_text

def get_date_1d(title, new_date):
    '''
    Get current date from layer title and construct new date from most recent date
    INPUT   title: current layer titile (string)
            new_date: latest date of data to be shown in this layer (datetime)
    RETURN  old_date_text: current date being used in the title (string)
            new_date_text: new date to be show in the title (string)
    '''
    # get current end date being used from title by string manupulation
    old_date_text = title.split(' Smoke')[0]
    # latest data is for one day ago, so subtracting a day
    new_date = (new_date - datetime.timedelta(days=1))
    # get text for new date
    new_date_text = datetime.datetime.strftime(new_date, "%B %d, %Y")

    return old_date_text, new_date_text

def update_layer(layer):
    '''
    Update layers in Resource Watch back office.
    INPUT   layer: layer that will be updated (string)
    '''
    # get current layer titile
    cur_title = layer['attributes']['name']
    
    # get layer description
    lyr_dscrptn = layer['attributes']['description']
    
    # get new date end which will be the current date
    current_date = datetime.datetime.now()  
    
    # if we are processing the layer that shows smoke plumes for laterst 7 days
    if lyr_dscrptn.endswith('latest 7 days.'):
        old_date_text, new_date_text = get_date_7d(cur_title, current_date)
    # if we are processing the layer that shows smoke plumes for previous day
    else:
        old_date_text, new_date_text = get_date_1d(cur_title, current_date)

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
    num_new = processNewData(existing_ids)
    
    logging.info('Previous rows: {},  New rows: {}, Max: {}'.format(len(existing_ids), num_new, MAXROWS))

    # Remove old observations
    deleteExcessRows(CARTO_TABLE, MAXROWS, TIME_FIELD, MAXAGE)

    # Update Resource Watch
    updateResourceWatch(num_new)

    logging.info('SUCCESS')
