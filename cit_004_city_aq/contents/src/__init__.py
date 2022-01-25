import os
import logging
import sys
from collections import OrderedDict
import datetime
import cartosql
import requests
import json
import time
import pandas as pd
import cartoframes
from shapely.geometry import mapping

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = False

# name of data directory in Docker container
DATA_DIR = 'data'

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')
# set up cartoframes auth
AUTH=cartoframes.auth.Credentials(username=CARTO_USER, api_key=CARTO_KEY)

# name of table in Carto where we will upload the data
CARTO_TABLE = 'cit_004_city_aq'
# name of table in Carto where we will upload the station data
STATION_CARTO_TABLE = 'cit_004_city_aq_stations'
# name of table in Carto where we will upload the calculated metrics
METRICS_CARTO_TABLE = 'cit_004_city_aq_metrics'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA = OrderedDict([
    ("the_geom", "geometry"),
    ("uid", "text"),
    ("name", "text"),
    ("created", "timestamp"),
    ("date", "timestamp"),
    ("o3_ugm3", "numeric"),
    ("no2_ugm3", "numeric"),
    ("pm25_gcc_ugm3", "numeric"),
    ("pm25_gocart_ugm3", "numeric"),
    ("o3_ppm", "numeric"),
    ("no2_ppm", "numeric"),
    ("o3_ppb", "numeric"),
    ("no2_ppb", "numeric"),
])

# column names and types for table of stations
STATION_CARTO_SCHEMA = OrderedDict([
    ("the_geom", "geometry"),
    ("id", "numeric"),
    ("name", "text")
])

# column names and types for table of calculated metrics
METRICS_CARTO_SCHEMA = OrderedDict([('the_geom', 'geometry'),
            ('name', 'text'),
            ('created', 'timestamp'),
            ('date', 'timestamp'),
            ('no2_ppb_max1houravg', 'numeric'),
            ('no2_ppm_max1houravg', 'numeric'),
            ('no2_ugm3_max1houravg', 'numeric'),
            ('pm25_gcc_ugm3_24houravg', 'numeric'),
            ('pm25_gocart_ugm3_24houravg', 'numeric'),
            ('o3_ppb_max8houravg', 'numeric'),
            ('o3_ppm_max8houravg', 'numeric'),
            ('o3_ugm3_max8houravg', 'numeric'),
            ('uid', 'text')])

# column of table that can be used as a unique ID (UID)
UID_FIELD = 'uid'

# column of stations table that can be used as a unique ID (UID)
STATION_UID_FIELD = 'id'

# column that stores datetime information
TIME_FIELD = 'created'

# format of dates in source and Carto table
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

# how many rows can be stored in the Carto table before the oldest ones are deleted?
MAXROWS = 500000

# url for devseed air quality data
SOURCE_URL = 'http://gmao-aq-prod-1707436367.us-east-1.elb.amazonaws.com/api/forecast/'

# url to get info about specific station
STATION_URL = 'http://gmao-aq-prod-1707436367.us-east-1.elb.amazonaws.com/api/station/{station}/'

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change these IDs OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_IDS = {
    'PM2.5':'f5599d62-7f3d-41c7-b3fd-9f8e08ee7b2a',
    'NO2': '8ee6a93b-ab7a-41a8-95f5-3d684626d9ea',
    'O3': '403726e5-6852-49f4-9dce-b82b599b09fc'
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

def genUID(created, dt, stn):
    '''Generate unique id using date and station number
    INPUT   created: date when forecast was generated (string)
            dt: date for current data row (string)
            stn: station number (integer)
    RETURN unique id for row (string)
    '''
    # split the date on '-' to create separate pieces
    created_pieces = created.split('-')
    # join the pieces to have date with format YYMMDDHH(2020-07-04T12:00:00.000Z > 20200704T12)
    mod_created = created_pieces[0] + created_pieces[1] + created_pieces[2].split(':')[0]
    # split the date on '-' to create separate pieces
    dt_pieces = dt.split('-')
    # join the pieces to have date with format YYMMDDHH(2020-07-04T12:00:00.000Z > 20200704T12)
    mod_dt = dt_pieces[0] + dt_pieces[1] + dt_pieces[2].split(':')[0]
    # joint the formatted date with station number to generate the unique id
    return '{}_{}_{}'.format(mod_created, mod_dt, stn)

def getForecastCreationDT(existing_ids, old_or_new):
    '''
    get the oldest forecast start date from the list of existing IDs in the Carto table
    INPUT   existing_ids: list of unique IDs that we already have in our Carto table (list of strings)
            old_or_new: do you want the 'oldest' or 'newest' forecast creation date (string)
    RETURN  oldest_forecast_dt: date of oldest forecast creation date in Carto table (datetime)
    '''
    # sort list of existing IDs so that are in order of oldest forecast to newest
    existing_ids.sort()
    if old_or_new=='oldest':
        # get the first (oldest) forecast ID
        carto_id = existing_ids[0]  
    if old_or_new=='newest':
        # get the last (newest) forecast ID
        carto_id = existing_ids[-1]  
    # get the string of the forecast creation date
    forecast_creation = carto_id.split('_')[0]
    # convert forecast date string to datetime
    forecast_creation_dt = datetime.datetime.strptime(forecast_creation, '%Y%m%dT%H')
    return forecast_creation_dt

def getConversion_ugm3_ppb(gas):
    '''
    Get conversion factor for an input gas - 1 ppb = _____ µg/m3
    INPUT   gas: gas we need conversion factor for (string)
    RETURN  s: µg/m3 that 1 ppb of this gas is equal to (float)
    '''
    if gas =='no2':
        s = 1.88
    elif gas =='o3':
        s = 1.96
    return s

def processStnData(stn_data):
    '''
    process new station data and send to Carto table
    INPUT   stn_data: station data from API (json)
    '''
    # create an empty list to store data from this row
    row = []
    # go through each column in the Carto table
    for field in STATION_CARTO_SCHEMA.keys():
        # if we are fetching data for geometry column
        if field == 'the_geom':
            # get geojson geometry
            data = stn_data.get("geometry")
            # add geojson geometry to the list of data from this row
            row.append(data)
        elif field=='id':
            # get id
            data = stn_data.get("id")
            # add to the list of data from this row
            row.append(data)
        elif field=='name':
            # get station name
            name = stn_data.get("properties")['name']
            # add to the list of data from this row
            row.append(name)
    cartosql.insertRows(STATION_CARTO_TABLE, STATION_CARTO_SCHEMA.keys(), STATION_CARTO_SCHEMA.values(), [row], user=CARTO_USER, key=CARTO_KEY)


def processNewData(src_url, existing_ids, existing_stations):
    '''
    Fetch, process and upload new data
    Due to the size of requested data, the API response is being processed in batches
    that are later written to a temporary json file
    INPUT   src_url: url where you can find the source data (string)
            existing_ids: list of unique IDs that we already have in our Carto table (list of strings)
            existing_stations: list of station IDs that we already have in our Carto table (list of strings)
    RETURN  new_ids: list of unique ids of new data sent to Carto table (list of strings)
    '''
    n_tries = 3
    fetch_exception = None
    # Using indx to iterate move over the number of json files
    indx = 0
    # Limit for each of the json files 
    file_size = 52428800
    
    for i in range(0, n_tries):
        try: 
            # Create path to json file where we're writing the API response
            raw_data_file = os.path.join(DATA_DIR, 'data_{}.json'.format(indx))
            logging.info('Requesting data from API')
            # Read  and write request in chunks to avoid memory crash 
            with requests.get(SOURCE_URL, stream=True) as r:
                 with open(raw_data_file, 'wb') as f_out:
                     for chunk in r.iter_content(chunk_size=1024 * 1024):
                         f_out.write(chunk)
                         # Check if json file is larger than limit, if so create a new one                
                         if int(os.path.getsize(raw_data_file)) > file_size:
                             f_out.close()
                             indx += 1
                             raw_data_file = os.path.join(DATA_DIR, 'data_{}.json'.format(indx))
                             f_out = open(raw_data_file, "wb")
                             logging.info('Moving to next batch!')
        except Exception as e:
            fetch_exception = e
            logging.info('Uh-oh. Attempt number {} failed, trying again'.format(i))
            # In case API request failed reset indx value
            indx = 0
            # Erase incomplete file before trying to fetch data again
            delete_local()
            time.sleep(300)
        
        else:
            break
            f_out.close()
    else:
        logging.info('Failed to fetch data.')
        raise fetch_exception
   
    
    # create an empty list to store unique ids of new data we will be sending to Carto table
    logging.info('Creating list to store unique ids')
    new_ids = []
    # create an empty list to store each row of new data
    logging.info('Creating list to store new rows')
    new_rows = []
    # List containing the json files where the API response will be stored
    logging.info('Creating list with json file paths')
    raw_files = os.listdir(DATA_DIR)
    # get most updated list of station information
    logging.info('Getting station table')
    station_df = cartoframes.read_carto('cit_004_city_aq_stations', credentials=AUTH)
    # Iterating over the json files
    for index_num,element in enumerate(raw_files):
        with open(os.path.join(DATA_DIR,raw_files[index_num])) as f:
            # Reading json file contents as a list of strings
            json_data = f.read()
        # Spliting elements in list as different lines
        all_json = json_data.split('\n')
        # Removing unnecesary commas at the end of strings
        all_json = [x[:-1] for x in all_json]
        # Removing corrupted first line
        all_json = all_json[1:]
        # Removing corrupted last line 
        all_json = all_json[:-1]
        # Sending clean data to a json object
        logging.info('Pulling json data')
        data = [json.loads(i) for i in all_json if i] 
        # Order json data by date
        logging.info('Ordering json by creation date')
        data = sorted(data, key = lambda x: datetime.datetime.strptime(x["created"],"%Y-%m-%dT%H:%M:%S.%fZ"))
        logging.info('Reversing data')
        data.reverse()    

        # Start processing batch of data 
        logging.info('Processing batch of data')
        # loop until no new observations
        for obs in data:
            # get the forecast creation date
            created = obs['created']
            # if there are existing IDs in the table, make sure the obs isn't older than the oldest
            if existing_ids:
                # get the oldest forecast creation date in the current Carto table:
                oldest_forecast_dt = getForecastCreationDT(existing_ids, old_or_new='oldest')
                # if the forecast creation date of the current observation is older than 
                # the oldest in the table, skip this observation
                if datetime.datetime.strptime(created, DATETIME_FORMAT) < oldest_forecast_dt:
                    continue
            # get the date from date feature 
            dt = obs['date']
            # get the station number from 'station' feature
            stn = obs['station']
            # generate unique id by using the date and station number
            uid = genUID(created, dt, stn)
            # if the id doesn't already exist in Carto table or 
            # isn't added to the list for sending to Carto yet 
            if uid not in existing_ids + new_ids:
                # if we don't already have the station information for this station, add it to the table
                if str(stn) not in existing_stations:
                    # generate url to get details of the station being processed
                    stn_url = STATION_URL.format(station = stn)
                    tries = 0
                    while tries < 3:
                        # get data from station url
                        stn_r = requests.get(stn_url)
                        if r.ok:
                            # pull data from request response json
                            stn_data = stn_r.json()
                            # process station data and send to Carto
                            processStnData(stn_data)
                            # add station to list of existing stations
                            existing_stations.append(str(stn_data['id']))
                            # get most updated list of station information
                            station_df = cartoframes.read_carto('cit_004_city_aq_stations', credentials=AUTH)
                            break
                        else:
                            logging.error('Could not fetch station data for uid: {}, trying again'.format(uid))
                            time.sleep(100)
                            tries += 1
                    if tries==3:
                        logging.error('Could not fetch station data for uid: {}, aborting'.format(uid))
                # append the id to the list for sending to Carto 
                new_ids.append(uid)
                # create an empty list to store data from this row
                row = []
                # get station data from Carto table
                station_row = station_df[station_df['id']==str(stn)].iloc[0]
                # go through each column in the Carto table
                for field in CARTO_SCHEMA.keys():
                    # if we are fetching data for geometry column
                    if field == 'the_geom':
                        # construct geojson geometry
                        geom = mapping(station_row['the_geom'])
                        # add geojson geometry to the list of data from this row
                        row.append(geom)
                    # if we are fetching data for unique id column
                    elif field == 'uid':
                        # add already generated unique id to the list of data from this row
                        row.append(uid)
                    # if we are fetching data for station name
                    elif field == 'name':
                        # get station name from the dictionary
                        name = station_row["name"]
                        # add text to the list of data from this row
                        row.append(name)
                    # if we are fetching data for date of forecast creation column
                    elif field == 'created':
                        # turn already generated creation date into a datetime
                        created = datetime.datetime.strptime(created, DATETIME_FORMAT)
                        # add date to the list of data from this row
                        row.append(created)
                    # if we are fetching data for date column
                    elif field == 'date':
                        # turn already generated date into a datetime
                        date = datetime.datetime.strptime(dt, DATETIME_FORMAT)
                        # add date to the list of data from this row
                        row.append(date)
                    # remaining fields to process are the different air quality variables
                    else:
                        # get unit for column we are processing
                        unit = field.rsplit('_', 1)[1]
                        # get gas for column we are processing
                        gas = field.rsplit('_', 1)[0]
                        # get concentratiion
                        conc = obs['gas'].get(gas)
                        # the API returns data in units of µg/m3, so no conversion is necessary for this column
                        # add the data to the row
                        if unit == 'ugm3':
                            row.append(conc)
                        # if we are processing a ppm column, convert units from µg/m3 and add the data to the row
                        if unit == 'ppm':
                            if conc is not None:
                                row.append(conc/getConversion_ugm3_ppb(gas)/1000)
                            else:
                                row.append(None)
                        # if we are processing a ppb column, convert units from µg/m3 and add the data to the row
                        if unit == 'ppb':
                            if conc is not None:
                                row.append(conc/getConversion_ugm3_ppb(gas))
                            else:
                                row.append(None)
                # add the list of values from this row to the list of new data
                new_rows.append(row)
            # once we reach an observation we have already uploaded to Carto
            # stop processing the old data
            else:
                logging.info('Batch of data processed, moving to next batch.')
                break
    # find the length (number of rows) of new_data 
    new_count = len(new_rows)
    # Delete local files created during process
    delete_local()
    # check if new data is available
    if new_count:
        logging.info('Sending new data to Carto')
        logging.info('Pushing {} new rows'.format(new_count))
        # insert new data into the carto table
        cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(),
            new_rows, user=CARTO_USER, key=CARTO_KEY)
    return new_ids

def deleteExcessRows(table, max_rows, time_field, max_age=''):
    ''' 
    Delete rows that are older than a certain threshold and also bring count down to max_rows
    INPUT   table: name of table in Carto from which we will delete excess rows (string)
            max_rows: maximum rows that can be stored in the Carto table (integer)
            time_field: column that stores datetime information (string) 
            max_age: optional, oldest date that can be stored in the Carto table (datetime object)
    RETURN  num_dropped: number of rows that have been dropped from the table (integer)
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

    return(num_dropped)

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

def update_layer(layer, new_creation_date, new_date):
    '''
    Update layers in Resource Watch back office.
    INPUT   layer: layer that will be updated (string)
            new_creation_date: creation date of forecast (datetime)
            new_date: date of forecast to be shown in this layer (datetime)
    '''
    # get SQL from layer
    sql = layer['attributes']['layerConfig']['body']['layers'][0]['options']['sql']
    # get previous creation date being used from sql
    old_creation_date = sql.split('created')[1].split()[1][1:-1]
    # get previous date being used from sql
    old_date = sql.split('date')[2].split()[0][1:-1]

    #update sql with new dates
    sql = sql.replace(old_date, datetime.datetime.strftime(new_date, DATETIME_FORMAT))
    sql = sql.replace(old_creation_date, datetime.datetime.strftime(new_creation_date, DATETIME_FORMAT))
    
    # change to layer name text of date
    old_date_dt = datetime.datetime.strptime(old_date, DATETIME_FORMAT)
    old_date_text = datetime.datetime.strftime(old_date_dt, "%B %-d, %Y")

    # get text for new date
    new_date_text = datetime.datetime.strftime(new_date, "%B %-d, %Y")

    # replace date in layer's title with new date
    layer['attributes']['name'] = layer['attributes']['name'].replace(old_date_text, new_date_text)

    # replace the sql in the layer def with new sql
    layer['attributes']['layerConfig']['body']['layers'][0]['options']['sql'] = sql

    # send patch to API to replace layers
    # generate url to patch layer
    rw_api_url_layer = "https://api.resourcewatch.org/v1/dataset/{dataset_id}/layer/{layer_id}".format(
        dataset_id=layer['attributes']['dataset'], layer_id=layer['id'])
    # create payload with new title and layer configuration
    payload = {
        'application': ['rw'],
        'layerConfig': layer['attributes']['layerConfig'],
        'name': layer['attributes']['name'],
        'interactionConfig': layer['attributes']['interactionConfig']
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

def getLatestForecastDates(ids):
    '''
    This function will find the set of most recent dates given a list of uids for this dataset
    First date will be the forecast creation date, and the following dates are the dates
    of the forecast.
    INPUT   ids: UIDs from Carto table (list of strings)
    RETURN  new_dates: new dates associated with most recent forecast (list of datetimes)
    '''
    # get the newest forecast creation date in the current Carto table:
    newest_forecast_dt = getForecastCreationDT(ids, old_or_new='newest')
    #create a list of the dates available for this creation date
    new_dates = []
    for i in range(0,6):
        new_dates.append(newest_forecast_dt+datetime.timedelta(days=i))
    return new_dates

def processMetrics(new_ids):
    '''
    Process air quality metrics for each of the compounds
    PM2.5: daily average
    O3: maximum daily 8-hour average
    NO2: maximum daily 1-hour average

    INPUT   new_ids: UIDs for new data added to unprocessed data table
    '''
    logging.info('Processing Air Quality Metrics')
    labels = ['cartodb_id', 'the_geom', 'the_geom_webmercator', 'uid', 'name','created', 'date', 'o3_ugm3', 'no2_ugm3', 'pm25_gcc_ugm3', 'pm25_gocart_ugm3', 'o3_ppm', 'no2_ppm', 'o3_ppb', 'no2_ppb']
    # If there are new entries in the Carto table
    if len(new_ids)>0:
        # get a list of the newest dates
        new_dates = getLatestForecastDates(new_ids)
        
        # delete the old rows in the carto table
        cartosql.deleteRows(METRICS_CARTO_TABLE, 'cartodb_id IS NOT NULL', user=CARTO_USER, key=CARTO_KEY)

        for date in new_dates[1:]:
            # define a sql statement to pull in the 24 hours of data for the current date
            sql = "SELECT *,  ST_AsGeoJSON(the_geom) AS the_geom from {table} where created = date '{creation_date}' AND date >= date '{date}' - interval '12 hours' AND date < date '{date}' + interval '12 hours'".format(table=CARTO_TABLE, creation_date=new_dates[0], date=date)
            # request the data from the Carto table
            r = cartosql.get(sql, user=CARTO_USER, key=CARTO_KEY)
            # turn the response into a dataframe
            df = pd.DataFrame.from_records(r.json()['rows'], columns = labels)
            # Calculate daily metrics for each variable
            # define the groupings to use when calculating metrics
            grouping = ['name', 'the_geom', 'the_geom_webmercator']
            # Calculate NO2 metric - maximum 1-hour average (for all units)
            df['no2_ppb_max1houravg'] = df.groupby(grouping)['no2_ppb'].transform(lambda x: x.max())
            df['no2_ppm_max1houravg'] = df.groupby(grouping)['no2_ppm'].transform(lambda x: x.max())
            df['no2_ugm3_max1houravg'] = df.groupby(grouping)['no2_ugm3'].transform(lambda x: x.max())

            # Calculate PM2.5 metric - daily average
            df['pm25_gcc_ugm3_24houravg'] = df.groupby(grouping)['pm25_gcc_ugm3'].transform(lambda x: x.mean())
            df['pm25_gocart_ugm3_24houravg'] = df.groupby(grouping)['pm25_gocart_ugm3'].transform(lambda x: x.mean())

            # Calculate O3 metric - maximum daily 8-hour average (for all units)
            df['o3_ppb_max8houravg'] = df.iloc[::-1].groupby(grouping)['o3_ppb'].transform(lambda x: x.rolling(8, min_periods=8).mean().max())
            df['o3_ppm_max8houravg'] = df.iloc[::-1].groupby(grouping)['o3_ppm'].transform(lambda x: x.rolling(8, min_periods=8).mean().max())
            df['o3_ugm3_max8houravg'] = df.iloc[::-1].groupby(grouping)['o3_ugm3'].transform(lambda x: x.rolling(8, min_periods=8).mean().max())
            
            # replace date column with the general date this metric was calculated for
            # (instead of the individual mesurement times)
            df['date'] = datetime.datetime.strftime(date, DATETIME_FORMAT)
            # drop non-metric columns, as well as columns that correspond to indivdual 
            # measurements instead of the daily metric ('cartodb_id', 'uid')
            df = df.drop(['cartodb_id', 'uid', 'the_geom_webmercator', 'no2_ppb', 'no2_ppm', 'no2_ugm3', 'o3_ppb', 'o3_ppm', 'o3_ugm3', 'pm25_gcc_ugm3', 'pm25_gocart_ugm3'], axis=1)
            # drop duplicate rows to get only one summary metric for each station
            df = df.drop_duplicates()

            # generate unique id by using the date and station number
            # convert geometry column from string to json
            # replace nan with none
            if len(df)>0:
                            df['uid'] = df.apply(lambda row: genUID(row['created'], row['date'], row['name']), axis=1)
                            df['the_geom'] = df.apply(lambda row: json.loads(row['the_geom']), axis=1)
                            df = df.where(pd.notnull(df), None)
                            # upload data to carto
                            cartosql.insertRows(METRICS_CARTO_TABLE, METRICS_CARTO_SCHEMA.keys(), METRICS_CARTO_SCHEMA.values(), df.values.tolist(), user=CARTO_USER, key=CARTO_KEY)
            else: continue



def updateResourceWatch(new_ids):
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date' and updating any dates on layers
    INPUT   new_ids: new IDs added to Carto table (list)
    '''
    # If there are new entries in the Carto table
    if len(new_ids)>0:
        # get a list of the newest dates
        new_dates = getLatestForecastDates(new_ids)
        # get the creation date for the forecast
        new_creation_date = new_dates[0]
        logging.info('Updating Resource Watch Layers')
        for var, ds_id in DATASET_IDS.items():
            # Update the dates on layer legends
            logging.info('Updating {}'.format(var))
            # pull dictionary of current layers from API
            layer_dict = pull_layers_from_API(ds_id)
            # go through each layer, pull the definition and update
            for layer in layer_dict:
                # check which point on the timeline this is
                order = layer['attributes']['layerConfig']['order']
                # get the new date that should be used for this layer
                new_date = new_dates[order+1]
                # replace layer sql and title with new dates
                update_layer(layer, new_creation_date, new_date)
            # Update dataset's last update date on Resource Watch
            lastUpdateDate(ds_id, new_creation_date)
            
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
    existing_stations = checkCreateTable(STATION_CARTO_TABLE, STATION_CARTO_SCHEMA, STATION_UID_FIELD)

    # Fetch, process, and upload new data
    new_ids = processNewData(SOURCE_URL, existing_ids, existing_stations)

    # Check if metrics table exists, create it if it does not
    checkCreateTable(METRICS_CARTO_TABLE, METRICS_CARTO_SCHEMA, UID_FIELD)

    # Calculate air quality metrics
    existing_metrics_ids = processMetrics(new_ids)

    # Delete data to get back to MAX_ROWS
    logging.info('Deleting excess rows')
    deleteExcessRows(CARTO_TABLE, MAXROWS, TIME_FIELD)

    # Update Resource Watch
    updateResourceWatch(new_ids)

    logging.info('SUCCESS')
