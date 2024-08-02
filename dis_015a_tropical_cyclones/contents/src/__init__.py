from __future__ import unicode_literals

import os
import logging
import sys
import urllib
import datetime
import cartoframes
import cartosql
from zipfile import ZipFile
import LMIPy as lmi
import requests
import geopandas as gpd
import glob
import json
import shutil

# name of data directory in Docker container
DATA_DIR = 'data'

# pull in RW API key for updating and adding new layers
API_TOKEN = os.getenv('RW_API_KEY')

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'dis_015a_hurricane_tracks'

# column of table that can be used as a unique ID (UID)
UID_FIELD = 'uid'

# column that stores datetime information
TIME_FIELD = 'iso_time'

# how many rows can be stored in the Carto table before the oldest ones are deleted?
MAX_ROWS = 150000
# oldest date that can be stored in the Carto table before we start deleting
MAX_AGE = datetime.datetime.utcnow() - datetime.timedelta(days=365*20)

# url for cyclone track data since 1980; we used it only the first time we created the table
# url_a = 'https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/'
# url_b = 'v04r00/access/shapefile/IBTrACS.since1980.list.v04r00.lines.zip'

# url for cyclone track data for latest 3 years
url_a = 'https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/'
url_b = 'v04r01/access/shapefile/IBTrACS.last3years.list.v04r01.lines.zip'
SOURCE_URL = url_a + url_b

# maximum number of attempts that will be made to download the data
MAX_TRIES = 5

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = 'b82eab85-0fee-4212-8a7e-ca0b28a16a2f'

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
                shutil.rmtree(DATA_DIR+'/'+f, ignore_errors=True)
    except NameError:
        logging.info('No local files to clean.')

'''
FUNCTIONS FOR THIS DATASET

The functions below have been tailored to this specific dataset.
They should all be checked because their format likely will need to be changed.
'''

def gen_uid(row):
    '''
    Generate unique id using Storm Id (SID) and Time (ISO_TIME)
    INPUT   row: rows of the geodataframe for tropical cyclone track data (Series object)
    RETURN  output: formatted rows with additional column for unique id (Series object)
    '''
    # get values from ISO_TIME and format it to have underscores inbetween each time variables 
    tm = row['ISO_TIME'].replace("-", "_").replace(" ", "_").replace(":", "_")
    # get storm id from the column 'SID' and join with the formatted time
    out = row['SID'] + '_' + tm
    return out

def fetch_data():
    '''
    Download shapefile from source url and put that into a geopandas dataframe
    RETURN  gdf: geodataframe for tropical cyclone track data (dataframe)
    '''
    # create a filename for the shapefile that will be downloaded
    tmpfile = '{}.zip'.format(os.path.join(DATA_DIR,'dis_015a_hurricane_tracks-shp'))
    logging.info('Get data from source url')

    try:
        # pull data from url and save to tmpfile
        urllib.request.urlretrieve(SOURCE_URL, tmpfile)
        # unzip source data
        tmpfile_unzipped = tmpfile.split('.')[0]
        zip_ref = ZipFile(tmpfile, 'r')
        zip_ref.extractall(tmpfile_unzipped)
        zip_ref.close()
        logging.info('load in the shapefile')
        # load in the polygon shapefile
        shapefile = glob.glob(os.path.join(tmpfile_unzipped, '*.shp'))[0]
        logging.info('read the shapefile')
        gdf = gpd.read_file(shapefile)
        logging.info('generate unique id')
        # generate unique id and the values to a new column in the geodataframe
        gdf['uid'] = gdf.apply(gen_uid, axis=1)
        
        return gdf

    except Exception as e:
        logging.info(e)
        logging.info("Error fetching data")

def get_existing_ids(table, id_field):
    '''
    Check if the table exist, and pull list of IDs already in the table if it does
    INPUT   table: Carto table to check or create (string)
            id_field: name of column that we want to use as a unique ID for this table; this will be used to compare the
                    source data to the our table each time we run the script so that we only have to pull data we
                    haven't previously uploaded (string)
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
        # return an empty list because there are no IDs in the new table yet
        return []
    
def processData():
    '''
    Function to download data and upload it to Carto.
    We will first try to get the data for MAX_TRIES then quit
    RETURN  existing_ids: list of existing IDs in the table (list of strings)
            new_rows: number of rows of new data sent to Carto table (integer)
    '''
    # set success to False initially
    success = False
    # initialize tries count as 0
    tries = 0
    # try to get the data from the url for MAX_TRIES 
    while tries < MAX_TRIES and success == False:
        logging.info('Try retrieving cyclone track data, try number = {}'.format(tries))
        try:
            # pull cyclone track data from source url and format the data into a geopandas dataframe
            # download the shapefile from source 
            gdf = fetch_data()
            # set success as True after retrieving the data to break out of this loop
            success = True
        except Exception as inst:
            logging.info(inst)
            logging.info("Error fetching data, trying again")
            tries = tries + 1
            if tries == MAX_TRIES:
                logging.error("Error fetching data, and max tries reached. See source for last data update.")
    # if we suceessfully collected data from the url
    if success == True:
        # generate credentials to access the table
        creds = cartoframes.auth.Credentials(username=CARTO_USER, api_key=CARTO_KEY, 
        base_url="https://{user}.carto.com/".format(user=CARTO_USER))
        logging.info('Load the existing table as a geodataframe')
        # Check if table exists, and get existing ids
        existing_ids = get_existing_ids(CARTO_TABLE, UID_FIELD)
        # create a new geodataframe with unique ids that are not already in our Carto table
        gdf_new = gdf[~gdf['uid'].isin(existing_ids)]
        # count the number of new rows to add to the Carto table
        new_rows = gdf_new.shape[0]
        # if we have new data to upload
        if new_rows != 0:
            logging.info('Sending new data to the Carto table')
            # send new rows to the Carto table
            cartoframes.to_carto(gdf_new, CARTO_TABLE, credentials=creds, if_exists='append')
            logging.info('Successfully sent new data to the Carto table!')
        else:
            logging.info('Table already upto date!')
            
        return(existing_ids, new_rows)

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
        # convert max_age to a string
        max_age = max_age.isoformat()

    # if the max_age variable exists
    if max_age:
        # delete rows from table which are older than the max_age
        r = cartosql.deleteRows(table, "{} < '{}'".format(time_field, max_age), CARTO_USER, CARTO_KEY)
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

def updateResourceWatch():
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date' and updating any dates on layers
    '''
    # Update dataset's last update date on Resource Watch
    most_recent_date = get_most_recent_date(CARTO_TABLE)
    lastUpdateDate(DATASET_ID, most_recent_date)

    # Create a new layer once a new year of data is uploaded to Carto 
    most_recent_year = most_recent_date.year
    # pull dictionary of current layers from API
    layer_dict = pull_layers_from_API(DATASET_ID)
    # extract the years we currently have in the layers 
    current_years = [layer['attributes']['name'][:4] for layer in layer_dict]
    # sort the years 
    current_years.sort()
    # find the most recent year among the layers 
    most_recent_ly = current_years[-1]
    # if the most recent year in the updated Carto table is larger than the most recent year among the layers 
    if most_recent_year > int(most_recent_ly):
       # pull the dataset we want to update
        dataset = lmi.Dataset(DATASET_ID)
        # pull out its first layer to use as a template to create new layers
        layer_to_clone = dataset.layers[0]

        # get attributes that might need to change:
        name = layer_to_clone.attributes['name']
        description = layer_to_clone.attributes['description']
        appConfig = layer_to_clone.attributes['layerConfig']
        sql = appConfig['body']['layers'][0]['options']['sql']
        order = str(appConfig['order'])
        timeLineLabel = appConfig['timelineLabel']
        interactionConfig = layer_to_clone.attributes['interactionConfig']

        # pull out the year from the example layer's name - we will use this to find all instances of the year within our
        # example layer so that we can replace it with the correct year in the new layers
        replace_string = name[:4]

        # replace year in example layer with {}
        name_convention = name.replace(replace_string, '{}')
        description_convention = description.replace(replace_string, '{}')
        sql_convention = sql.replace(replace_string, '{}')
        order_convention = order.replace(replace_string, '{}')
        timeLineLabel_convention = timeLineLabel.replace(replace_string, '{}')
        for i, dictionary in enumerate(interactionConfig.get('output')):
            for key, value in dictionary.items():
                if value != None:
                    if replace_string in value:
                        interactionConfig.get('output')[i][key] = value.replace(replace_string, '{}')

        # generate the layer attributes with the correct year
        new_layer_name = name_convention.replace('{}', str(most_recent_year))
        new_description = description_convention.replace('{}', str(most_recent_year))
        new_sql = sql_convention.replace('{}', str(most_recent_year))
        new_timeline_label = timeLineLabel_convention.replace('{}', str(most_recent_year))
        new_order = int(order_convention.replace('{}', str(most_recent_year)))

        # Clone the example layer to make a new layer
        clone_attributes = {
            'name': new_layer_name,
            'description': new_description
        }
        new_layer = layer_to_clone.clone(token=API_TOKEN, env='production', layer_params=clone_attributes,
                                         target_dataset_id=DATASET_ID)

        # Replace layerConfig with new values
        appConfig = new_layer.attributes['layerConfig']
        appConfig['body']['layers'][0]['options']['sql'] = new_sql
        appConfig['order'] = new_order
        appConfig['timelineLabel'] = new_timeline_label
        payload = {
            'layerConfig': {
                **appConfig
            }
        }
        new_layer = new_layer.update(update_params=payload, token=API_TOKEN)

         # Replace interaction config with new values
        interactionConfig = new_layer.attributes['interactionConfig']
        for i, element in enumerate(interactionConfig['output']):
            if '{}' in element.get('property'):
                interactionConfig['output'][i]['property'] = interactionConfig['output'][i]['property'].replace(
                    '{}', str(most_recent_year))
        payload = {
            'interactionConfig': {
                **interactionConfig
            }
        }
        new_layer = new_layer.update(update_params=payload, token=API_TOKEN)
    update_default_layer(DATASET_ID, most_recent_year)

def update_default_layer(ds_id, default_year):
    '''
    Given a Resource Watch dataset's API ID and the year we want to set as the default layer, this function will 
    update the default layer on Resource Watch
    INPUT   ds_id: Resource Watch API dataset ID (string)
            default_year: year to be used as default layer on Resource Watch (integer)
    '''
    # pull the dataset we want to update
    dataset = lmi.Dataset(ds_id)
    for layer in dataset.layers:
        # check which year the current layer is for
        year = layer.attributes['name'][:4]
        # check if this is currently the default layer
        default = layer.attributes['default']
        # if it is the year we want to set as default, and it is not already set as default,
        # update the 'default' parameter to True
        if year == str(default_year) and default==False:
            payload = {
                'default': True}
            # update the layer on the API
            layer = layer.update(update_params=payload, token=API_TOKEN)
            print(f'default layer updated to {year}')
        # if this layer should no longer be the default layer, but it was previously,
        # make sure the 'default' parameter is False
        elif year != str(default_year) and default==True:
            payload = {
                'default': False}
            # update the layer on the API
            layer = layer.update(update_params=payload, token=API_TOKEN)
            print(f'{year} is no longer default layer')

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Fetch, process, and upload new data
    existing_ids, num_new = processData()
    logging.info('Previous rows: {},  New rows: {}'.format(len(existing_ids), num_new))

    # Delete data to get back to MAX_ROWS
    deleteExcessRows(CARTO_TABLE, MAX_ROWS, TIME_FIELD, MAX_AGE)

    # Update Resource Watch
    updateResourceWatch()

    # Delete local files
    delete_local()

    logging.info('SUCCESS')

