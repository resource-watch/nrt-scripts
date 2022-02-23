import logging
import sys
import os
import datetime
import pandas as pd
import cartoframes
import requests
import numpy as np
import json
from geopandas import GeoDataFrame, points_from_xy
from cartoframes.auth import set_default_credentials

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'dis_009_tsunamis'

# url for tsunami data
SOURCE_URL = "https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/tsunamis/events"

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = '2fb159b3-e613-40ec-974c-21b22c930ce4'

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
FUNCTIONS FOR THIS DATASET

The functions below have been tailored to this specific dataset.
They should all be checked because their format likely will need to be changed.
'''
def processData():
    """
    Retrive data from source url, create a dataframe, and return the processed dataframe
    RETURN  df: dataframe with data (dataframe object)
    """
    # get data from source url through a request response JSON
    data = requests.get(SOURCE_URL)
    json_dict = json.loads(data.text)
    # convert to a pandas datafrome
    df = pd.DataFrame.from_dict(json_dict['items'])
    # specify column names for columns containing string data
    text_cols = ['country', 'locationName', 'publish', 'area']
    # find numeric columns (all columns that are not in the text_cols list)
    number_cols = [x for x in df.columns if x not in text_cols]
    # check if there are any empty entries; replace them with NAN
    df = df.replace(r'^\s*$', np.nan, regex = True)
    # loop through each numeric columns
    for col in number_cols:
        # convert those columns in dataframe to numeric type
        df[col] =  df[col].astype(float)
    # drop rows without latitude or longitude
    df = df.loc[df['latitude'].notnull(),]
    df = df.loc[df['longitude'].notnull(),]
    # convert column names to lower case
    df.columns = [x.lower() for x in df.columns]

    return(df)

def get_most_recent_date(table):
    '''
    Find the most recent date of data in the Carto table using the parsed dataframe
    INPUT   table: dataframe that was written to the Carto table (dataframe object)
    RETURN  most_recent_date: most recent date of data in the Carto table, 
            found using the 'year', 'month' and 'day' column of the dataframe (datetime object)
    '''
    #convert columns associated with date values to numeric for sorting
    table.year = pd.to_numeric(table.year, errors = 'coerce')
    table.month = pd.to_numeric(table.month, errors = 'coerce')
    table.day = pd.to_numeric(table.day, errors = 'coerce')
    # sort the table by the 'year', 'month' and 'day' column
    sorted_table = table.sort_values(by = ['year', 'month', 'day'], ascending = False).reset_index()
    # get the first value from 'year', 'month' and 'day' column
    # those will represent the most recent year, month, day
    year = int(sorted_table['year'][0])
    month = int(sorted_table['month'][0])
    day = int(sorted_table['day'][0])
    # create a datetime object using the most recent year, month, day
    most_recent_date = datetime.date(year, month, day)

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

def update_layer(layer, title):
    '''
    Update layers in Resource Watch back office.
    INPUT   layer: layer that will be updated (string)
            title: current title of the layer (string)
    '''
    # get current date being used from title by string manupulation
    old_date_text = title.split(' Tsunami Events (Past Year)')[0]

    # get current date
    current_date = datetime.datetime.now()    
    # get text for new date end which will be the current date
    new_date_end = current_date.strftime("%B %d, %Y")
    # get most recent starting date, 365 days ago
    new_date_start = (current_date - datetime.timedelta(days = 365))
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
        
def updateResourceWatch(df):
    '''
    This function should update Resource Watch to reflect the uploaded data.
    This may include updating the 'last update date' and updating any dates on layers
    INPUT   df: dataframe that was written to the Carto table (dataframe object)
    '''
    # Update dataset's last update date on Resource Watch
    most_recent_date = get_most_recent_date(df)
    # Update the dates on layer legends
    logging.info('Updating {}'.format(CARTO_TABLE))
    # pull dictionary of current layers from API
    layer_dict = pull_layers_from_API(DATASET_ID)
    # go through each layer, pull the definition and update
    for layer in layer_dict:
        # get current layer titile
        cur_title = layer['attributes']['name']         
        # if we are processing the layer that shows tsunami events occurred over the past year
        if cur_title.endswith('Tsunami Events (Past Year)'):
            # replace layer title with new dates
            update_layer(layer, cur_title)

    # get current date
    current_date = datetime.datetime.now().date()
    # use script running date as last update date
    if most_recent_date < current_date:
        most_recent_date = current_date
    lastUpdateDate(DATASET_ID, most_recent_date)

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # set Carto credentials
    # it will be used to write dataframe to Carto table
    set_default_credentials(username = CARTO_USER, base_url = "https://{user}.carto.com/".format(user = CARTO_USER), api_key = CARTO_KEY)

    # fetch data from FTP, dedupe, process
    df = processData()
    # convert dataframe to geodataframe and set geomtry attribute
    gdf = GeoDataFrame(df, geometry = points_from_xy(df.longitude, df.latitude))

    # get the number of rows in the dataframe
    num_rows = df.shape[0]
    
    # write the dataframe to the Carto table, overwriting existing data
    cartoframes.to_carto(df, CARTO_TABLE, if_exists = 'replace', geom_col = 'geomtry')
    # update privacy settings
    cartoframes.update_privacy_table(CARTO_TABLE, 'link')

    # Update Resource Watch
    updateResourceWatch(df)

    # Notify results
    logging.info('Existing rows: {}'.format(num_rows))
    logging.info("SUCCESS")
