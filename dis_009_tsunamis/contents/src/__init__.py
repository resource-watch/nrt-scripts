import logging
import sys
import os
import requests as req
import datetime
import pandas as pd
import cartoframes
import requests
import numpy as np
import json

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'dis_009_tsunamis'

# url for tsunami data
SOURCE_URL = "https://ngdc.noaa.gov/nndc/struts/results?type_0=Exact&query_0=$ID&t=101650&s=69&d=59&dfn=tsevent.txt"

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

def create_geom(lat, lon):
    ''' 
    Create geometry using latitude and longitude information from source data
    INPUT   lat: latitude of the tsunami event (string)
            lon: longitude of the tsunami event (string)
    RETURN  geom: geometry of the tsunami event (geojson)
    ''' 
    # check if there is input data for latitude
    if lat:
        # create geometry using latitude and longitude information
        geom = {
            "type": "Point",
            "coordinates": [
                lon,
                lat
            ]
        }
        return geom
    else:
        return None

def processData():
    """
    Retrive data from source url, create a dataframe, and return the processed dataframe
    RETURN  df: dataframe with data (dataframe object)
    """

    # get data from source url through a request response JSON
    data = req.get(SOURCE_URL).text
    # split data based on new lines to get each rows as a separate value
    data = data.split('\n')
    # split each rows using tab character to separate out the columns
    lines = [line.split('\t') for line in data]
    # get header from first row
    header = lines[0]
    # get all rows of data (all rows that come after the header)
    rows = lines[1:]
    # create a pandas dataframe using the data
    df = pd.DataFrame(rows)
    # specify column names for the dataframe using header
    df.columns = header
    # create the geomtery for each data point using 'LATITUDE' and 'LONGITUDE' columns from source data
    # add the geometry to a new column in pandas dataframe
    df['the_geom'] = list(map(lambda coords: create_geom(*coords), zip(df['LATITUDE'],df['LONGITUDE'])))
    # specify column names for columns containing string data
    text_cols = ['the_geom', 'COUNTRY', 'STATE', 'LOCATION_NAME']
    # find numeric columns (all columns that are not in the text_cols list)
    number_cols = [x for x in df.columns if x not in text_cols]
    # check if there are any empty entries; replace them with NAN
    df = df.replace(r'^\s*$', np.nan, regex=True)
    # loop through each numeric columns
    for col in number_cols:
        # convert those columns in dataframe to numeric type
        # invalid parsing will be set as NaN
        df[col] =  pd.to_numeric(df[col], errors='coerce')

    return(df)

def get_most_recent_date(table):
    '''
    Find the most recent date of data in the Carto table using the parsed dataframe
    INPUT   table: dataframe that was written to the Carto table (dataframe object)
    RETURN  most_recent_date: most recent date of data in the Carto table, 
            found using the 'YEAR', 'MONTH' and 'DAY' column of the dataframe (datetime object)
    '''
    #convert columns associated with date values to numeric for sorting
    table.YEAR = pd.to_numeric(table.YEAR, errors='coerce')
    table.MONTH = pd.to_numeric(table.MONTH, errors='coerce')
    table.DAY = pd.to_numeric(table.DAY, errors='coerce')
    # sort the table by the 'YEAR', 'MONTH' and 'DAY' column
    sorted_table = table.sort_values(by=['YEAR', 'MONTH', 'DAY'], ascending=False).reset_index()
    # get the first value from 'YEAR', 'MONTH' and 'DAY' column
    # those will represent the most recent year, month, day
    year = int(sorted_table['YEAR'][0])
    month = int(sorted_table['MONTH'][0])
    day = int(sorted_table['DAY'][0])
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
    new_date_start = (current_date - datetime.timedelta(days=365))
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
    lastUpdateDate(DATASET_ID, most_recent_date)

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # create a CartoContext object that is authenticated against our CARTO account
    # it will be used to write dataframe to Carto table
    cc = cartoframes.CartoContext(base_url='https://{}.carto.com/'.format(CARTO_USER),
                                  api_key=CARTO_KEY)

    # fetch data from FTP, dedupe, process
    df = processData()

    # get the number of rows in the dataframe
    num_rows = df.shape[0]
    # write the dataframe to the Carto table, overwriting existing data
    cc.write(df, CARTO_TABLE, overwrite=True, privacy='public')

    # Update Resource Watch
    updateResourceWatch(df)

    # Notify results
    logging.info('Existing rows: {}'.format(num_rows))
    logging.info("SUCCESS")
