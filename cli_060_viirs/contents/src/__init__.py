import logging
import sys
import os
import datetime
import json
import requests

# Resource Watch dataset API ID for daytime layer
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DAY_DATASET_ID = 'bd9f603e-a559-4cc1-84f4-de0ddc7c341f'
# API url for daytime dataset on RW
DAY_RW_API = f'https://api.resourcewatch.org/v1/dataset/{DAY_DATASET_ID}/layer/'
# define layer config to be used in daytime layer definition on RW
DAY_LAYER_CONFIG = {
        "type": "tileLayer",
        "url": "https://gibs.earthdata.nasa.gov/wmts/epsg3857/best/VIIRS_SNPP_CorrectedReflectance_TrueColor/default/{date}/GoogleMapsCompatible_Level9/{z}/{y}/{x}.jpg",
        "body": {
            "format": "image/jpeg"
        },
        "layer_id": "275dcc83-673b-44e4-b7db-253ff1d2d867"
    }

# Resource Watch dataset API ID for nighttime layer
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
NIGHT_DATASET_ID = '55a1e0d5-5af9-4ebc-a261-e9c40606d81c'
# API url for daytime dataset on RW
NIGHT_RW_API = f'https://api.resourcewatch.org/v1/dataset/{NIGHT_DATASET_ID}/layer/'
# define layer config to be used in nighttime layer definition on RW
NIGHT_LAYER_CONFIG = {
        "body": {
            "format": "image/png"
        },
        "url": "https://gibs.earthdata.nasa.gov/wmts/epsg3857/best/VIIRS_SNPP_DayNightBand_ENCC/default/{date}/GoogleMapsCompatible_Level8/{z}/{y}/{x}.png",
        "type": "tileLayer",
        "layer_id": "91642712-916c-4b03-9d3c-1924a998ea98"
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

def getLastUpdate(dataset):
    '''
    Given a Resource Watch dataset's API ID,
    this function will get the current 'last update date' from the API
    and return it as a datetime
    INPUT   dataset: Resource Watch API dataset ID (string)
    RETURN  lastUpdateDT: current 'last update date' for the input dataset (datetime)
    '''
    # generate the API url for this dataset
    apiUrl = 'http://api.resourcewatch.org/v1/dataset/{}'.format(dataset)
    # pull the dataset from the API
    r = requests.get(apiUrl)
    # find the 'last update date'
    lastUpdateString=r.json()['data']['attributes']['dataLastUpdated']
    # split this date into two pieces at the seconds decimal so that the datetime module can read it:
    # ex: '2020-03-11T00:00:00.000Z' will become '2020-03-11T00:00:00' (nofrag) and '000Z' (frag)
    nofrag, frag = lastUpdateString.split('.')
    # generate a datetime object
    nofrag_dt = datetime.datetime.strptime(nofrag, "%Y-%m-%dT%H:%M:%S")
    # add back the microseconds to the datetime
    lastUpdateDT = nofrag_dt.replace(microsecond=int(frag[:-1])*1000)
    return lastUpdateDT

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
    cur_title = layer['attributes']['name']
    
    # get current date being used from title by string manupulation
    old_date_text = cur_title.split(' VIIRS')[0]
    # get text for new date
    new_date_text = datetime.datetime.strftime(new_date, "%B %d, %Y")

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
        
def updateResourceWatch(most_recent_date):
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date' and updating any dates on layers
    INPUT   most_recent_date: most recent date of imagery being shown (datetime)
    '''
    for ds_id in [DAY_DATASET_ID, NIGHT_DATASET_ID]:
        # Get the current 'last update date' from the dataset on Resource Watch
        current_date = getLastUpdate(ds_id)
        # Update the dates on layer legends
        logging.info('Updating {}'.format(ds_id))
        # pull dictionary of current layers from API
        layer_dict = pull_layers_from_API(ds_id)
        # go through each layer, pull the definition and update
        for layer in layer_dict:
            # replace layer title with new dates
            update_layer(layer, most_recent_date)
        # If the most recent date from the GEE collection does not match the 'last update date' on the RW API, update it
        if current_date != most_recent_date:
            logging.info('Updating last update date and flushing cache.')
            # Update dataset's last update date on Resource Watch
            lastUpdateDate(ds_id, most_recent_date)

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # create a string of yesterdays date that we can use in the leaflet url to pull in the most recent imagery
    yesterday = datetime.datetime.today() - datetime.timedelta(days=2)
    date = yesterday.strftime('%Y-%m-%d')

    # create headers to send with the request to update the dataset layers
    headers = {
        'Content-Type': 'application/json',
        'Authorization': os.getenv('apiToken')
    }

    # Update daytime imagery layer configuration to pull yesterday's data
    api_url = DAY_RW_API + DAY_LAYER_CONFIG['layer_id']
    DAY_LAYER_CONFIG['url'] = DAY_LAYER_CONFIG['url'].format(x='{x}', y='{y}', z='{z}', date=date)
    payload = {
                'application': ['rw'],
                'layerConfig': DAY_LAYER_CONFIG
            }
    logging.debug(payload)
    # send request to API to update layers
    response = requests.request(
        'PATCH',
        api_url,
        data=json.dumps(payload),
        headers=headers
    )
    if not response.ok:
        logging.error("ERROR: failed to update daytime layer")
        logging.error(response.text)
    logging.info('Success for daytime imagery')

    # Update nighttime imagery layer configuration to pull yesterday's data
    api_url = NIGHT_RW_API + NIGHT_LAYER_CONFIG['layer_id']
    NIGHT_LAYER_CONFIG['url'] = NIGHT_LAYER_CONFIG['url'].format(x='{x}', y='{y}', z='{z}', date=date)
    payload = {
                'application': ['rw'],
                'layerConfig': NIGHT_LAYER_CONFIG
            }
    logging.debug(payload)
    # send request to API to update layers
    response = requests.request(
        'PATCH',
        api_url,
        data=json.dumps(payload),
        headers=headers
    )
    if not response.ok:
        logging.error("ERROR: failed to update nighttime layer")
        logging.error(response.text)
    logging.info('Success for nighttime imagery')

    # Update Resource Watch
    updateResourceWatch(yesterday)

    logging.info('SUCCESS')
