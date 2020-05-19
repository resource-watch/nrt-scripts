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

def updateResourceWatch(most_recent_date):
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date', flushing the tile cache, and updating any dates on layers
    INPUT   most_recent_date: most recent date of imagery being shown (datetime)
    '''
    for ds_id in [DAY_DATASET_ID, NIGHT_DATASET_ID]:
        # Get the current 'last update date' from the dataset on Resource Watch
        current_date = getLastUpdate(ds_id)
        # If the most recent date from the GEE collection does not match the 'last update date' on the RW API, update it
        if current_date != most_recent_date:
            logging.info('Updating last update date and flushing cache.')
            # Update dataset's last update date on Resource Watch
            lastUpdateDate(ds_id, most_recent_date)
            # get layer ids and flush tile cache for each
            layer_ids = getLayerIDs(ds_id)
            for layer_id in layer_ids:
                flushTileCache(layer_id)
        # Update the dates on layer legends - TO BE ADDED IN FUTURE

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
