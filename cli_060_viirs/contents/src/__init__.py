import logging
import sys
import os
import requests
import datetime
import json
import requests

logging.basicConfig(stream=sys.stderr, level=logging.INFO)

DATASETS = ['bd9f603e-a559-4cc1-84f4-de0ddc7c341f','20cc5eca-8c63-4c41-8e8e-134dcf1e6d76']
DATASET_ID = 'bd9f603e-a559-4cc1-84f4-de0ddc7c341f'
### Constants
RW_API = 'https://api.resourcewatch.org/v1/dataset/{dataset}/layer/'.format(dataset = DATASET_ID)
LAYER_CONFIGS = [
    {
        "type": "tileLayer",
        "url": "https://gibs.earthdata.nasa.gov/wmts/epsg3857/best/VIIRS_SNPP_CorrectedReflectance_TrueColor/default/{date}/GoogleMapsCompatible_Level9/{z}/{y}/{x}.jpg",
        "body": {
            "format": "image/jpeg"
        },
        "id": "275dcc83-673b-44e4-b7db-253ff1d2d867"
    },
    {
        "body": {
            "format": "image/png"
        },
        "url": "https://gibs.earthdata.nasa.gov/wmts/epsg3857/best/VIIRS_SNPP_DayNightBand_ENCC/default/{date}/GoogleMapsCompatible_Level8/{z}/{y}/{x}.png",
        "type": "tileLayer",
        "id": "91642712-916c-4b03-9d3c-1924a998ea98"
    }
]
apiToken = os.getenv('apiToken') or os.environ.get('rw_api_token') or os.environ.get('RW_API_KEY')

def lastUpdateDate(dataset, date):
   apiUrl = 'http://api.resourcewatch.org/v1/dataset/{0}'.format(dataset)
   headers = {
   'Content-Type': 'application/json',
   'Authorization': apiToken
   }
   body = {
       "dataLastUpdated": date.isoformat()
   }
   try:
       r = requests.patch(url = apiUrl, json = body, headers = headers)
       logging.info('[lastUpdated]: SUCCESS, '+ date.isoformat() +' status code '+str(r.status_code))
       return 0
   except Exception as e:
       logging.error('[lastUpdated]: '+str(e))


def main():
    logging.info('BEGIN')
    today = datetime.datetime.today()
    yesterday = today - datetime.timedelta(days=2)
    date = yesterday.strftime('%Y-%m-%d')

    for layer in LAYER_CONFIGS:
        api_url = RW_API + layer['id']
        layer['url'] = layer['url'].format(x='{x}', y='{y}', z='{z}', date=date)
        payload = {
                    'application': ['rw'],
                    'layerConfig': layer
                }
        logging.debug(payload)
        headers = {
            'Content-Type': 'application/json',
            'Authorization': apiToken
        }
        response = requests.request(
            'PATCH',
            api_url,
            data=json.dumps(payload),
            headers=headers
        )
        if not response.ok:
            logging.error("ERROR: failed to update layer")
            logging.error(response.text)
        elif response.ok:
            for dataset in DATASETS:
                lastUpdateDate(dataset, date)

    logging.info('SUCCESS')
