from __future__ import unicode_literals

import os
import sys
import datetime
import logging
import ee

import requests


DATE_FORMAT = '%Y%m%d'

DATASET_ID_BY_COLLECTION = {
    'HYCOM/GLBu0_08/sea_temp_salinity':'e6c0dd9e-3dde-4296-91d8-87ac26ed038f',
    'HYCOM/GLBu0_08/sea_water_velocity': 'e050ee5c-0dfa-491d-862c-2274e8597793',
    'NASA_USDA/HSL/SMAP_soil_moisture': 'e7b9efb2-3836-45ae-8b6a-f8391c7bcd2f',
    'UCSB-CHG/CHIRPS/PENTAD': '932baa47-32f4-492c-8965-89aab5be0c37',
    'JAXA/GPM_L3/GSMaP/v6/operational': '1e8919fc-c1a8-4814-b819-31cdad17651e'
}


def lastUpdateDate(dataset, date):
   apiUrl = 'http://api.resourcewatch.org/v1/dataset/{0}'.format(dataset)
   headers = {
   'Content-Type': 'application/json',
   'Authorization': os.getenv('apiToken')
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

def initialize_ee():
    GEE_JSON = os.environ.get("GEE_JSON")
    _CREDENTIAL_FILE = 'credentials.json'
    GEE_SERVICE_ACCOUNT = os.environ.get("GEE_SERVICE_ACCOUNT")
    with open(_CREDENTIAL_FILE, 'w') as f:
        f.write(GEE_JSON)
    auth = ee.ServiceAccountCredentials(GEE_SERVICE_ACCOUNT, _CREDENTIAL_FILE)
    ee.Initialize(auth)

def main():
    '''Ingest new data into EE and delete old data'''
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    #initialize ee module
    initialize_ee()

    for collection_name, id in DATASET_ID_BY_COLLECTION.items():
        logging.info('Updating '+collection_name)
        #load collection and get most recent asset time stamp
        collection = ee.ImageCollection(collection_name)
        most_recent_asset = collection.sort('system:time_end', opt_ascending=False).first()
        #get time from asset in milliseconds since the UNIX epoch and convert to seconds
        most_recent_date_unix = most_recent_asset.get('system:time_end').getInfo()/1000
        #convert to datetime
        most_recent_date = datetime.datetime.fromtimestamp(most_recent_date_unix)
        # Update data set's last update date on Resource Watch
        lastUpdateDate(id, most_recent_date)

    logging.info('SUCCESS')
