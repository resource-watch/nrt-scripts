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
    # 1. update data sets in GEE Catalog
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

    logging.info('Success for GEE Catalog data sets')
    
    # 2. update data sets on WRI-RW Carto account
    WRIRW_DATASETS = {'modis_c6_global_7d': 'a9e33aad-eece-4453-8279-31c4b4e0583f',
                      'df_map_2ylag_1': '25eebe25-aaf2-48fc-ab7b-186d7498f393'}

    url = "https://{account}.carto.com/api/v1/synchronizations/?api_key={API_key}".format(
        account=os.getenv('CARTO_WRI_RW_USER'), API_key=os.getenv('CARTO_WRI_RW_KEY'))
    r = requests.get(url)
    json = r.json()
    sync = json['synchronizations']

    for table_name, id in WRIRW_DATASETS.items():
        logging.info('Updating ' + table_name)
        table = next(item for item in sync if item["name"] == table_name)
        # ran_at = The date time at which the table had its contents synched with the source file.
        # updated_at = The date time at which the table had its contents modified.
        # modified_at = The date time at which the table was manually modified, if applicable.
        last_sync = table['updated_at']
        TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
        last_update_time = datetime.datetime.strptime(last_sync, TIME_FORMAT)
        lastUpdateDate(id, last_update_time)
    logging.info('Success for WRI-RW')

    # 3. update data sets on GFW Carto account
    GFW_DATASETS = {'gfw_oil_palm': '6e05a9e8-ba07-4e6f-8337-31c5362d07fe',
                    'gfw_wood_fiber': '83de627f-524b-4162-a10c-384dc3e8107a',
                    'vnp14imgtdl_nrt_global_7d': '20cc5eca-8c63-4c41-8e8e-134dcf1e6d76',
                    'forma_activity': 'e1b40fdd-04f9-43ab-b4f1-d3ceee39fea1'}

    url = "https://{account}.carto.com/api/v1/synchronizations/?api_key={API_key}".format(
        account=os.getenv('CARTO_WRI_01_USER'), API_key=os.getenv('CARTO_WRI_01_KEY'))

    r = requests.get(url)
    json = r.json()
    sync = json['synchronizations']

    for table_name, id in GFW_DATASETS.items():
        logging.info('Updating ' + table_name)
        table = next(item for item in sync if item["name"] == table_name)
        # ran_at = The date time at which the table had its contents synched with the source file.
        # updated_at = The date time at which the table had its contents modified.
        # modified_at = The date time at which the table was manually modified, if applicable.
        last_sync = table['updated_at']
        TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
        last_update_time = datetime.datetime.strptime(last_sync, TIME_FORMAT)
        lastUpdateDate(id, last_update_time)
    logging.info('Success for WRI-01')

    # 4. update data sets on WRI-RW Carto account
    RWNRT_DATASETS = {'vnp14imgtdl_nrt_global_7d': '20cc5eca-8c63-4c41-8e8e-134dcf1e6d76'}

    url = "https://{account}.carto.com/api/v1/synchronizations/?api_key={API_key}".format(
        account=os.getenv('CARTO_USER'), API_key=os.getenv('CARTO_KEY'))
    r = requests.get(url)
    json = r.json()
    sync = json['synchronizations']

    for table_name, id in RWNRT_DATASETS.items():
        logging.info('Updating ' + table_name)
        table = next(item for item in sync if item["name"] == table_name)
        # ran_at = The date time at which the table had its contents synched with the source file.
        # updated_at = The date time at which the table had its contents modified.
        # modified_at = The date time at which the table was manually modified, if applicable.
        last_sync = table['updated_at']
        TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
        last_update_time = datetime.datetime.strptime(last_sync, TIME_FORMAT)
        lastUpdateDate(id, last_update_time)
    logging.info('Success for RW-NRT')
