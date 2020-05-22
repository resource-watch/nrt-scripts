from __future__ import unicode_literals

import os
import sys
import datetime
import logging
import ee
import time
import requests

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

def getLayerIDs(dataset):
    '''
    Given a Resource Watch dataset's API ID,
    this function will return a list of all the layer IDs associated with it
    INPUT   dataset: Resource Watch API dataset ID (string)
    RETURN  layerIDs: Resource Watch API layer IDs for the input dataset (list of strings)
    '''
    # generate the API url for this dataset - this must include the layers
    apiUrl = 'http://api.resourcewatch.org/v1/dataset/{}?includes=layer'.format(dataset)
    # pull the dataset from the API
    r = requests.get(apiUrl)
    #get a list of all the layers
    layers = r.json()['data']['attributes']['layer']
    # create an empty list to store the layer IDs
    layerIDs =[]
    # go through each layer and add its ID to the list
    for layer in layers:
        # only add layers that have Resource Watch listed as its application
        if layer['attributes']['application']==['rw']:
            layerIDs.append(layer['id'])
    return layerIDs

def flushTileCache(layer_id):
    """
    Given the API ID for a GEE layer on Resource Watch,
    this function will clear the layer cache.
    If the cache is not cleared, when you view the dataset on Resource Watch, old and new tiles will be mixed together.
    INPUT   layer_id: Resource Watch API layer ID (string)
    """
    # generate the API url for this layer's cache
    apiUrl = 'http://api.resourcewatch.org/v1/layer/{}/expire-cache'.format(layer_id)
    # create headers to send with the request to clear the cache
    headers = {
    'Content-Type': 'application/json',
    'Authorization': os.getenv('apiToken')
    }

    # clear the cache for the layer
    # sometimetimes this fails, so we will try multiple times, if it does

    # specify that we are on the first try
    try_num=1
    tries = 4
    while try_num<tries:
        try:
            # try to delete the cache
            r = requests.delete(url = apiUrl, headers = headers, timeout=1000)
            # if we get a 200, the cache has been deleted
            # if we get a 504 (gateway timeout) - the tiles are still being deleted, but it worked
            if r.ok or r.status_code==504:
                logging.info('[Cache tiles deleted] for {}: status code {}'.format(layer_id, r.status_code))
                return r.status_code
            # if we don't get a 200 or 504:
            else:
                # if we are not on our last try, wait 60 seconds and try to clear the cache again
                if try_num < (tries-1):
                    logging.info('Cache failed to flush: status code {}'.format(r.status_code))
                    time.sleep(60)
                    logging.info('Trying again.')
                # if we are on our last try, log that the cache flush failed
                else:
                    logging.error('Cache failed to flush: status code {}'.format(r.status_code))
                    logging.error('Aborting.')
            try_num += 1
        except Exception as e:
            logging.error('Failed: {}'.format(e))

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

def initialize_ee():
    '''
    Initialize ee module
    '''
    # get GEE credentials from env file
    GEE_JSON = os.environ.get("GEE_JSON")
    _CREDENTIAL_FILE = 'credentials.json'
    GEE_SERVICE_ACCOUNT = os.environ.get("GEE_SERVICE_ACCOUNT")
    with open(_CREDENTIAL_FILE, 'w') as f:
        f.write(GEE_JSON)
    auth = ee.ServiceAccountCredentials(GEE_SERVICE_ACCOUNT, _CREDENTIAL_FILE)
    ee.Initialize(auth)

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # initialize ee module
    initialize_ee()

    '''
    update last update dates on RW for datasets in GEE Catalog
    '''
    # make dictionary associating GEE assets with RW dataset IDs
    GEE_DATASETS = {
        'HYCOM/GLBu0_08/sea_temp_salinity': 'e6c0dd9e-3dde-4296-91d8-87ac26ed038f',
        'HYCOM/GLBu0_08/sea_water_velocity': 'e050ee5c-0dfa-491d-862c-2274e8597793',
        'NASA_USDA/HSL/SMAP_soil_moisture': 'e7b9efb2-3836-45ae-8b6a-f8391c7bcd2f',
        'UCSB-CHG/CHIRPS/PENTAD': '55cb7e8d-a978-4184-b347-4ba64cd88ad2',
        'JAXA/GPM_L3/GSMaP/v6/operational': '1e8919fc-c1a8-4814-b819-31cdad17651e'
    }
    # Check if datasets have been updated
    for collection_name, dataset_id in GEE_DATASETS.items():
        # get last update date currently being displayed on RW
        current_date = getLastUpdate(dataset_id)
        # load GEE collection and get most recent asset time stamp
        collection = ee.ImageCollection(collection_name)
        most_recent_asset = collection.sort('system:time_end', opt_ascending=False).first()
        # get time from asset in milliseconds since the UNIX epoch and convert to seconds
        most_recent_date_unix = most_recent_asset.get('system:time_end').getInfo()/1000
        # convert to datetime
        most_recent_date = datetime.datetime.fromtimestamp(most_recent_date_unix)
        # if our timestamp is not correct, update it
        if current_date!=most_recent_date:
            logging.info('Updating ' + collection_name)
            # Update dataset's last update date on Resource Watch
            lastUpdateDate(dataset_id, most_recent_date)
            # flush the tile cache for all layer in the dataset so that the old tiles are deleted
            layer_ids = getLayerIDs(dataset_id)
            for layer_id in layer_ids:
                flushTileCache(layer_id)
    logging.info('Success for GEE Catalog data sets')


    '''
    update last update dates on RW for datasets on WRI-RW Carto account
    '''
    # make dictionary associating Carto tables with RW dataset IDs
    WRIRW_DATASETS = {'modis_c6_global_7d': 'a9e33aad-eece-4453-8279-31c4b4e0583f',
                      'df_map_2ylag_1': '25eebe25-aaf2-48fc-ab7b-186d7498f393'}

    # pull the latest information about dataset syncs from WRI-RW Carto account
    url = "https://{account}.carto.com/api/v1/synchronizations/?api_key={API_key}".format(
        account=os.getenv('CARTO_WRI_RW_USER'), API_key=os.getenv('CARTO_WRI_RW_KEY'))
    r = requests.get(url)
    json = r.json()
    sync = json['synchronizations']

    # go through each of the dataset on RW from this account
    for table_name, id in WRIRW_DATASETS.items():
        # get the last update date currently showing on RW
        current_date = getLastUpdate(id)
        # find the synchronization information from Carto for this table
        table = next(item for item in sync if item["name"] == table_name)
        # find when the last sync occurred
        # note about sync info available from Carto:
        # ran_at = The date time at which the table had its contents synched with the source file.
        # updated_at = The date time at which the table had its contents modified.
        # modified_at = The date time at which the table was manually modified, if applicable.
        last_sync = table['ran_at']
        # define the time format used by Carto
        TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
        # generate a datetime for the last synchronization of this table
        last_update_time = datetime.datetime.strptime(last_sync, TIME_FORMAT)
        # update the last update date on RW, if needed
        if current_date!=last_update_time:
            logging.info('Updating ' + table_name)
            lastUpdateDate(id, last_update_time)
    logging.info('Success for WRI-RW')


    '''
    update last update dates on RW for datasets on GFW Carto account (WRI-01)
    '''
    # make dictionary associating Carto tables with RW dataset IDs
    GFW_DATASETS = {'gfw_oil_palm': '6e05a9e8-ba07-4e6f-8337-31c5362d07fe',
                    'gfw_wood_fiber': '83de627f-524b-4162-a10c-384dc3e8107a',
                    'forma_activity': 'e1b40fdd-04f9-43ab-b4f1-d3ceee39fea1',
                    'biodiversity_hotspots': '4458eb12-8572-45d1-bf07-d5a3ee097021'}

    # pull the latest information about dataset syncs from WRI-01 Carto account
    url = "https://{account}.carto.com/api/v1/synchronizations/?api_key={API_key}".format(
        account=os.getenv('CARTO_WRI_01_USER'), API_key=os.getenv('CARTO_WRI_01_KEY'))
    r = requests.get(url)
    json = r.json()
    sync = json['synchronizations']

    # go through each of the dataset on RW from this account
    for table_name, id in GFW_DATASETS.items():
        # get the last update date currently showing on RW
        current_date = getLastUpdate(id)
        # find the synchronization information from Carto for this table
        table = next(item for item in sync if item["name"] == table_name)
        # find when the last sync occurred
        # note about sync info available from Carto:
        # ran_at = The date time at which the table had its contents synched with the source file.
        # updated_at = The date time at which the table had its contents modified.
        # modified_at = The date time at which the table was manually modified, if applicable.
        last_sync = table['ran_at']
        # define the time format used by Carto
        TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
        # generate a datetime for the last synchronization of this table
        last_update_time = datetime.datetime.strptime(last_sync, TIME_FORMAT)
        # update the last update date on RW, if needed
        if current_date!=last_update_time:
            logging.info('Updating ' + table_name)
            lastUpdateDate(id, last_update_time)
    logging.info('Success for WRI-01')


    '''
    update last update dates on RW for datasets on RW-NRT Carto account
    '''
    # make dictionary associating Carto tables with RW dataset IDs
    RWNRT_DATASETS = {'vnp14imgtdl_nrt_global_7d': '64c948a6-5e34-4ef2-bb69-6a6535967bd5'}

    # pull the latest information about dataset syncs from RW-NRT Carto account
    url = "https://{account}.carto.com/api/v1/synchronizations/?api_key={API_key}".format(
        account=os.getenv('CARTO_USER'), API_key=os.getenv('CARTO_KEY'))
    r = requests.get(url)
    json = r.json()
    sync = json['synchronizations']

    # go through each of the dataset on RW from this account
    for table_name, id in RWNRT_DATASETS.items():
        # get the last update date currently showing on RW
        current_date = getLastUpdate(id)
        # find the synchronization information from Carto for this table
        table = next(item for item in sync if item["name"] == table_name)
        # find when the last sync occurred
        # note about sync info available from Carto:
        # ran_at = The date time at which the table had its contents synched with the source file.
        # updated_at = The date time at which the table had its contents modified.
        # modified_at = The date time at which the table was manually modified, if applicable.
        last_sync = table['ran_at']
        # define the time format used by Carto
        TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
        # generate a datetime for the last synchronization of this table
        last_update_time = datetime.datetime.strptime(last_sync, TIME_FORMAT)
        # update the last update date on RW, if needed
        if current_date!=last_update_time:
            logging.info('Updating ' + table_name)
            lastUpdateDate(id, last_update_time)
    logging.info('Success for RW-NRT')
