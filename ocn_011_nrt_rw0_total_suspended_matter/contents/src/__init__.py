from __future__ import unicode_literals

import os
import sys
import datetime
import logging
import subprocess
import eeUtil
import requests
import time
import urllib
import urllib.request
import gdal
import numpy as np
from collections import OrderedDict 
import json 
import ftplib

DATA_DICT = OrderedDict()
DATA_DICT['tsm_month'] = {
        'sds': [
            'TSM_mean',
        ],
        'interval': 'month',
        'original_nodata': -999,
        'missing_data': [
            -999,
        ],
        'pyramiding_policy': 'MEAN',
    }
DATA_DICT['tsm_8_days'] = {
        'sds': [
            'TSM_mean',
        ],
        'interval':'8-day',
        'original_nodata': -999,
        'missing_data': [
            -999,
        ],
        'pyramiding_policy': 'MEAN',
    }

# filename format for GEE
FILENAME = 'ocn_011_total_suspended_matter_{var}_{date}'

# name of data directory in Docker container
DATA_DIR = os.path.join(os.getcwd(),'data')

# name of collection in GEE where we will upload the final data
COLLECTION = '/projects/resource-watch-gee/ocn_011_nrt_total_suspended_matter'

# generate generic string that can be formatted to name each product's GEE collection
EE_COLLECTION_GEN = COLLECTION + '/{var}'

# name of folder to store data in Google Cloud Storage
GS_FOLDER = COLLECTION[1:]

# do you want to delete everything currently in the GEE collection when you run this script?
CLEAR_COLLECTION_FIRST = False

# how many assets can be stored in the GEE collection before the oldest ones are deleted?
MAX_ASSETS = 3

# format of date used in both source and GEE
DATE_FORMAT = '%Y%m%d'

# url from which the data is downloaded 
SOURCE_URL = 'ftp://{}:{}@ftp.hermes.acri.fr{}'

# username and password for the ftp service to download data 
ftp_username = os.environ.get('GLOBCOLOUR_USERNAME')
ftp_password = os.environ.get('GLOBCOLOUR_PASSWORD')

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = '6ad0f556-20fd-4ddf-a5cc-bf93c003a463'

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
FUNCTIONS FOR RASTER DATASETS

The functions below must go in every near real-time script for a RASTER dataset.
Their format should not need to be changed.
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
    # get a list of all the layers
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
    tries=4
    while try_num<tries:
        try:
            # try to delete the cache
            logging.info(headers)
            logging.info(apiUrl)
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

'''
FUNCTIONS FOR THIS DATASET

The functions below have been tailored to this specific dataset.
They should all be checked because their format likely will need to be changed.
'''
def getCollectionName(var):
    '''
    get GEE collection name
    INPUT   var: variable to be used in asset name (string)
    RETURN  GEE collection name for input date (string)
    '''
    return EE_COLLECTION_GEN.format(var=var)

def getAssetName(date, product):
     '''
     get asset name
     INPUT   date: date in the format of the DATE_FORMAT variable (string)
             product: the product of which the data is (string)
     RETURN  GEE asset name for input date (string) and product (string)
     '''
     return '/'.join([getCollectionName(product), FILENAME.format(var=product, date=date)])

def getDate(filename):
     '''
     get date from asset name (last 8 characters of filename after removing extension)
     INPUT   filename: file name that ends in a date of the format YYYYMMDD (string)
     RETURN  existing_dates: dates in the format YYYYMMDD (string)
     '''
     existing_dates = os.path.splitext(os.path.basename(filename))[0][-8:]

     return existing_dates

def find_latest_date():
    '''
    Fetch the latest date for which coral bleach monitoring data is available and store it in the data dictionary
    '''
    ftp = ftplib.FTP('ftp.hermes.acri.fr')
    ftp.login(ftp_username, ftp_password)
    
    for product, val in DATA_DICT.items():
        date = ''
        ftp.cwd('/GLOB/olcib/{}'.format(val['interval']))
        for i in range(3):
            list_dates = list(ftp.nlst())
            list_dates.sort()
            date = ''.join([date, list_dates[-1]])
            ftp.cwd(list_dates[-1])
        val['latest date'] = date 
        file = [x for x in list(ftp.nlst()) if ('L3m' in x and '.nc' in x and 'GLOB_4_AV-OLB_TSM' in x)][0]
        val['url'] = SOURCE_URL.format(ftp_username,ftp_password,'/'.join([ftp.pwd(), file]))
        ftp.cwd('/')
        
def fetch(product):
     '''
     Fetch latest netcdef files by using the url from the global dictionary
     INPUT   product: the product of which to fetch data (string)
     '''
     logging.info('Downloading raw data')
     # go through each item in the parent dictionary
     url = DATA_DICT[product]['url']
     # create a path under which to save the downloaded file
     raw_data_file = os.path.join(DATA_DIR,os.path.basename(url))
     try:
         # try to download the data
         urllib.request.urlretrieve(url, raw_data_file)
         # if successful, add the file to a new key in the parent dictionary
         DATA_DICT[product]['raw_data_file'] = raw_data_file
     except Exception as e:
         # if unsuccessful, log an error that the file was not downloaded
         logging.error('Unable to retrieve data from {}'.format(url))
         logging.debug(e)

def processNewData():
    '''
    fetch, process, upload, and clean new data
    INPUT   existing_dates: list of dates we already have in GEE (list of strings)
    RETURN  asset: file name for asset that have been uploaded to GEE (string)
    '''
    # Get latest available date that is availble on the source
    find_latest_date()
    # loop through the items in the data dictionary
    for product, val in DATA_DICT.items():
        # if the latest available data does not exist in the image collection on GEE 
        if val['latest date'] not in val['existing dates']:
            # fetch files for the latest date
            logging.info('Fetching files')   
            fetch(product)
            # convert netcdfs to tifs and store the tif filenames to a new key in the parent dictionary
            logging.info('Extracting relevant GeoTIFFs from source NetCDFs')
            # file path to the netcdf file
            nc = val['raw_data_file'] 
            # the name of the layer in netcdf that is being converted to GEOTIFF 
            sds = val['sds'][0]

            # should be of the format 'NETCDF:"filename.nc":variable'
            sds_path = f'NETCDF:"{nc}":{sds}'
            # generate a name to save the tif file we will translate the netcdf file's subdataset into
            sds_tif = '{}_{}.tif'.format(os.path.splitext(nc)[0], sds_path.split(':')[-1])
            # create the gdal command and run it to convert the netcdf to tif
            cmd = ['gdal_translate','-q', '-a_srs', 'EPSG:4326',  sds_path, sds_tif]
            completed_process = subprocess.run(cmd, shell=False)
            logging.debug(str(completed_process))
            # store the file path to the tif file in the data dictionary
            val['tif'] = sds_tif

            logging.info('Uploading files')
            # Generate a name we want to use for the asset once we upload the file to GEE
            asset = getAssetName(val['latest date'], product)
            # Upload new file (tif) to GEE
            eeUtil.uploadAsset(sds_tif, asset, GS_FOLDER, timeout=1000)
            # store the name of the uploaded asset to the dictionary
            val['asset'] = asset[1:]
            logging.info('{} uploaded to GEE'.format(val['asset']))
            
        else:
            logging.info('Data for {} already up to date'.format(product))
            # if no new assets, assign empty lists to the key 'tif' and 'asset' in the data dictionary
            val['tif'] = []
            val['asset'] = []

def checkCreateCollection():
    '''
    List assets in collection if it exists, else create new collection
    '''
    # Check if folder to store GEE collections exists. If not, create it.
        # we will make one collection per product, all stored in the parent folder for the dataset

    # if the parent folder does not exist yet, create it on gee
    if not eeUtil.exists(COLLECTION):
        logging.info('{} does not exist, creating'.format(COLLECTION))
        eeUtil.createFolder(COLLECTION)

    # loop through each product that we want to pull
    for product, val in DATA_DICT.items():
        # fetch the name of the collection storing this product 
        collection = getCollectionName(product)

        # If the GEE collection for a particular product exists
        if eeUtil.exists(collection):
            existing_assets = eeUtil.ls(collection)
            # get a list of the dates from these existing assets
            dates = [getDate(a) for a in existing_assets]
            # append the dates as a list as the value of the key 'existing dates' in the data dictionary
            val['existing dates'] = dates 

        # If the GEE collection does not exist, add an empty list as the value of the key 'existing dates' in the data dictionary
        else:
            # add an empty list as the value of the key 'existing dates'
            val['existing dates'] = []
            # create a collection for this product
            logging.info('{} does not exist, creating'.format(collection))
            eeUtil.createFolder(collection, True)

def deleteExcessAssets(product, dates, max_assets):
    '''
    Delete oldest assets, if more than specified in max_assets variable
    INPUT   product: the product of which the data is (string)
            dates: dates for all the assets currently in the GEE collection; dates should be in the format specified
                    in DATE_FORMAT variable (list of strings)
            max_assets: maximum number of assets allowed in the collection (int)
    '''
    # sort the list of dates so that the oldest is first
    dates.sort()
    # if we have more dates of data than allowed
    if len(dates) > max_assets:
        # go through each date, starting with the oldest, and delete until we only have the max number of assets left
        for date in dates[:-max_assets]:
            eeUtil.removeAsset(getAssetName(date, product))

def get_most_recent_date():
    '''
    Get most recent date we have assets for
    RETURN  most_recent_date: most recent date in GEE collection (datetime)
    '''
    # update the 'existing dates' values in the data dictionary
    checkCreateCollection()
    # get list of assets in collection
    existing_dates =  [y for lst in [x['existing dates'] for x in DATA_DICT.values()] for y in lst]
    # sort these dates oldest to newest
    existing_dates.sort()
    # get the most recent date (last in the list) and turn it into a datetime
    most_recent_date = datetime.datetime.strptime(existing_dates[-1], DATE_FORMAT)

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

def create_headers():
    '''
    Create headers to perform authorized actions on API

    '''
    return {
        'Content-Type': "application/json",
        'Authorization': "{}".format(os.getenv('apiToken')),
    }

def update_layer(layer, new_date):
    '''
    Update layers in Resource Watch back office.
    INPUT  layer: layer that will be updated (string)
           new_date: the time period of the data (string)
    '''
    # get previous date being used from
    old_date_text = layer['attributes']['name'].replace(' Total Suspended Matter Concentration (g/m³)', '')

    # convert new datetimes to string
    new_date_start = datetime.datetime.strptime(new_date.split('-')[0], DATE_FORMAT)
    new_date_end = datetime.datetime.strptime(new_date.split('-')[1], DATE_FORMAT)
    new_date_text = '-'.join([new_date_start.strftime("%B %d, %Y"), new_date_end.strftime("%B %d, %Y")])

    # replace date in layer's title with new date range
    layer['attributes']['name'] = layer['attributes']['name'].replace(old_date_text, new_date_text)

    # store the current asset id used in the layer 
    old_asset = layer['attributes']['layerConfig']['assetId']

    # find the asset id of the latest image 
    product = [key for key in list(DATA_DICT.keys()) if key in old_asset][0]
    new_asset = DATA_DICT[product]['asset']
    # replace the asset id in the layer def with new asset id
    layer['attributes']['layerConfig']['assetId'] = new_asset

    # replace the asset id in the interaction config with new asset id
    layer['attributes']['interactionConfig']['config']['url'] = layer['attributes']['interactionConfig']['config']['url'].replace(old_asset,new_asset)

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

def updateResourceWatch():
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date', flushing the tile cache, and updating any dates on layers
    '''
    # Get the most recent date from the data in the GEE collection
    most_recent_date = get_most_recent_date()
    # Get the current 'last update date' from the dataset on Resource Watch
    current_date = getLastUpdate(DATASET_ID)

    # If the most recent date from the GEE collection does not match the 'last update date' on the RW API, update it
    if current_date != most_recent_date:
        logging.info('Updating last update date and flushing cache.')
        # Update dataset's last update date on Resource Watch
        lastUpdateDate(DATASET_ID, most_recent_date)
       
        # pull dictionary of current layers from API
        layer_dict = pull_layers_from_API(DATASET_ID)
        
        for product, val in DATA_DICT.items():
            if val['asset']:
                layer_product = [x for x in layer_dict if product in x['attributes']['layerConfig']['assetId']]
                layer_date = os.path.basename(val['url'])[4:21]
                # go through each layer, pull the definition and update
                for layer in layer_product:
                    # update layer name, asset id, and interaction configuration 
                    update_layer(layer, layer_date)
            
         # get layer ids and flush tile cache for each
        layer_ids = getLayerIDs(DATASET_ID)
        for layer_id in layer_ids:
            flushTileCache(layer_id)
    else:
        logging.info('Data on Resource Watch up to date!')
        

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Initialize eeUtil
    eeUtil.initJson()

    # clear the GEE collection, if specified above
    if CLEAR_COLLECTION_FIRST:
        if eeUtil.exists(COLLECTION):
            eeUtil.removeAsset(COLLECTION, recursive=True)

    # Check if collection exists, create it if it does not
    # If it exists add the list of existing dates of each product to the data dictionary
    checkCreateCollection()

    # Fetch, process, and upload the new data
    os.chdir(DATA_DIR)
    processNewData()
    for product, val in DATA_DICT.items():
        logging.info('Previous assets for product {}: {}, new: {}, max: {}'.format(
            product, len(val['existing dates']), val['asset'], MAX_ASSETS))

        # Delete excess assets
        deleteExcessAssets(product, val['existing dates'] + [val['latest date']], MAX_ASSETS)

    # Update Resource Watch
    updateResourceWatch()

    logging.info('SUCCESS')
