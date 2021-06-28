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
from osgeo import gdal
import shlex
import re

DATA_DICT = OrderedDict()

DATA_DICT['daily'] = {
        'sds': [
            'no3', 'po4','o2'
        ],
        'x': ['a','b','c'],
        'interval': 'daily',
        'original_nodata': -9.96920996838686905e+36,
        'missing_data': [
            -9.96920996838686905e+36,
        ],
        'pyramiding_policy': 'MEAN',
        'date_format': '%Y%m%d',
        'existing dates':[],
        'tif': [],
        'asset':[],
    }

DATA_DICT['monthly'] = {
        'sds': [
            'no3', 'po4','o2'
        ],
        'x': ['a','b','c'],
        'interval': 'monthly',
        'original_nodata': -9.96920996838686905e+36,
        'missing_data': [
            -9.96920996838686905e+36,
        ],
        'pyramiding_policy': 'MEAN',
        'date_format': '%Y%m',
        'existing dates':[],
        'tif': [],
        'asset':[],
    }


# filename format for GEE
FILENAME = 'ocn_020{x}_nutrient_concentration_{var}_{interval}_{date}'

# name of data directory in Docker container
DATA_DIR = os.path.join(os.getcwd(),'data')

# name of collection in GEE where we will upload the final data
COLLECTION = '/projects/resource-watch-gee/ocn_020_nrt_rw0_nutrient_concentration'

# generate generic string that can be formatted to name each product's GEE collection
EE_COLLECTION_GEN = COLLECTION + '/{var}_concentration'

# name of folder to store data in Google Cloud Storage
GS_FOLDER = COLLECTION[1:]

# do you want to delete everything currently in the GEE collection when you run this script?
CLEAR_COLLECTION_FIRST = False

# how many assets can be stored in the GEE collection before the oldest ones are deleted?
MAX_ASSETS = 3

# url from which the data is downloaded 
SOURCE_URL = 'ftp://{}:{}@nrt.cmems-du.eu{}'

DATE_STR = re.compile(r'2[0-9][0-9][0-9]*')

TODAY_DATE = datetime.date.today().strftime('%Y%m%d') 
TODAY_YEAR = datetime.date.today().strftime('%Y')
TODAY_MONTH = datetime.date.today().strftime('%m')
TODAY_DAY = datetime.date.today().strftime('%d')


# username and password for the ftp service to download data 
ftp_username = os.environ.get('CMEMS_USERNAME')
ftp_password = os.environ.get('CMEMS_PASSWORD')

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_IDS= {'no3': '92327c78-a473-402b-8edf-409869823216', 'po4': 'f1aa9ec7-c3b6-441c-b395-96fc796b7612', 'o2': '877cdf39-5536-409c-bcba-2220e1b72796'}

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
    if lastUpdateString == None:
        return None 
    else:
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
def getCollectionName(val,n):
    '''
    get GEE collection name
    INPUT   val: variable to be used in asset name (string)
            n: index for that variable in the data dictionary (string)
            interval: the interval of the data (string)
    RETURN  GEE collection name for input date (string)
    '''
    return EE_COLLECTION_GEN.format(var=val['sds'][n])

def getAssetName(n, val, date):
     '''
     get asset name
     INPUT   date: date in the format of the DATE_FORMAT variable (string)
             product: the product of which the data is (string)
     RETURN  GEE asset name for input date (string) and product (string)
     '''
     return '/'.join([getCollectionName(val, n), FILENAME.format(x=val['x'][n], var= val['sds'][n], interval=val['interval'], date=date)])

def getDate(filename):
     '''
     get date from asset name (last 8 characters of filename after removing extension)
     INPUT   filename: file name that ends in a date of the format YYYYMMDD (string)
     RETURN  existing_dates: dates in the format YYYYMMDD (string)
     '''
     
     existing_dates = DATE_STR.search(filename).group()

     return existing_dates

def find_latest_date(val):
    '''
    Fetch the latest date for which coral bleach monitoring data is available and store it in the data dictionary
    '''
    ftp = ftplib.FTP('nrt.cmems-du.eu')
    ftp.login(ftp_username, ftp_password)
    
    if val['interval'] == 'monthly':
        ftp.cwd('/Core/GLOBAL_ANALYSIS_FORECAST_BIO_001_028/global-analysis-forecast-bio-001-028-{}/{}'.format(val['interval'], TODAY_YEAR))
        list_files = list(ftp.nlst())
        list_files.sort()
        file = list_files[-1]
    else:
        ftp.cwd('/Core/GLOBAL_ANALYSIS_FORECAST_BIO_001_028/global-analysis-forecast-bio-001-028-{}/{}/{}'.format(val['interval'], TODAY_YEAR, TODAY_MONTH))
        list_files = list(ftp.nlst())
        file = [x for x in list_files if TODAY_DATE in x][0]
    date = DATE_STR.search(file).group()
    val['latest date'] = date 
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
         # urllib.request.urlretrieve(url, raw_data_file) 
         # if successful, add the file to a new key in the parent dictionary
         DATA_DICT[product]['raw_data_file'] = raw_data_file
     except Exception as e:
         # if unsuccessful, log an error that the file was not downloaded
         logging.error('Unable to retrieve data from {}'.format(url))
         logging.debug(e)

def processNewData():
    '''
    fetch, process, upload, and clean new data
    INPUT   existing dates: list of dates we already have in GEE (list of strings)
    RETURN  asset: file name for asset that have been uploaded to GEE (string)
    '''
   
    # loop through the items in the data dictionary
    for product, val in DATA_DICT.items():
        # Get latest available date that is availble on the source
        find_latest_date(val)
        # fetch files for the latest date
        logging.info('Fetching files')  
        fetch(product)
        # file path to the netcdf file
        nc = val['raw_data_file'] 
        # if the latest available data does not exist in the image collection on GEE 
        if val['latest date'] not in val['existing dates']:
            # convert netcdfs to tifs and store the tif filenames to a new key in the parent dictionary
            logging.info('Extracting relevant GeoTIFFs from source NetCDFs')
            for i in range(len(val['sds'])):
                # the name of the layer in netcdf that is being processed 
                sds = val['sds'][i] 
                # should be of the format 'NETCDF:"filename.nc":variable'
                sds_path = f'NETCDF:"{nc}":{sds}'
                # generate a name to save the raw tif file we will translate the netcdf file's subdataset into
                raw_sds_tif = '{}_{}.tif'.format(os.path.splitext(nc)[0], sds_path.split(':')[-1])
                # create the gdal command and run it to convert the netcdf to tif
                cmd = ['gdal_translate','-q', '-a_srs', 'EPSG:4326',  sds_path, raw_sds_tif, '-b', '1', '-b','2', '-b','3', '-b', '4', '-b', '5']
                #completed_process = subprocess.run(cmd, shell=False)
                #logging.debug(str(completed_process))
                # generate a name to save the processed tif file
                processed_sds_tif = '{}_{}_edit.tif'.format(os.path.splitext(nc)[0], sds_path.split(':')[-1])
                # create the gdal command and run it to average pixel values
                cmd = 'gdal_calc.py -A ' + raw_sds_tif +' -B ' + raw_sds_tif + ' -C ' + raw_sds_tif + ' -D ' + raw_sds_tif +' -E ' + raw_sds_tif + ' --A_band=1 --B_band=2 --C_band=3 --D_band=4 --E_band=5 --outfile=' + processed_sds_tif + ' --calc="numpy.average((A,B,C,D,E), axis = 0)" --NoDataValue=-9.96920996838686905e+36'
                # format to command line
                posix_cmd = shlex.split(cmd, posix=True)
                #completed_process= subprocess.check_call(posix_cmd)   
                #logging.debug(str(completed_process))
                # store the file path to the tif file in the data dictionary
                val['tif'].append(processed_sds_tif)
                logging.info('Uploading files')
                # Generate a name we want to use for the asset once we upload the file to GEE
                asset = getAssetName(i, val, val['latest date'])
                # Upload new file (tif) to GEE
                #eeUtil.uploadAsset(processed_sds_tif, asset, GS_FOLDER, timeout=1000)
                # store the name of the uploaded asset to the dictionary
                val['asset'].append(asset[1:])
                logging.info('{} uploaded to GEE'.format(val['asset'][i]))
            
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

    for product, val in DATA_DICT.items():
        for i in range(len(val['sds'])):
            # fetch the name of the collection storing this product 
            collection = getCollectionName(val,i)

            # If the GEE collection for a particular product exists
            if eeUtil.exists(collection):
                existing_assets = eeUtil.ls(collection)
                # get a list of the dates from these existing assets
                dates = [getDate(a) for a in existing_assets if product in a]
                # append the dates as a list as the value of the key 'existing dates' in the data dictionary
                for date in dates:
                    if date not in val['existing dates']:
                        val['existing dates'].extend(dates) 
                

            # If the GEE collection does not exist, add an empty list as the value of the key 'existing dates' in the data dictionary
            else:
                # create a collection for this product
                logging.info('{} does not exist, creating'.format(collection))
                eeUtil.createFolder(collection, True)

def deleteExcessAssets(val, dates, max_assets):
    '''
    Delete oldest assets, if more than specified in max_assets variable
    INPUT   product: the product of which the data is (string)
            dates: dates for all the assets currently in the GEE collection; dates should be in the format specified
                    in DATE_FORMAT variable (list of strings)
            max_assets: maximum number of assets allowed in the collection (int)
    '''
    # if we have more dates of data than allowed
    if len(dates) > max_assets:
        # go through each date, starting with the oldest, and delete until we only have the max number of assets left
        for date in dates[:-max_assets]:
            for i in range(len(val['sds'])):
                eeUtil.removeAsset(getAssetName(i, val, date))

def get_most_recent_date(val):
    '''
    Get most recent date we have assets for
    RETURN  most_recent_date: most recent date in GEE collection (datetime)
    '''
    # update the 'existing dates' values in the data dictionary
    checkCreateCollection()
    # get list of assets in collection
    existing_dates = val['existing dates']
    # sort these dates oldest to newest
    existing_dates.sort()
    # get the most recent date (last in the list) and turn it into a datetime
    format = val['date_format']
    date = existing_dates[-1]
    most_recent_date = datetime.datetime.strptime(existing_dates[-1], val['date_format'])

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

def update_layer(layer, new_date, date_format, id):
    '''
    Update layers in Resource Watch back office.
    INPUT  layer: layer that will be updated (string)
           new_date: the time period of the data (string)
    '''
    # get previous date being used from
    text = re.compile(r' Mole Concentration of [\w,\W]*')
    old_date_text = text.sub("", layer['attributes']['name'])

    # convert new datetimes to string
    new_date = datetime.datetime.strptime(new_date, date_format)
    if date_format == '%Y%m':
        new_date_text = new_date.strftime("%B %Y") 
    elif date_format == '%Y%m%d':
        new_date_text = new_date.strftime("%B %d, %Y")
    
    # replace date in layer's title with new date range
    layer['attributes']['name'] = layer['attributes']['name'].replace(old_date_text, new_date_text)

    # store the current asset id used in the layer 
    old_asset = layer['attributes']['layerConfig']['assetId']

    # find the asset id of the latest image 
    product = [key for key in list(DATA_DICT.keys()) if key in old_asset][0]
    new_asset = [asset for asset in DATA_DICT[product]['asset'] if id in asset][0]
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
    if r.ok or r.status_code==504 or r.status_code==200:
        logging.info('Layer replaced: {}'.format(layer['id']))
    else:
        logging.error('Error replacing layer: {} ({})'.format(layer['id'], r.status_code))

def updateResourceWatch():
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date', flushing the tile cache, and updating any dates on layers
    '''
    # Get the most recent date from the data in the GEE collection
    
    for var, id in DATASET_IDS.items():
        # Get the current 'last update date' from the dataset on Resource Watch
        current_date = None #getLastUpdate(id) 
        for product, val in DATA_DICT.items():
            most_recent_date = get_most_recent_date(val)
            # If the most recent date from the GEE collection does not match the 'last update date' on the RW API, update it
            if current_date != most_recent_date:
                logging.info(('Updating last update date for {} (dataset ID = {}) and flushing cache.').format(var, id)) 
                # Update dataset's last update date on Resource Watch
                lastUpdateDate(id, most_recent_date)
       
                # pull dictionary of current layers from API
                layer_dict = pull_layers_from_API(id)
        
                layer_product = [x for x in layer_dict if product in x['attributes']['layerConfig']['assetId']]
                layer_date = val['latest date']
                # go through each layer, pull the definition and update
                for layer in layer_product:
                    # update layer name, asset id, and interaction configuration 
                    update_layer(layer, layer_date, val['date_format'],var)
                
                # get layer ids and flush tile cache for each
                layer_ids = getLayerIDs(id)
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
        latest_date = val['latest date']
        existing_dates = [date for date in val['existing dates']]
        if latest_date not in existing_dates:
            existing_dates.append(latest_date)
        # sort the list of dates so that the oldest is first
        existing_dates.sort()
        deleteExcessAssets(val, existing_dates, MAX_ASSETS)

    # Update Resource Watch
    updateResourceWatch()

    logging.info('SUCCESS')

main()
