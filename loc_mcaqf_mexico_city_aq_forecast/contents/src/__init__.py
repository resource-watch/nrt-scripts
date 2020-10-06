from __future__ import unicode_literals
import os
import sys
import datetime
from datetime import timedelta
import logging
import ee
import eeUtil
import time
import requests
import rasterio
import boto3
from botocore.exceptions import NoCredentialsError
from netCDF4 import Dataset
import numpy as np
import copy
import json
from multiprocessing.dummy import Pool

# set up boto3 client with AWS credentials
S3 = boto3.client('s3', aws_access_key_id=os.getenv('S3_ACCESS_KEY'), aws_secret_access_key=os.getenv('S3_SECRET_KEY'))
# bucket on S3 where data is located
S3_BUCKET = 'rw-mexico-city-aq'
# unformatted filename for mexico city AQ data
S3_FILENAME = '{compound}_{date}.06_D4_CB05.nc'

# number of timesteps in source netcdf file
NUM_TIMESTEPS = 48

# filename format for GEE
FILENAME = 'loc_mcaqf_mexico_city_aq_forecast_{compound}_{date}'

# nodata value for netcdf
# this netcdf has a nodata value of -5
# GEE can't accept a negative no data value, set to 251 for Byte type?
NODATA_VALUE = None

# name of data directory in Docker container
DATA_DIR = 'data'

# name of folder to store data in Google Cloud Storage
GS_FOLDER = 'loc_mcaqf_mexico_city_aq_forecast'

# name of collection in GEE where we will upload the final data
COLLECTION = '/projects/resource-watch-gee/loc_mcaqf_mexico_city_aq_forecast'

# generate name for dataset's parent folder on GEE which will be used to store
# several collections - one collection per variable
PARENT_FOLDER = COLLECTION

# generate generic string that can be formatted to name each variable's GEE collection
EE_COLLECTION_GEN = PARENT_FOLDER + '/{var}'

# do you want to delete everything currently in the GEE collection when you run this script?
CLEAR_COLLECTION_FIRST = False

# how many dates can be stored in the GEE collection before the oldest ones are deleted?
MAX_DATES = 96

# format of date used in the source data files
DATE_FORMAT = '%Y%m%d'
# format of date used in GEE assets
DATE_FORMAT_GEE = '%Y%m%d_%H'

# Resource Watch dataset API IDs
# Important! Before testing this script:
# Please change these IDs OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on different datasets on Resource Watch
DATASET_IDS = {
    'CO':'e39f5910-a9b8-4ef1-b4b4-f6b141b15541',
    'NO2':'918ba6bc-69ed-44fb-9b29-5fb445fdfef6',
    'O3':'00d6bae1-e105-4165-8230-ee73a8128538',
    'PM25':'7a34b770-83f9-4c6a-acb8-31edcff7241e',
    'SO2': '59790e64-d95d-43fa-a124-5c7eb1cb4456',
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
    apiUrl = f'http://api.resourcewatch.org/v1/dataset/{dataset}'
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
    apiUrl = f'http://api.resourcewatch.org/v1/dataset/{dataset}?includes=layer'
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

def flushTileCache_future(layer_id):
    """
    Given the API ID for a GEE layer on Resource Watch,
    this function will generate the inputs for the 
    requests.delete function to clear the layer cache.
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

    # generate request.delete arguments to clear cache for this layer
    return {'url': apiUrl, 'headers': headers}

'''
FUNCTIONS FOR THIS DATASET

The functions below have been tailored to this specific dataset.
They should all be checked because their format likely will need to be changed.
'''
def clearCollectionMultiVar():
    '''
    Clear the GEE collection for all variables
    '''
    logging.info('Clearing collections.')
    for var in DATASET_IDS.keys():
        # get name of GEE collection for variable
        collection = getCollectionName(var)
        # if the collection exists,
        if eeUtil.exists(collection):
            # remove the / from the beginning of the collection name to be used in ee module
            if collection[0] == '/':
                collection = collection[1:]
            # pull the image collection
            a = ee.ImageCollection(collection)
            # check how many assets are in the collection
            collection_size = a.size().getInfo()
            # if there are assets in the collection
            if collection_size > 0:
                # create a list of assets in the collection
                list = a.toList(collection_size)
                # delete each asset
                for item in list.getInfo():
                    ee.data.deleteAsset(item['id'])

def getCollectionName(var):
    '''
    get GEE collection name
    INPUT   var: variable to be used in asset name (string)
    RETURN  GEE collection name for input date (string)
    '''
    return EE_COLLECTION_GEN.format(var=var)

def getSourceFilename(date, compound):
    '''
    format source filename with date
    INPUT   date: date in the format YYYYMMDD (string)
            compound: compound we are downloading data for (string)
    RETURN  source url to download data, formatted for the input date and compound(string)
    '''
    return S3_FILENAME.format(compound=compound, date=date)

def getAssetName(tif):
    '''
    get asset name
    INPUT   tif: name of tif file (string)
    RETURN  GEE asset name for input date (string)
    '''
    var = tif.split('_')[-3]
    return EE_COLLECTION_GEN.format(var=var)+'/'+os.path.splitext(os.path.basename(tif))[0]

def getFilename(date, compound):
    '''
    get netcdf filename to save source file as
    INPUT   date: date in the format of the DATE_FORMAT variable (string)
            compound: compound we are downloading data for (string)
    RETURN  file name to save netcdf from source under (string)
    '''
    return os.path.join(DATA_DIR, '{}.nc'.format(FILENAME.format(date=date, compound=compound)))

def getDate(filename):
    '''
    get date from filename (last 8 characters of filename after removing extension)
    INPUT   filename: file name that ends in a date of the format YYYYMMDD_H (string)
    RETURN  date in the format YYYYMMDD_H (string)
    '''
    # get base of filename
    base = os.path.splitext(os.path.basename(filename))[0]
    return base.split('_')[-2]+'_'+base.split('_')[-1]

def getNewDates(existing_dates):
    '''
    Get new dates we want to try to fetch data for
    INPUT   existing_dates: list of dates that we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
    RETURN  new_dates: list of new dates we want to try to get, in the format of the DATE_FORMAT variable (list of strings)
    '''
    # create empty list to store dates we want to fetch
    new_dates = []
    # start with today's date
    date = datetime.date.today()
    for i in range(int(MAX_DATES/NUM_TIMESTEPS+2)):
        # generate a string from the date
        datestr = date.strftime(DATE_FORMAT)
        # if the date string is not the list of dates we already have, add it to the list of new dates to try and fetch
        if datestr not in existing_dates:
            new_dates.append(datestr)
        # go back one day at a time
        date -= datetime.timedelta(days=1)
    return new_dates

def convert(files):
    '''
    Convert netcdf files to tifs
    INPUT   files: list of file names for netcdfs that have already been downloaded (list of strings)
    RETURN  tifs: list of file names for tifs that have been generated (list of strings)
    '''

    # create and empty list to store the names of the tifs we generate
    tifs = []

    #go through each netcdf file and translate
    for f in files:
        # load the netcdf file
        nc = Dataset(f)
        # get the name of the variable in this netcdf
        var =f.split('_')[-2]
        for date_ix in range(NUM_TIMESTEPS):
            # generate a name to save the tif file we will translate the netcdf file into
            tif = '{}_{}.tif'.format(os.path.splitext(f)[0], str(date_ix).zfill(2))
            # tranlate the netcdf into a tif
            logging.debug('Converting {} to {}'.format(f, tif))
            # Extract data
            data = nc[var][date_ix,0,:,:]
            # Create profile/tif metadata
            south_lat = nc['LAT'][0][0].min()
            north_lat = nc['LAT'][0][0].max()
            west_lon = nc['LON'][0][0].min()
            east_lon = nc['LON'][0][0].max()

            # pull the extent of the dataset and generate its transform
            extent = [west_lon, north_lat, east_lon, south_lat]
            transform = rasterio.transform.from_bounds(*extent, data.shape[1], data.shape[0])

            # Profile
            profile = {
                'driver':'GTiff',
                'height':data.shape[0],
                'width':data.shape[1],
                'count':1,
                'dtype':rasterio.float32,
                'crs':'EPSG:4326',
                'transform':transform,
                'compress':'lzw',
                'nodata':NODATA_VALUE
            }
            with rasterio.open(tif, 'w', **profile) as dst:
                dst.write(data.astype(rasterio.float32), indexes=1)
            # add the new tif files to the list of tifs
            tifs.append(tif)
    return tifs

def aws_download(s3_filename, local_file):
    '''
    Upload original data and processed data to Amazon S3 storage
    INPUT   s3_filename: filname of data in AWS bucket (string)
            local_file: filename we want to save the file under locally (string)
    '''
    try:
        S3.download_file(S3_BUCKET, s3_filename, local_file)
        logging.info("AWS download successful: http://{}.s3.amazonaws.com/{}".format(S3_BUCKET, s3_filename))
        return True
    except FileNotFoundError:
        logging.error("aws_download - file was not found: + local_file.")
        return False
    except NoCredentialsError:
        logging.error("aws_download - credentials not available.")
        return False

def fetch(dates, var):
    '''
    Fetch files by datestamp
    INPUT   dates: list of dates we want to try to fetch, in the format YYYYMMDD (list of strings)
            var: variable to fetch files for (string)
    RETURN  files: list of file names for netcdfs that have been downloaded (list of strings)
    '''
    # make an empty list to store names of the files we downloaded
    files = []
    # go through each input date
    for date in dates:
        # get the url to download the file from the source for the given date/compound
        s3_filename = getSourceFilename(date, var)
        # get the filename we want to save the file under locally
        f = getFilename(date, var)
        logging.debug('Fetching {}'.format(s3_filename))
        try:
            # try to download the data
            aws_download(s3_filename, f)
            # if successful, add the file to the list of files we have downloaded
            files.append(f)
        except Exception as e:
            # if unsuccessful, log that the file was not downloaded
            # (could be because we are attempting to download a file that is not available yet)
            logging.info('Could not fetch {}'.format(s3_filename))
            logging.info(e)
    return files


def processNewData(existing_dates):
    '''
    fetch, process, upload, and clean new data
    INPUT   existing_dates: list of dates we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
    RETURN  new_assets_all_var: list of file names for netcdfs that have been downloaded (list of strings)
    '''
    # Get list of new dates we want to try to fetch data for
    new_dates = getNewDates(existing_dates)
    # create a list to store all the new assets in
    new_assets_all_var = []
    for var in DATASET_IDS.keys():
        # Fetch new files
        logging.info('Fetching files')
        files = fetch(new_dates, var)
    
        # If we have successfully been able to fetch new data files
        if files:
            # Convert new files from netcdf to tif files
            logging.info('Converting files to tifs')
            tifs = convert(files)
    
            logging.info('Uploading files')
            # Get a list of the dates we have to upload from the tif file names
            dates = [getDate(tif) for tif in tifs]
            # Get a list of datetimes from these dates for each of the dates we are uploading
            datestamps = [datetime.datetime.strptime(date.split('_')[0], DATE_FORMAT)+timedelta(hours=int(date.split('_')[1])) for date in dates]
            # Get a list of the names we want to use for the assets once we upload the files to GEE
            new_assets = [getAssetName(tif) for tif in tifs]
            try:
                # Upload new files (tifs) to GEE
                eeUtil.uploadAssets(tifs, new_assets, GS_FOLDER, datestamps, timeout=900)
            except:
                # add uploaded assets to final list of assets uploaded
                new_assets_all_var += new_assets
                # Delete local files
                logging.info('Cleaning local files')
                for tif in tifs:
                    os.remove(tif)
                for f in files:
                    os.remove(f)
    return new_assets_all_var

def checkCreateCollection(vars):
    '''
    List assets in collection if it exists, else create new collection
    INPUT   vars: list variables (as named in netcdf) that we want to check collections for (list of strings)
    RETURN  existing_dates_all_vars: list of dates, in the format of the DATE_FORMAT variable, that exist for all variable collections in GEE (list of strings)
            existing_dates_by_var: list of dates, in the format of the DATE_FORMAT variable, that exist for each individual variable collection in GEE (list containing list of strings for each variable)
    '''
    # create a master list (not variable-specific) to store the dates for which all variables already have data for
    existing_dates = []
    # create an empty list to store the dates that we currently have for each AQ variable
    # will be used in case the previous script run crashed before completing the data upload for every variable.
    existing_dates_by_var = []
    # loop through each variables that we want to pull
    for var in vars:
        # For one of the variables, get the date of the most recent dataset
        # All variables come from the same file
        # If we have one for a particular data, we should have them all
        collection = getCollectionName(var)

        # Check if folder to store GEE collections exists. If not, create it.
        # we will make one collection per variable, all stored in the parent folder for the dataset
        if not eeUtil.exists(PARENT_FOLDER):
            logging.info('{} does not exist, creating'.format(PARENT_FOLDER))
            eeUtil.createFolder(PARENT_FOLDER)

        # If the GEE collection for a particular variable exists, get a list of existing assets
        if eeUtil.exists(collection):
            existing_assets = eeUtil.ls(collection)
            # get a list of the dates from these existing assets
            dates = [getDate(a).split('_')[-2] for a in existing_assets]
            # append this list of dates to our list of dates by variable
            existing_dates_by_var.append(dates)

            # for each of the dates that we have for this variable, append the date to the master
            # list of which dates we already have data for (if it isn't already in the list)
            for date in dates:
                if date not in existing_dates:
                    existing_dates.append(date)
        # If the GEE collection does not exist, append an empty list to our list of dates by variable
        else:
            existing_dates_by_var.append([])
            # create a collection for this variable
            logging.info('{} does not exist, creating'.format(collection))
            eeUtil.createFolder(collection, True)

    '''
     We want make sure all variables correctly uploaded the data on the last run. To do this, we will
     check that we have the correct number of appearances of the data in our GEE collection. If we do
     not, we will want to re-upload this date's data.
    '''
    # Create a copy of the master list of dates that will store the dates that were properly uploaded for all variables.
    existing_dates_all_vars = copy.copy(existing_dates)
    for date in existing_dates:
        # check how many times each date appears in our list of dates by variable
        date_count = sum(x.count(date) for x in existing_dates_by_var)
        # divide this count by the number of time intervals we have (because the date will be
        # repeated for each time)
        count = date_count / 48
        # If this count is less than the number of variables we have, one of the variables did not finish
        # uploading for this date, and we need to re-upload this file.
        if count < len(vars):
            # remove this from the list of existing dates for all variables
            existing_dates_all_vars.remove(date)
    return existing_dates_all_vars, existing_dates_by_var

def deleteExcessAssets(dates, max_dates):
    '''
    Delete oldest assets, if more than specified in max_assets variable
    INPUT   dates: dates for all the assets currently in the GEE collection; dates should be in the format specified
                    in DATE_FORMAT variable (list of strings)
            max_dates: maximum number of dates allowed in the collection (int)
    '''
    # sort the list of dates so that the oldest is first
    dates.sort()
    # if we have more dates of data than allowed,
    if len(dates) > max_dates/NUM_TIMESTEPS:
        # go through each date, starting with the oldest, and delete until we only have the max number of assets left
        for date in dates[:-int(max_dates/NUM_TIMESTEPS)]:
            # delete assets and S3 files for every compound
            for compound in DATASET_IDS.keys():
                # delete S3 files
                logging.info('Deleting {} from S3'.format(getSourceFilename(date, compound)))
                S3.delete_object(Bucket=S3_BUCKET, Key=getSourceFilename(date, compound))
                # delete assets for every timestep
                for date_ix in range(NUM_TIMESTEPS):
                    asset = getAssetName(getFilename(date, compound).split('.')[0]+'_' +str(date_ix).zfill(2))
                    logging.info('Deleting {} from GEE'.format(asset))
                    eeUtil.removeAsset(asset)

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

def update_layer(var, layer, most_recent_date):
    '''
    Update layers in Resource Watch back office.
    INPUT   var: variable for which we are updating layers (string)
            layer: layer that will be updated (string)
            most_recent_date: most recent date in GEE collection (datetime)
    '''
    # check which point on the timeline this is
    order = layer['attributes']['layerConfig']['order']

    # get datetime to show in asset
    new_layer_dt = most_recent_date+timedelta(hours=order)
    # get name of asset - drop first / in string or asset won't be pulled into RW
    new_asset = EE_COLLECTION_GEN.format(var=var)[1:]+'/'+FILENAME.format(compound=var,date=datetime.datetime.strftime(most_recent_date, DATE_FORMAT)+'_'+str(order).zfill(2))
    # get text for new date
    new_date_text = new_layer_dt.strftime("%B %-d, %Y %H:00")


    # get previous date being used from
    old_date = getDate(layer['attributes']['layerConfig']['assetId'])
    # convert to datetime
    old_date_dt = datetime.datetime.strptime(old_date.split('_')[0], DATE_FORMAT) + timedelta(hours=int(old_date.split('_')[1]))
    # get name of asset - drop first / in string or asset won't be pulled into RW
    old_asset = EE_COLLECTION_GEN.format(var=var)[1:]+'/'+FILENAME.format(compound=var,date=old_date)
    # change to layer name text of date
    old_date_text = old_date_dt.strftime("%B %-d, %Y %H:00")


    # replace date in layer's title with new date
    layer['attributes']['name'] = layer['attributes']['name'].replace(old_date_text, new_date_text)

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
    #r = requests.request('PATCH', rw_api_url_layer, data=json.dumps(payload), headers=create_headers())
    # check response
    # if we get a 200, the layers have been replaced
    # if we get a 504 (gateway timeout) - the layers are still being replaced, but it worked
    # if r.ok or r.status_code==504:
    #     logging.info('Layer replaced: {}'.format(layer['id']))
    # else:
    #     logging.error('Error replacing layer: {} ({})'.format(layer['id'], r.status_code))
    return {'url': rw_api_url_layer, 'data': json.dumps(payload), 'headers': create_headers()}
def get_most_recent_date(var):
    '''
    Get most recent data it
    INPUT   var: variable to check dates for (string)
    RETURN  most_recent_date: most recent date in GEE collection (datetime)
    '''
    # get a list of strings of dates in the collection
    existing_dates, existing_dates_by_var = checkCreateCollection(DATASET_IDS.keys())
    # get a list of the dates availale for input variable
    existing_dates_current_var = np.unique(existing_dates_by_var[list(DATASET_IDS.keys()).index(var)])
    # sort these dates oldest to newest
    existing_dates_current_var.sort()
    # get the most recent date (last in the list) and turn it into a datetime
    most_recent_date = datetime.datetime.strptime(existing_dates[-1], DATE_FORMAT)
    return most_recent_date

def updateResourceWatch():
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date', flushing the tile cache, and updating any dates on layers
    '''
    for var, ds_id in DATASET_IDS.items():
        logging.info('Updating {}'.format(var))
        # Get the most recent date from the data in the GEE collection
        most_recent_date = get_most_recent_date(var)
        # Get the current 'last update date' from the dataset on Resource Watch
        current_date = getLastUpdate(ds_id)
        # If the most recent date from the GEE collection does not match the 'last update date' on the RW API, update it
        if current_date != most_recent_date:
            # Update the dates on layer legends - TO BE ADDED IN FUTURE
            layer_dict = pull_layers_from_API(ds_id)
            # create a pool of processes
            pool = Pool()
            # create an empty list to store layer update calls
            futures = []                
            # go through each layer, pull the definition and update
            for layer in layer_dict:
                # replace layer asset and title date with new
                kwds = update_layer(var,  layer, most_recent_date)
                futures.append(pool.apply_async(requests.patch, kwds=kwds))
            # execute requests
            for future in futures:
                future.get()

            logging.info('Updating last update date and flushing cache.')
            # create a pool of processes
            pool = Pool()
            # create an empty list to store layer update calls
            futures = [] 
            # Update dataset's last update date on Resource Watch
            lastUpdateDate(ds_id, most_recent_date)
            # get layer ids and flush tile cache for each
            layer_ids = getLayerIDs(ds_id)
            for layer_id in layer_ids:
                kwds = flushTileCache_future(layer_id)
                futures.append(pool.apply_async(requests.delete, kwds=kwds))
            # execute requests
            for future in futures:
                future.get()
 
def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Initialize eeUtil
    eeUtil.initJson()
    # initialize ee and eeUtil modules for uploading to Google Earth Engine
    auth = ee.ServiceAccountCredentials(os.getenv('GEE_SERVICE_ACCOUNT'), os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
    ee.Initialize(auth)

    # Clear collection in GEE if desired
    if CLEAR_COLLECTION_FIRST:
        clearCollectionMultiVar()

    # Check if collection exists. If not, create it.
    # Return a list of dates that exist for all variables collections in GEE (existing_dates),
    # as well as a list of which dates exist for each individual variable (existing_dates_by_var).
    # The latter will be used in case the previous script run crashed before completing the data upload for every variable.
    logging.info('Getting existing dates.')
    existing_dates, existing_dates_by_var = checkCreateCollection(DATASET_IDS.keys())

    # Fetch, process, and upload the new data
    new_assets = processNewData(existing_dates)
    # Get the dates of the new data we have added
    new_dates = list(np.unique([getDate(a).split('_')[0] for a in new_assets]))

    logging.info('Previous assets: {}, new: {}'.format(
        len(existing_dates), len(new_dates)))

    # Delete excess assets and files from S3
    deleteExcessAssets(existing_dates+new_dates, MAX_DATES)

    # Update Resource Watch
    updateResourceWatch()

    logging.info('SUCCESS')
