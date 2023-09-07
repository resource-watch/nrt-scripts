from __future__ import unicode_literals

import os
import sys
import urllib.request
import datetime
import logging
import subprocess
import eeUtil
from netCDF4 import Dataset
import rasterio as rio
from collections import defaultdict
import requests
import time
import ee
import json 
import shutil

# url for vegetation health products data
# old url 'ftp://ftp.star.nesdis.noaa.gov/pub/corp/scsb/wguo/data/Blended_VH_4km/VH/{target_file}'
SOURCE_URL = 'https://www.star.nesdis.noaa.gov/pub/corp/scsb/wguo/data/Blended_VH_4km/VH/{target_file}'

# filename format for GEE
# old format 'VHP.G04.C07.npp.P{date}.VH.nc'
SOURCE_FILENAME = 'VHP.G04.C07.j01.P{date}.VH.nc'

# list variables (as named in GEE) that we want to pull
VARS = ['VHI', 'VCI']

#  netcdf subdataset variables to be converted to tif files and their associated GEE collection names
COLLECTION_NAMES = {
    'VHI':'foo_024_vegetation_health_index',
    'VCI':'foo_051_vegetation_condition_index',
}

# name of data directory in Docker container
DATA_DIR = 'data'

# do you want to delete everything currently in the GEE collection when you run this script?
CLEAR_COLLECTION_FIRST = False

# filename format for GEE
FILENAME = '{collection}_{date}'

# how many assets can be stored in the GEE collection before the oldest ones are deleted?
MAX_ASSETS = 36

# format of date (used in both the source data files and GEE)
DATE_FORMAT = '%Y0%V'

# Resource Watch dataset API IDs
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_IDS = {
    'foo_051_vegetation_condition_index':'2447d765-dc04-4e4a-aeaa-904760e94991',
    'foo_024_vegetation_health_index':'c12446ce-174f-4ffb-b2f7-77ecb0116aba'
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

def delete_local():
    '''
    Delete all files and folders in Docker container's data directory
    '''
    try:
        # for each object in the data directory
        for f in os.listdir(DATA_DIR):
            # try to remove it as a file
            try:
                logging.info('Removing {}'.format(f))
                os.remove(DATA_DIR+'/'+f)
            # if it is not a file, remove it as a folder
            except:
                shutil.rmtree(DATA_DIR+'/'+f, ignore_errors=True)
    except NameError:
        logging.info('No local files to clean.')

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
    # specify the maximum number of attempt we will make
    tries = 4
    while try_num<tries:
        try:
            # try to delete the cache
            r = requests.delete(url = apiUrl, headers = headers, timeout=1000)
            # if we get a 200, the cache has been deleted
            # if we get a 504 (gateway timeout) - the tiles are still being deleted, but it worked
            if r.ok or r.status_code==504 or r.status_code==500:
                logging.info('[Cache tiles deleted] for {}: status code {}'.format(layer_id, r.status_code))
                return r.status_code
            # if we don't get a 200 or 504 or 500:
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
    INPUT   var: variable we are creating asset name for (string)
    RETURN  GEE collection name for input date (string)
    '''
    return COLLECTION_NAMES[var]

def clearCollectionMultiVar():
    '''
    Clear the GEE collection for all variables
    '''
    logging.info('Clearing collections.')
    for var_num in range(len(VARS)):
        # get name of variable we are clearing GEE collections for
        var = VARS[var_num]
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

def getAssetName(tif, collection):
    '''
    get asset name from tif name
    INPUT   tif: name of tif file (string)
            collection: GEE collection where the asset will be uploaded (string)
    RETURN  name of asset that this tif should be uploaded as (string)
    '''
    # get the date from the tif file name
    date = getDate(tif)
    return os.path.join(collection, FILENAME.format(collection=collection, date=date))

def getDate(filename):
    '''
    get date from filename (last 7 characters of filename after removing extension)
    INPUT   filename: file name that ends in a date, in the format of the DATE_FORMAT variable (string)
    RETURN  date, in the format of the DATE_FORMAT variable (string)
    '''
    return os.path.splitext(os.path.basename(filename))[0][-7:]

def getNewTargetDates(existing_dates):
    '''
    Get new dates we want to try to fetch data for
    INPUT  existing_dates: list of dates that we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
    RETURN  new_dates: list of dates that we want to try to fetch data for (list of strings)
    '''
    # create empty list to store dates we should try to fetch data for
    new_dates = []
    # start with today's date and time
    date = datetime.date.today()
    # go back one week
    date -= datetime.timedelta(days = 7)
    # get a list of dates as long as the maximum number of assets we can store in this collection
    for i in range(MAX_ASSETS):
        # create a string of the current datetime
        datestr = date.strftime(DATE_FORMAT)
        # if the current date we are checking isn't already on GEE, add it to the list of new dates to try to fetch
        if datestr not in existing_dates:
            new_dates.append(datestr)
        # go back one more week and repeat
        date -= datetime.timedelta(days = 7)
    return new_dates

def fetch(date):
    '''
    Fetch files by datestamp
    INPUT   date: date we are fetching data for in the format specified in DATE_FORMAT variable (string)
    RETURN  file we have downloaded (string)
    '''
    # format the source filename with the date we are fetching
    target_file = SOURCE_FILENAME.format(date=date)
    # generate the full url where we can fetch this data
    _file = SOURCE_URL.format(target_file=target_file)
    # try to download the data
    urllib.request.urlretrieve(_file, os.path.join(DATA_DIR,target_file))
    return os.path.join(DATA_DIR,target_file)

def convert(nc_file, var, collection, date):
    '''
    convert variable in netcdf file to compressed tif file
    INPUT   nc_file: file location of netcdf we are converting to a tif (string)
            var: variable we are converting to tif (string)
            collection: GEE collection where this file will be uploaded (string)
            date: date of file we are converting in the format specified in DATE_FORMAT variable (string)
    RETURN  new_file: tif file that we have generated (string)
    '''

    logging.info('Extracting subdata')
    # open netcdf file
    nc = Dataset(nc_file)
    # generate a file name for the tif file we will extract
    extracted_var_tif = '{}_{}.tif'.format(os.path.splitext(nc_file)[0], var)
    # extract data
    data = nc[var][:, :]
    # create a copy of the data
    outdata = data.data.copy()
    # get the scale factor for this variable
    scale_factor = nc[var].scale_factor
    # get the add offset for this variable
    add_offset = nc[var].add_offset
    # apply the scale_factor and add_offset to the data to get the correct data values
    outdata[outdata>=0] = outdata[outdata>=0] * scale_factor + add_offset

    # pull the extent of the dataset and generate its transform
    extent = [nc.geospatial_lon_min, nc.geospatial_lat_min, nc.geospatial_lon_max, nc.geospatial_lat_max]
    transform = rio.transform.from_bounds(*extent, data.shape[1], data.shape[0])

    # create a profile for the tif file we will generate
    profile = {
        'driver': 'GTiff',
        'height': data.shape[0],
        'width': data.shape[1],
        'count': 1,
        'dtype': rio.float32,
        'crs':'EPSG:4326',
        'transform': transform,
        'nodata': nc[var]._FillValue
    }
    # write the tif file
    with rio.open(extracted_var_tif, 'w', **profile) as dst:
        dst.write(outdata.astype(rio.float32), 1)
    # delete the netcdf variable
    del nc

    logging.info('Compressing')
    # generate a file name to use for the compressed tif file we will create
    new_file = os.path.join(DATA_DIR, '{}.tif'.format(FILENAME.format(collection = collection, date = date)))
    # compress tif file
    cmd = ['gdal_translate','-co','COMPRESS=LZW','-of','GTiff', extracted_var_tif, new_file]
    subprocess.call(cmd)

    # delete uncompressed tif file
    os.remove(extracted_var_tif)

    logging.info('Converted {} to {}'.format(nc_file, new_file))
    return new_file

def uploadAssets(tifs, collection):
    '''
    upload tif files to Google Earth Engine collection
    INPUT   tifs: list of tif files to upload (list of strings)
            collection: GEE collection we want to upload the files to(string)
    RETURN  assets: list of GEE assets that have been uploaded (list of strings)
    '''
    # get a list of asset names to use for the input tif files
    assets = [getAssetName(tif, collection) for tif in tifs]
    # get a list of dates associated with each tif file
    dates = [getDate(tif) for tif in tifs]
    # Get a list of datetimes from these dates for each of the dates we are uploading
    # Set date to the end of the reported week, -0 corresponding to Sunday at end of week
    datestamps = [datetime.datetime.strptime(date + '-0', '%G0%V-%w') for date in dates]
    # Upload new files (tifs) to GEE - try to upload data twice before quitting
    try_num=1
    while try_num<=2:
        try:
            logging.info('Upload {} try number {}'.format(collection, try_num))
            eeUtil.uploadAssets(tifs, assets, collection, datestamps, timeout=3000)
            break
        except:
            try_num+=1
    return assets

def processNewData(existing_dates_by_var):
    '''
    fetch, process, upload, and clean new data
    INPUT   existing_dates_by_var: dictionary containing GEE collection name and list of dates that it already
                                    has uploaded to it (dictionary)
    RETURN  new_assets_by_var: dictionary containing GEE collection name and list of new dates that that have just
                                    been uploaded to it (dictionary)
    '''

    # Turn the dictionary of existing dates by variable into a single, combined list of existing dates
    existing_dates = []
    for collection, e_dates in existing_dates_by_var.items():
        existing_dates.extend(e_dates)

    # get a list of dates that we want to try to fetch data for
    target_dates = getNewTargetDates(existing_dates)

    # fetch new files
    logging.info('Fetching files')
    # create an empty dictionary to store the file locations of the tifs we will create and upload to each collection
    tifs_dict = defaultdict(list)
    # loop through each date we want to try to fetch
    for date in target_dates:
        # try to fetch the data
        try:
            nc_file = fetch(date)
        # if we can't fetch the file, log an error and continue to next date
        except Exception as e:
            logging.error('Could not fetch data for date: {}'.format(date))
            logging.error(e)
            continue
        # process each variable of interest
        for var, collection in COLLECTION_NAMES.items():
            # convert each variable into its own tif file
            tif = convert(nc_file, var, collection, date)
            # add the processed tif file location to our dictionary of tifs to upload
            tifs_dict[collection].append(tif)
        # delete netcdf file for this date because we have finished processing it
        os.remove(nc_file)

    # Upload new files (tifs) to GEE
    logging.info('Uploading files')
    new_assets_by_var = defaultdict(list)
    for collection, tifs in tifs_dict.items():
        new_assets_by_var[collection] = uploadAssets(tifs, collection)

    # Delete local files
    logging.info('Cleaning local files')
    for collection, tifs in tifs_dict.items():
        for tif in tifs:
            os.remove(tif)
    return new_assets_by_var

def checkCreateCollection(collection):
    '''
    List assets in collection if it exists, else create new collection
    INPUT   collection: GEE collection to check or create (string)
    RETURN  list of assets in collection (list of strings)
    '''
    # if collection exists, return list of assets in collection
    if eeUtil.exists(collection):
        return eeUtil.ls(collection)
    # if collection does not exist, create it and return an empty list (because no assets are in the collection)
    else:
        logging.info('{} does not exist, creating'.format(collection))
        eeUtil.createFolder(collection, True, public=True)
        return []

def deleteExcessAssets(dates, collection, max_assets):
    '''
    Delete oldest assets, if more than specified in max_assets variable
    INPUT   dates: dates for all the assets currently in the GEE collection; dates should be in the format specified
                    in DATE_FORMAT variable (list of strings)
            collection: name of collection that dates are in (string)
            max_assets: maximum number of assets allowed in the collection (int)
    '''
    # sort the list of dates so that the oldest is first
    dates.sort()
    # if we have more dates of data than allowed,
    if len(dates) > max_assets:
        # go through each date, starting with the oldest, and delete until we only have the max number of assets left
        for date in dates[:-max_assets]:
            asset_name = os.path.join(collection, FILENAME.format(collection=collection, date=date))
            eeUtil.removeAsset(asset_name)

def get_most_recent_date(collection):
    '''
    Get most recent date from the data in the GEE collection
    INPUT   collection: GEE collection to check dates for (string)
    RETURN  most_recent_date: most recent date in GEE collection (datetime)
    '''
    # get list of assets in collection
    existing_assets = checkCreateCollection(collection)
    # get a list of strings of dates in the collection
    existing_dates = [getDate(a) for a in existing_assets]
    # sort these dates oldest to newest
    existing_dates.sort()
    # get the most recent date (last in the list) and turn it into a datetime
    # since data sets represent a week of data, take the last day of the week (7) as the most recent update
    datestr = existing_dates[-1]+'7'
    most_recent_date = datetime.datetime.strptime(datestr, '%G0%V%u')
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

def get_date_7d(title, new_date):
    '''
    Get current date from layer title and construct new date from most recent date
    INPUT   title: current layer titile (string)
            new_date: latest date of data to be shown in this layer (datetime)
    RETURN  old_date_text: current date being used in the title (string)
            new_date_text: new date to be show in the title (string)
    '''
    # get current end date being used from title by string manupulation
    old_date_text = title.split(' Vegetation')[0]
    # get text for new date
    new_date_end = datetime.datetime.strftime(new_date, "%B %d, %Y")
    # get most recent starting date, 7 days ago
    new_date_start = (new_date - datetime.timedelta(days=6))
    new_date_start = datetime.datetime.strftime(new_date_start, "%B %d, %Y")
    # construct new date range by joining new start date and new end date
    new_date_text = new_date_start + ' - ' + new_date_end

    return old_date_text, new_date_text

def get_date(title, new_date):
    '''
    Get current date from layer title and construct new date from most recent date
    INPUT   title: current layer titile (string)
            new_date: latest date of data to be shown in this layer (datetime)
    RETURN  old_date_text: current date being used in the title (string)
            new_date_text: new date to be show in the title (string)
    '''
    # get current end date being used from title by string manupulation
    old_date_text = title.split(' Vegetation')[0]
    # get text for new date
    new_date_text = datetime.datetime.strftime(new_date, "%B %d, %Y")

    return old_date_text, new_date_text

def update_layer(layer, new_date):
    '''
    Update layers in Resource Watch back office.
    INPUT   layer: layer that will be updated (string)
            new_date: date of asset to be shown in this layer (datetime)
    '''
    # get current layer titile
    cur_title = layer['attributes']['name']
    
    # if we are processing the layer that shows most recent Vegetation Condition Index
    if cur_title.endswith('(NDVI anomalies)'):
        old_date_text, new_date_text = get_date(cur_title, new_date)
    # if we are processing the layer that shows Vegetation Condition Index for last 7 days
    else:
        old_date_text, new_date_text = get_date_7d(cur_title, new_date)

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
    # if we get a 503 - the layers are still being replaced, but it worked
    if r.ok or r.status_code==504 or r.status_code==503:
        logging.info('Layer replaced: {}'.format(layer['id']))
    else:
        logging.error('Error replacing layer: {} ({})'.format(layer['id'], r.status_code))
        
def updateResourceWatch():
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date', flushing the tile cache, and updating any dates on layers
    '''
    for collection, id in DATASET_IDS.items():
        # Get most recent date in GEE
        most_recent_date = get_most_recent_date(collection)
        # Get the current 'last update date' from the dataset on Resource Watch
        current_date = getLastUpdate(id)
        # Update the dates on layer legends
        logging.info('Updating {}'.format(collection))
        # pull dictionary of current layers from API
        layer_dict = pull_layers_from_API(id)
        # go through each layer, pull the definition and update
        for layer in layer_dict:
            # replace layer title with new dates
            update_layer(layer, most_recent_date)
        # If the most recent date from the GEE collection does not match the 'last update date' on the RW API, update it
        if current_date != most_recent_date:
            logging.info('Updating last update date and flushing cache.')
            # Update dataset's last update date on Resource Watch
            lastUpdateDate(id, most_recent_date)
            # get layer ids and flush tile cache for each
            layer_ids = getLayerIDs(id)
            for layer_id in layer_ids:
                flushTileCache(layer_id)

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
    '''Ingest new data into EE and delete old data'''
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Initialize eeUtil and ee modules
    eeUtil.initJson()
    initialize_ee()

    # Clear the GEE collection, if specified above
    if CLEAR_COLLECTION_FIRST:
        clearCollectionMultiVar()

    # Check if each collection exists, create it if it does not
    # If it exists return the list of assets currently in the collection
    existing_assets = {}
    for var_num in range(len(VARS)):
        # get name of variable we are clearing GEE collections for
        var = VARS[var_num]
        # get name of GEE collection for variable
        collection = getCollectionName(var)
        existing_assets[collection] = checkCreateCollection(collection)
    # Get a list of the dates of data we already have in each collection
    existing_dates_by_var = {}
    for collection, ex_assets in existing_assets.items():
        existing_dates_by_var[collection] = list(map(getDate, ex_assets))

    # Fetch, process, and upload the new data
    new_assets = processNewData(existing_dates_by_var)
    # Get the dates of the new data we have added
    new_dates = {}
    for collection, assets in new_assets.items():
        new_dates[collection] = list(map(getDate, assets))

    # Delete excess assets
    for var, collection in COLLECTION_NAMES.items():
        # get previously existing dates for this variable
        e = existing_dates_by_var[collection]
        # get new dates added for this variable
        n = new_dates[collection] if collection in new_dates else []
        logging.info('Previous assets: {}, new: {}, max: {}'.format(len(e), len(n), MAX_ASSETS))
        # delete any excess assets
        deleteExcessAssets(e+n, collection, MAX_ASSETS)

    # Update Resource Watch
    updateResourceWatch()

    # Delete local files
    delete_local()
    
    logging.info('SUCCESS')
