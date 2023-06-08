from __future__ import unicode_literals

import os
import sys
import datetime
import logging
import subprocess
import eeUtil
import requests
from bs4 import BeautifulSoup
import urllib.request
import time
import json
import re
import shutil

# url for fire weather data
SOURCE_URL = 'https://portal.nccs.nasa.gov/datashare/GlobalFWI/v2.0/fwiCalcs.GEOS-5/Default/GPM.LATE.v5/{year}/FWI.GPM.LATE.v5.Daily.Default.{date}.nc'

# subdatasets to be converted to tif
# should be of the format 'NETCDF:"filename.nc":variable'
SDS_NAMES = ['NETCDF:"{fname}":GPM.LATE.v5_FWI', 'NETCDF:"{fname}":GPM.LATE.v5_BUI', 'NETCDF:"{fname}":GPM.LATE.v5_DC',
             'NETCDF:"{fname}":GPM.LATE.v5_DMC', 'NETCDF:"{fname}":GPM.LATE.v5_FFMC', 'NETCDF:"{fname}":GPM.LATE.v5_ISI']

# filename format for GEE
FILENAME = 'for_012_fire_risk_{date}'

# nodata value for netcdf
NODATA_VALUE = None

# name of data directory in Docker container
DATA_DIR = 'data'

# name of folder to store data in Google Cloud Storage
GS_FOLDER = 'for_012_fire_risk'

# name of collection in GEE where we will upload the final data
EE_COLLECTION = '/projects/resource-watch-gee/for_012_fire_risk'

# do you want to delete everything currently in the GEE collection when you run this script?
CLEAR_COLLECTION_FIRST = False

# how many assets can be stored in the GEE collection before the oldest ones are deleted?
MAX_ASSETS = 15

# format of date (used in both the source data files and GEE)
DATE_FORMAT = '%Y%m%d'

# Resource Watch dataset API ID and GFW dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_IDS = {
    'RW': 'c56ee507-9a3b-41d3-90ac-1406bee32c32',
    'GFW': '3b850f92-c7e3-4103-9f24-ea7d41a94b84'}

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

def getUrl(date):
    '''
    format source url with date
    INPUT   date: date in the format YYYYMMDD (string)
    RETURN  source url to download data, formatted for the input date (string)
    '''
    return SOURCE_URL.format(year=date[0:4], date=date)

def getAssetName(date):
    '''
    get asset name
    INPUT   date: date in the format of the DATE_FORMAT variable (string)
    RETURN  GEE asset name for input date (string)
    '''
    return os.path.join(EE_COLLECTION, FILENAME.format(date=date))

def getFilename(date):
    '''
    get netcdf filename to save source file as
    INPUT   date: date in the format of the DATE_FORMAT variable (string)
    RETURN  file name to save netcdf from source under (string)
    '''
    return os.path.join(DATA_DIR, '{}.nc'.format(FILENAME.format(date=date)))
        
def getDate(filename):
    '''
    get date from filename (last 8 characters of filename after removing extension)
    INPUT   filename: file name that ends in a date of the format YYYYMMDD (string)
    RETURN  date in the format YYYYMMDD (string)
    '''
    return os.path.splitext(os.path.basename(filename))[0][-8:]

def getNewDates(exclude_dates):
    '''
    Get new dates we want to try to fetch data for
    INPUT   exclude_dates: list of dates that we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
    RETURN  new_dates: list of new dates we want to try to get, in the format of the DATE_FORMAT variable (list of strings)
    '''
    # create empty list to store dates we want to fetch
    new_dates = []
    # start with today's date
    date = datetime.date.today()
    # if anything is in the collection, check back until last uploaded date
    if len(exclude_dates) > 0:
        # if the date string is not the list of dates we already have, add it to the list of new dates to try and fetch
        while (date.strftime(DATE_FORMAT) not in exclude_dates):
            # generate a string from the date
            datestr = date.strftime(DATE_FORMAT)
            # add to list of new dates
            new_dates.append(datestr) 
            # go back one day at a time
            date -= datetime.timedelta(days=1)
    #if the collection is empty, make list of most recent 10 days to check
    else:
        for i in range(10):
            # generate a string from the date
            datestr = date.strftime(DATE_FORMAT)
            # add to list of new dates
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
    # create an empty list to store the names of the tifs we generate from all input netcdf files
    tifs = []
    # go through each netcdf file and translate
    for f in files:
        # create an empty list to store the names of the tifs we generate from this netcdf file
        band_tifs = []
        # go through each variables to process in this netcdf file
        for sds_name in SDS_NAMES:
            # extract subdataset by name
            sds_path = sds_name.format(fname=f)
            # generate a name to save the tif file we will translate the netcdf file's subdataset into
            band_tif = '{}_{}.tif'.format(os.path.splitext(f)[0], sds_name.split('_')[-1]) 
            # translate the netcdf file's subdataset into a tif
            cmd = ['gdal_translate','-q', '-a_nodata', str(NODATA_VALUE), '-a_srs', 'EPSG:4326', sds_path, band_tif] 
            logging.debug('Converting {} to {}'.format(f, band_tif))
            subprocess.call(cmd)
            # add the new subdataset tif files to the list of tifs generated from this netcdf file
            band_tifs.append(band_tif)
        # generate a name to save the tif file that will be produced by merging all the sub tifs from this netcdf   
        merged_tif = '{}.tif'.format(os.path.splitext(f)[0])  
        # merge all the sub tifs from this netcdf to create an overall tif representing all variables
        merge_cmd = ['gdal_merge.py', '-seperate'] + band_tifs + ['-o', merged_tif]
        subprocess.call(merge_cmd)
        # add the new tif files to the list of tifs
        tifs.append(merged_tif)
    return tifs

def list_available_files(url, ext=''):
    '''
    Fetch a list of filenames from source url by year
    INPUT   url: url for data source where we want to check for download links (string)
            ext: extension of file type we are checking for (string)
    RETURN  list of files available for download from source website (list of strings)
    '''    
    # open and read the url
    page = requests.get(url).text
    # use BeautifulSoup to read the content as a nested data structure
    soup = BeautifulSoup(page, 'html.parser')
    # Extract all the <a> tags within the html content to find the files available for download marked with these tags.
    # Get only the files that ends with input extension (if specified)
    return [node.get('href') for node in soup.find_all('a') if type(node.get('href'))==str and node.get('href').endswith(ext)]

def fetch(new_dates):
    '''
    Fetch files by datestamp
    INPUT   dates: list of dates we want to try to fetch, in the format YYYYMMDD (list of strings)
    RETURN  files: list of file names for netcdfs that have been downloaded (list of strings)
    '''
    # make an empty list to store names of the files we downloaded
    files = []
    # go through each input date
    for date in new_dates:
        # get the url to download the file from the source for the given date
        url = getUrl(date)
        # get the filename we want to save the file under locally
        f = getFilename(date)
        # get the filename to download from the url
        file_name = os.path.split(url)[1]
        # get a list of available filenames from source website for the input date's year
        file_list = list_available_files(os.path.split(url)[0], ext='.nc')
        # check if the filename to download is present in the source website
        if file_name in file_list:
            logging.info('Retrieving {}'.format(file_name))
            try:
                # try to download the data
                urllib.request.urlretrieve(url, f)
                # if successful, add the file to the list of files we have downloaded
                files.append(f)
                logging.info('Successfully retrieved {}'.format(f))
            except Exception as e:
                # if unsuccessful, log that the file was not downloaded
                logging.error('Unable to retrieve data from {}'.format(url))
                logging.debug(e)
        else:
            # if the filename to download is not present in the source website,
            # log that we are attempting to download a file that is not available yet
            logging.info('{} not available yet'.format(file_name))

    return files

def processNewData(existing_dates):
    '''
    fetch, process, upload, and clean new data
    INPUT   existing_dates: list of dates we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
    RETURN  assets: list of file names for netcdfs that have been downloaded (list of strings)
    '''
    # Get list of new dates we want to try to fetch data for
    new_dates = getNewDates(existing_dates)

    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    # Fetch new files
    logging.info('Fetching files')
    files = fetch(new_dates) 

    # If we have successfully been able to fetch new data files
    if files: 
        # Convert new files from netcdf to tif files
        logging.info('Converting files')
        tifs = convert(files)

        logging.info('Uploading files')
        # Get a list of the dates we have to upload from the tif file names
        dates = [getDate(tif) for tif in tifs] 
        # Get a list of datetimes from these dates for each of the dates we are uploading
        datestamps = [datetime.datetime.strptime(date, DATE_FORMAT) for date in dates] 
        # Get a list of the names we want to use for the assets once we upload the files to GEE
        assets = [getAssetName(date) for date in dates] 
        # Upload new files (tifs) to GEE
        eeUtil.uploadAssets(tifs, assets, GS_FOLDER, datestamps)

        # Delete local files
        logging.info('Cleaning local files')
        for tif in tifs:
            os.remove(tif)
        for f in files:
            os.remove(f)

        return assets
    return []

def checkCreateCollection(collection):
    '''
    List assests in collection if it exists, else create new collection
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

def deleteExcessAssets(dates, max_assets):
    '''
    Delete oldest assets, if more than specified in max_assets variable
    INPUT   dates: dates for all the assets currently in the GEE collection; dates should be in the format specified
                    in DATE_FORMAT variable (list of strings)
            max_assets: maximum number of assets allowed in the collection (int)
    '''
    # sort the list of dates so that the oldest is first
    dates.sort()
    # if we have more dates of data than allowed,
    if len(dates) > max_assets:
        # go through each date, starting with the oldest, and delete until we only have the max number of assets left
        for date in dates[:-max_assets]:
            eeUtil.removeAsset(getAssetName(date))

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
    most_recent_date = datetime.datetime.strptime(existing_dates[-1], DATE_FORMAT)
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
    rw_api_url = 'https://api.resourcewatch.org/v1/dataset/{}?includes=layer&page[size]=100'.format(dataset_id)
    # request data
    r = requests.get(rw_api_url)
    # convert response into json and make dictionary of layers
    layer_dict = json.loads(r.content.decode('utf-8'))['data']['attributes']['layer']
    return layer_dict

def update_layer(layer, new_date):
    '''
    Update layers in Resource Watch back office.
    INPUT   layer: layer that will be updated (string)
            new_date: date of asset to be shown in this layer (datetime)
    '''
    # get current layer titile
    cur_title = layer['attributes']['name']
    
    # get current end date being used from title by string manupulation
    old_date_text = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}', cur_title).group()

    # latest data is for one day ago, so subtracting a day
    new_date_end = (new_date)
    # get text for new date
    new_date_text = datetime.datetime.strftime(new_date_end, "%B %d, %Y")

    # replace date in layer's title with new date
    layer['attributes']['name'] = layer['attributes']['name'].replace(old_date_text, new_date_text)

    # send patch to API to replace layers
    # generate url to patch layer
    rw_api_url_layer = "https://api.resourcewatch.org/v1/dataset/{dataset_id}/layer/{layer_id}".format(
        dataset_id=layer['attributes']['dataset'], layer_id=layer['id'])
    # create payload with new title and layer configuration
    payload = {
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
    # Get the most recent date from the data in the GEE collection
    most_recent_date = get_most_recent_date(EE_COLLECTION)
    # update the layer dates on RW and GFW
    for DATASET_ID in DATASET_IDS.values():
        # Get the current 'last update date' from the dataset on Resource Watch
        current_date = getLastUpdate(DATASET_ID)
        # Update the dates on layer legends
        logging.info('Updating {}'.format(DATASET_ID))
        # pull dictionary of current layers from API
        layer_dict = pull_layers_from_API(DATASET_ID)
        # go through each layer, pull the definition and update
        for layer in layer_dict:
            # replace layer title with new dates
            update_layer(layer, most_recent_date)
    # If the most recent date from the GEE collection does not match the 'last update date' on the RW API, update it
    if current_date != most_recent_date:
        logging.info('Updating last update date and flushing cache.')
        # Update dataset's last update date on Resource Watch
        lastUpdateDate(DATASET_IDS["RW"], most_recent_date)
        # get layer ids and flush tile cache for each
        layer_ids = getLayerIDs(DATASET_IDS["RW"])
        for layer_id in layer_ids:
            flushTileCache(layer_id)

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Initialize eeUtil 
    eeUtil.initJson()

    # Clear the GEE collection, if specified above
    if CLEAR_COLLECTION_FIRST:
        if eeUtil.exists(EE_COLLECTION):
            eeUtil.removeAsset(EE_COLLECTION, recursive=True)

    # Check if collection exists, create it if it does not
    # If it exists return the list of assets currently in the collection
    existing_assets = checkCreateCollection(EE_COLLECTION)
    # Get a list of the dates of data we already have in the collection
    existing_dates = [getDate(a) for a in existing_assets]

    # Fetch, process, and upload the new data
    new_assets = processNewData(existing_dates)
    # Get the dates of the new data we have added
    new_dates = [getDate(a) for a in new_assets]

    logging.info('Previous assets: {}, new: {}, max: {}'.format(
        len(existing_dates), len(new_dates), MAX_ASSETS))

    # Delete excess assets
    deleteExcessAssets(existing_dates+new_dates, MAX_ASSETS)

    # Update Resource Watch
    updateResourceWatch()

    # Delete local files in Docker container
    delete_local()

    logging.info('SUCCESS')
