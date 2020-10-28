from __future__ import unicode_literals

import os
import sys
import datetime
import logging
import subprocess
import eeUtil
import requests
import time
from bs4 import BeautifulSoup
import gdal
import urllib
import urllib.request
from http.cookiejar import CookieJar

# url for MODIS burned area data
SOURCE_URL = 'https://e4ftl01.cr.usgs.gov/MOTA/MCD64A1.006/{date}/'

# filename format for GEE
FILENAME = 'for_030_burned_areas_{date}'

# name of data directory in Docker container
DATA_DIR = os.path.join(os.getcwd(),'data')

# name of folder to store data in Google Cloud Storage
GS_FOLDER = 'for_030_burned_areas'

# name of collection in GEE where we will upload the final data
EE_COLLECTION = 'for_030_burned_areas'

# do you want to delete everything currently in the GEE collection when you run this script?
CLEAR_COLLECTION_FIRST = False

# how many assets can be stored in the GEE collection before the oldest ones are deleted?
# maximum assets is 60 in this case (5 years of monthly data)
MAX_ASSETS = 60

# format of date (used in source data files)
DATE_FORMAT_HDF = '%Y.%m.%d'

# format of date (used in GEE)
DATE_FORMAT = '%Y%m%d'

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = ''

'''
FUNCTIONS FOR ALL DATASETS

The functions below must go in every near real-time script.
Their format should not need to be changed.
'''

def lastUpdateDate(dataset, date):
    '''
    Given a Resource Watch dataset's API ID and a datetime,
    this function will update the dataset's 'last update date' on the API with the given datetime
    INPUT   dataset: Resource Watch API dataset ID (string)
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
    INPUT   dataset: Resource Watch API dataset ID (string)
    RETURN  lastUpdateDT: current 'last update date' for the input dataset (datetime)
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
    INPUT   dataset: Resource Watch API dataset ID (string)
    RETURN  layerIDs: Resource Watch API layer IDs for the input dataset (list of strings)
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
    INPUT   layer_id: Resource Watch API layer ID (string)
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
     INPUT   date: date in the format YYYY.MM.DD (string)
     RETURN  source url to download data, formatted for the input date (string)
     '''
     return SOURCE_URL.format(date=date)

def getsubFilename(hdf):
     '''
     Data for each year is saved into several sub files in source url. Here we
     get hdf filename to save individual source file as 
     INPUT   hdf: file name for the hdf file (string)
     RETURN  file name to save individual hdf from source under (string)
     '''
     return os.path.join(DATA_DIR, hdf)

def getFilename(date):
     '''
     After acquiring the individual files, we merge them to have a single file
     for each year. Here we get hdf filename to save merged source file as
     INPUT   date: date in the format of the DATE_FORMAT variable (string)
     RETURN  file name to save merged hdf from source under (string)
     '''
     return os.path.join(DATA_DIR, '{}.hdf'.format(FILENAME.format(date=date)))

def getFormDate(date):
    '''
    format date according to the format used in Google Earth Engine
    INPUT   date: date in the format YYYY.MM.DD (string)
    RETURN  form_date: date in the format YYYYMMDD (string)
    '''
    # separate the input date using '.' to separate out year, month and day
    pieces = date.split('.')
    # join the year, month and day to create the date
    form_date = pieces[0]+pieces[1]+pieces[2]
    return form_date

def getAssetName(date):
     '''
     get asset name
     INPUT   date: date in the format of the DATE_FORMAT variable (string)
     RETURN  GEE asset name for input date (string)
     '''
     return os.path.join(EE_COLLECTION, FILENAME.format(date=date))

def getDate(filename):
     '''
     get date from filename (last 8 characters of filename after removing extension)
     INPUT   filename: file name that ends in a date of the format YYYYMMDD (string)
     RETURN  date in the format YYYYMMDD (string)
     '''
     return os.path.splitext(os.path.basename(filename))[0][-8:]

def list_available_dates():
    '''
    Fetch a list of folders from source url by year
    RETURN  available_dates: list of dates available for download from source website (list of strings)
    '''   
    # get rid of date from SOURCE_URL to get the parent directory where inidividual folder for each 
    # year is present
    url = SOURCE_URL.split('{')[0]
    # open and read the url
    page = requests.get(url).text
    # use BeautifulSoup to read the content as a nested data structure
    soup = BeautifulSoup(page, 'html.parser')
    # Extract all the <a> tags within the html content to find the files available for download marked with these tags.
    # Get only the files that starts with '2'. These are the folders that contain all the hdf files
    folders = [node.get('href') for node in soup.find_all('a') if node.get('href').startswith('2')]
    # get rid of '/' from every folder name to separate out dates
    available_dates = ([s.strip('/') for s in folders])
    
    return available_dates

def fetch(date):
     '''
     Fetch files by datestamp
     INPUT   date: date we want to try to fetch, in the format YYYY.MM.DD (string)
     RETURN  files: list of file names for hdfs that have been downloaded (list of strings)
     '''
     # Get the value of 'EARTHDATA_USER' & 'EARTHDATA_PASS' environment variable using get operation
     username = os.environ.get('EARTHDATA_USER')
     password = os.environ.get('EARTHDATA_PASS')
     # set up authentication with the urllib.request library
     password_manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
     password_manager.add_password(None, "https://urs.earthdata.nasa.gov", username, password)
     # the CookieJar class stores HTTP cookies. It extracts cookies from HTTP requests,
     # and returns them in HTTP responses.
     cookie_jar = CookieJar()
     # return an OpenerDirector instance, which chains the handlers for password_manager and cookie_jar
     # the OpenerDirector class opens URLs via BaseHandlers chained together
     opener = urllib.request.build_opener(
          urllib.request.HTTPBasicAuthHandler(password_manager),
          urllib.request.HTTPCookieProcessor(cookie_jar))
     # install an OpenerDirector instance as the default global opener
     urllib.request.install_opener(opener)

     # make an empty list to store names of the files we downloaded
     files = []

     # get the url where data for the given date is stored at the source
     url = getUrl(date)
     try:
          # open the url
          response = urllib.request.urlopen(url)
          # read the opened url
          content = response.read()
          # use BeautifulSoup to read the content as a nested data structure
          soup = BeautifulSoup(content, 'html.parser')
          # create an empty list to store the name of all hdf files available for the input date
          hdfs = []
          # Extract all the <a> tags within the html content. The <a> tags are used to mark links, so 
          # we will be able to find the files available for download marked with these tags.
          for a in soup.find_all('a'):
               str_a = str(a)
               # if one of the links available to download contains the word 'hdf'
               if 'hdf' in str_a:
                    # get the name of the hdf file
                    ext = str_a.index('.hdf')
                    hdf = str_a[9:ext+4]
                    # add the hdf file name to the list of files to download
                    hdfs.append(hdf)
          # convert the list to a set to remove duplicates and .xml files
          # convert it back to list again
          hdfs = list(set(hdfs))
          # loop through each hdf file in the list
          for hdf in hdfs:
              # join the source url with the id of each hdf to generate complete URLs
              # for each hdf file download
              sub_url = os.path.join(url, hdf)
              # get the filename we want to save the individual file under locally
              sub_filename = getsubFilename(hdf)
              try:
                   # try to download the data
                   urllib.request.urlretrieve(sub_url, sub_filename)
                   # if successful, add the file to the list of files we have downloaded
                   files.append(sub_filename)
                   # if successful, log that the file was downloaded successfully
                   logging.info('Successfully retrieved {}'.format(sub_filename))
              except Exception as e:
                   # if unsuccessful, log an error that the file was not downloaded
                   logging.error('Unable to retrieve data from {}'.format(sub_url))
                   logging.debug(e)

     except Exception as e:
          # if unsuccessful, log that no data were found for the input date
          # (could be one of the days not covered by this data set)
          logging.debug('No data found for date {}, could be one of the days not covered by this data set (reminder, only updates once every 8 days)'.format(date))
          logging.debug(e)

     return files

def convert(file):
     '''
     Convert hdf file to tifs
     INPUT   file: file names for hdf that have already been downloaded (string)
     RETURN  merged_tif: file name for tif that have been generated (string)
     '''
     # create an empty list to store the names of the tifs we generate from all input hdf files
     tifs = []

     # open the hdf file
     hdf_handle = gdal.Open(file)
     # get a list of the subdataset from hdf 
     sds_list = hdf_handle.GetSubDatasets()
     # create an empty list to store tifs for each subdataset
     band_tifs = []
     # loop through each subdataset
     for sds in sds_list:
         # process only "Burn Date", "First Day" and "Last Day" subdataset
         if '"Burn Date"' in sds[0] or '"First Day"' in sds[0] or '"Last Day"' in sds[0]:
             # get the name of the subdataset that we are processing
             # example filename: 
             # HDF4_EOS:EOS_GRID:"MCD64A1.A2000306.h16v07.006.2017012020102.hdf":MOD_Grid_Monthly_500m_DB_BA:"Burn Date"
             # for this filename we want to retrieve 'Burn Date'
             # get the index where BA:" is present
             atops = sds[0].find('BA:"')
             # get all the letter after 'BA:"', get rid of '"' from the end and replace empty space with underscore
             # the final ouptput for this filename would be 'Burn_Date'
             sppos = sds[0][atops+4:].strip('"').replace (" ", "_")
             # generate the tif file name by joining the filename with subdataset name
             band_tif = file.split('.hdf')[0] + sppos + '.tif'
             # translate the hdf file's subdataset into a tif
             cmd = ['gdal_translate','-q', sds[0], band_tif]
             subprocess.call(cmd)
             # append the translated tif file to the list of tif files for this hdf file
             band_tifs.append(band_tif)
     # generate a name to save the tif file that will be produced by merging all the sub tifs from this hdf   
     merged_tif = file.split('.hdf')[0] + '.tif'
     # merge all subdatasets from this hdf into a single tif by adding each subdataset as 
     # separate bands
     merge_cmd = ['gdal_merge.py', '-seperate'] + band_tifs + ['-o', merged_tif]
     subprocess.call(merge_cmd)

     return merged_tif

def processNewData(existing_dates):
    '''
    fetch, process, upload, and clean new data
    INPUT   existing_dates: list of dates we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
    RETURN  new_assets: list of file names for hdfs that have been downloaded (list of strings)
    '''
    # Get list of dates that are availble on the source
    available_dates = list_available_dates()
    logging.debug('Available dates: {}'.format(available_dates))

    # create an empty list to store asset names that will be uploaded to GEE
    new_assets = []
    # create an empty list to store a list of the dates we have to upload
    dates = []
    # create an empty list to store tif filenames that were created from hdf files
    tifs = []
    # fetch data one year at a time
    for date in available_dates:
        # if we don't have this date already in GEE
        if date not in existing_dates:
            # fetch new files
            logging.info('Fetching files')
            files = fetch(date)
            # create an empty list to store tif generated
            sub_tifs = []
            for _file in files:
                logging.info('Converting file: {}'.format(_file))
                # convert hdfs to tifs and store the tif filenames to a list
                sub_tifs.append(convert(_file))
            # format date according to the format used in Google Earth Engine
            formt_date = getFormDate(date) 
            # generate a name to save the tif file that will be produced by merging all the sub tifs from this netcdf   
            merged_tif = '{}.tif'.format(os.path.join(DATA_DIR, formt_date))
            # merge all the sub tifs from this hdf to create an overall tif representing all variables
            merge_cmd = ['gdal_merge.py'] + sub_tifs + ['-o', merged_tif]
            subprocess.call(merge_cmd)
            # add the new tif files to the list of tifs
            tifs.append(merged_tif)
            # Get a list of the dates we have to upload from the tif file names
            dates.append(formt_date)

        logging.info('Uploading files')
        # Get a list of the names we want to use for the assets once we upload the files to GEE
        assets = [getAssetName(date) for date in dates]
        # Get a list of datetimes from each of the dates we are uploading
        datestamps = [datetime.datetime.strptime(date, DATE_FORMAT) for date in dates]
        # Upload new files (tifs) to GEE
        eeUtil.uploadAssets(tifs, assets, GS_FOLDER, dates=datestamps, public=True, timeout=3000)
    # add list of assets uploaded to the new_assets list
    new_assets.extend(assets)

    return new_assets

def checkCreateCollection(collection):
    '''
    List assests in collection if it exists, else create new collection
    INPUT   collection: GEE collection to check or create (string)
    RETURN  list of assets in collection (list of strings)
    '''
    # if collection exists, return list of assets in collection
    if eeUtil.exists(collection):
        return eeUtil.ls(collection)
    # if collection does not exist, create it and return an empty list (because no assets are in the collection)
    else:
        logging.info('{} does not exist, creating'.format(collection))
        eeUtil.createFolder(collection, imageCollection=True, public=True)
        return []

def deleteExcessAssets(dates, max_assets):
    '''
    Delete oldest assets, if more than specified in max_assets variable
    INPUT   dates: dates for all the assets currently in the GEE collection; dates should be in the format specified
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
    Get most recent data we have assets for
    INPUT   collection: GEE collection to check dates for (string)
    RETURN  most_recent_date: most recent date in GEE collection (datetime)
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

def updateResourceWatch():
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date', flushing the tile cache, and updating any dates on layers
    '''
    # Get the most recent date from the data in the GEE collection
    most_recent_date = get_most_recent_date(EE_COLLECTION)
    # Get the current 'last update date' from the dataset on Resource Watch
    current_date = getLastUpdate(DATASET_ID)
    # If the most recent date from the GEE collection does not match the 'last update date' on the RW API, update it
    if current_date != most_recent_date:
        logging.info('Updating last update date and flushing cache.')
        # Update dataset's last update date on Resource Watch
        lastUpdateDate(DATASET_ID, most_recent_date)
        # get layer ids and flush tile cache for each
        layer_ids = getLayerIDs(DATASET_ID)
        for layer_id in layer_ids:
            flushTileCache(layer_id)
    # Update the dates on layer legends - TO BE ADDED IN FUTURE

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Initialize eeUtil
    eeUtil.initJson()

    # clear the GEE collection, if specified above
    if CLEAR_COLLECTION_FIRST:
        if eeUtil.exists(EE_COLLECTION):
            eeUtil.removeAsset(EE_COLLECTION, recursive=True)

    # Check if collection exists, create it if it does not
    # If it exists return the list of assets currently in the collection
    existing_assets = checkCreateCollection(EE_COLLECTION)
    # Get a list of the dates of data we already have in the collection
    existing_dates = [getDate(asset) for asset in existing_assets]
    logging.debug(existing_dates)

    # Fetch, process, and upload the new data
    os.chdir(DATA_DIR)
    new_assets = processNewData(existing_dates)
    # Get the dates of the new data we have added
    new_dates = [getDate(a) for a in new_assets]

    logging.info('Existing assets: {}, new: {}, max: {}'.format(
        len(existing_assets), len(new_assets), MAX_ASSETS))

    # Delete excess assets
    deleteExcessAssets(existing_dates+new_dates, MAX_ASSETS)

    # Update Resource Watch
    updateResourceWatch()

    logging.info('SUCCESS')
