from __future__ import unicode_literals

import sys
import urllib
import datetime
import logging
import subprocess
import eeUtil
import urllib.request
from bs4 import BeautifulSoup
import os
from http.cookiejar import CookieJar
import requests
import time
from dateutil.relativedelta import relativedelta

# url for snow cover data
SOURCE_URL = 'https://n5eil01u.ecs.nsidc.org/MOST/MOD10CM.006/{date}'
    
# subdataset to be converted to tif
# should be of the format 'HDF4_EOS:EOS_GRID:"filename.hdf":variable'
SDS_NAME = 'HDF4_EOS:EOS_GRID:"{fname}":MOD_CMG_Snow_5km:Snow_Cover_Monthly_CMG'

# filename format for GEE
FILENAME = 'cli_021_{date}'

# nodata value for hdf
NODATA_VALUE = 255

# name of data directory in Docker container
DATA_DIR = 'data'

# name of folder to store data in Google Cloud Storage
GS_FOLDER = 'cli_021_snow_cover_monthly'

# name of collection in GEE where we will upload the final data
EE_COLLECTION = 'cli_021_snow_cover_monthly'

# do you want to delete everything currently in the GEE collection when you run this script?
CLEAR_COLLECTION_FIRST = False

# do you want to delete the hdf and tif files downloaded to the Docker container once the tif files are uploaded to GEE?
DELETE_LOCAL = True

# how many assets can be stored in the GEE collection before the oldest ones are deleted?
MAX_ASSETS = 8

# format of date (used in source data files)
DATE_FORMAT_HDF = '%Y.%m.%d'

# format of date (used in GEE)
DATE_FORMAT = '%Y%m%d'

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = '23f29e9a-ca07-4c08-a018-28a25af14b49'

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


def getAssetName(date):
     '''
     get asset name
     INPUT   date: date in the format of the DATE_FORMAT variable (string)
     RETURN  GEE asset name for input date (string)
     '''
     return os.path.join(EE_COLLECTION, FILENAME.format(date=date))


def getFilename(date):
     '''
     get hdf filename to save source file as
     INPUT   date: date in the format of the DATE_FORMAT variable (string)
     RETURN  file name to save hdf from source under (string)
     '''
     return os.path.join(DATA_DIR, '{}.hdf'.format(FILENAME.format(date=date)))


def getDate(filename):
     '''
     get date from filename (last 8 characters of filename after removing extension)
     INPUT   filename: file name that ends in a date of the format YYYYMMDD (string)
     RETURN  date in the format YYYYMMDD (string)
     '''
     return os.path.splitext(os.path.basename(filename))[0][-8:]


def getNewDates(exclude_dates):
     '''
     Get new dates we want to try to fetch data for
     INPUT   exclude_dates: list of dates that we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
     RETURN  new_dates: list of new dates we want to try to get, in the format of the DATE_FORMAT variable (list of strings)
     '''
     # create empty list to store dates we want to fetch
     new_dates = []
     # get today's date, then replace day to be the first of the current month
     date = datetime.date.today().replace(day=1)
     # turn the date into a string in the same format used for the dates in the exclude_dates list
     exclude_datestr = date.strftime(DATE_FORMAT)
     # if the date string is not the list of dates we already have, add it to the list of new dates to try and fetch
     while exclude_datestr not in exclude_dates:
          # change the format of date to match the format used in source data files
          datestr = date.strftime(DATE_FORMAT_HDF)
          # add to list of new dates
          new_dates.append(datestr)
          # subtract 1 month from date to go back to next previous month
          date = date - relativedelta(months=1)
          # change the format of date to match the format used in GEE
          exclude_datestr = date.strftime(DATE_FORMAT)
     return new_dates


def convert(files):
     '''
     Convert hdf files to tifs
     INPUT   files: list of file names for hdfs that have already been downloaded (list of strings)
     RETURN  tifs: list of file names for tifs that have been generated (list of strings)
     '''

     # create and empty list to store the names of the tifs we generate
     tifs = []

     #go through each hdf file and translate
     for f in files:
          # generate the subdatset name for current hdf file
          sds_path = SDS_NAME.format(fname=f)
          # generate a name to save the tif file we will translate the hdf file into
          tif = '{}.tif'.format(os.path.splitext(f)[0])
          # tranlate the hdf into a tif
          cmd = ['gdal_translate','-q', '-a_nodata', '255', sds_path, tif]
          logging.debug('Converting {} to {}'.format(f, tif))
          subprocess.call(cmd)
          # add the new tif files to the list of tifs
          tifs.append(tif)
     return tifs


def fetch(new_dates):
     '''
     Fetch files by datestamp
     INPUT   new_dates: list of dates we want to try to fetch, in the format YYYY.MM.DD (list of strings)
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
     # go through each input date
     for date in new_dates:
          # get the url where data for the given date is stored at the source
          url = getUrl(date)
          # change date string from format used in HDF to format used in GEE
          # input date is initially a string, strptime changes it to datetime object, strftime reformats into string
          file_date = datetime.datetime.strptime(date, DATE_FORMAT_HDF).strftime(DATE_FORMAT)
          # get the filename we want to save the file under locally
          f = getFilename(file_date)
          try:
               # open the url
               response = urllib.request.urlopen(url)
               # read the opened url
               content = response.read()
               # use BeautifulSoup to read the content as a nested data structure
               soup = BeautifulSoup(content, 'html.parser')

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
               # convert the list to a set to remove duplicates, convert it back to list again
               hdfs = list(set(hdfs))
               # get the first item from the list
               hdf = hdfs[0]
               # join the source url with the id of each hdf to generate complete URLs
               # for each hdf file download
               url = os.path.join(url, hdf)

               try:
                    # try to download the data
                    urllib.request.urlretrieve(url, f)
                    # if successful, add the file to the list of files we have downloaded
                    files.append(f)
                    # if successful, log that the file was downloaded successfully
                    logging.info('Successfully retrieved {}'.format(f))

               except Exception as e:
                    # if unsuccessful, log an error that the file was not downloaded
                    logging.error('Unable to retrieve data from {}'.format(url))
                    logging.debug(e)

          except Exception as e:
               # if unsuccessful, log that no data were found for the input date
               # (could be one of the days not covered by this data set)
               logging.debug('No data found for date {}, could be one of the days not covered by this data set (reminder, only updates once every 8 days)'.format(date))
               logging.debug(e)
     return files

def processNewData(existing_dates):
     '''
     fetch, process, upload, and clean new data
     INPUT   existing_dates: list of dates we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
     RETURN  assets: list of file names for hdfs that have been downloaded (list of strings)
     '''
     # Get list of new dates we want to try to fetch data for
     new_dates = getNewDates(existing_dates)

     # fetch new files
     logging.info('Fetching files')
     files = fetch(new_dates)

     # if we have successfully been able to fetch new data files
     if files:
          # Convert new files from hdf to tif files
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

          # delete local files
          if DELETE_LOCAL:
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
     INPUT   collection: GEE collection to check or create (string)
     RETURN  list of assets in collection (list of strings)
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

     logging.info('SUCCESS')
