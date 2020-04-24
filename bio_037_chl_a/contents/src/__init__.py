from __future__ import unicode_literals

import sys
import urllib
import datetime
import logging
import subprocess
import eeUtil
import urllib.request
from netCDF4 import Dataset
import os
import calendar
import numpy as np
import requests
import time

# url for chlorophyll concentration data
SOURCE_URL = 'https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A{date}.L3m_MO_CHL_chlor_a_4km.nc'

# subdataset to be converted to tif
# should be of the format 'NETCDF:"filename.nc":variable'
SDS_NAME = 'NETCDF:"{fname}":chlor_a'

# filename format for GEE
FILENAME = 'bio_037_chl_a_{date}'

# nodata value for netcdf
NODATA_VALUE = -32767.0

# name of data directory in Docker container
DATA_DIR = 'data'

# name of folder to store data in Google Cloud Storage
GS_FOLDER = 'bio_037_chl_a'

# name of collection in GEE where we will upload the final data
EE_COLLECTION = 'bio_037_chl_a'

# do you want to delete everything currently in the GEE collection when you run this script?
CLEAR_COLLECTION_FIRST = False

# how many assets can be stored in the GEE collection before the oldest ones are deleted?
MAX_ASSETS = 8

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = 'd4e91298-b994-4e2c-947c-4f6486a66f30'

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
    while try_num<4:
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
    INPUT   date: date in the format {start year}{julian day of the first of the month}{end year}{julian day of the end of the month} (string)
    RETURN  source url to download data, formatted for the input date (string)
    '''
    return SOURCE_URL.format(date=date)

def getAssetName(date):
    '''
    get asset name
    INPUT   date: date in the format {start year}{julian day of the first of the month}{end year}{julian day of the end of the month} (string)
    RETURN  GEE asset name for input date (string)
    '''
    # Example asset name: users/resourcewatch_wri/bio_037_chl_a/bio_037_chl_a_20181822018212
    return os.path.join(EE_COLLECTION, FILENAME.format(date=date))

def getFilename(date):
    '''
    get netcdf filename to save source file as
    INPUT   date: date in the format {start year}{julian day of the first of the month}{end year}{julian day of the end of the month} (string)
    RETURN  file name to save netcdf from source under (string)
    '''
    return os.path.join(DATA_DIR, '{}.nc'.format(FILENAME.format(date=date)))
        
def getDate(filename):
    '''
    get date from filename (last 14 characters of filename after removing extension)
    INPUT   filename: file name that ends in a date of the format YYYYDDDYYYYDDD (string)
    RETURN  date in the format YYYYMMDD (string)
    '''
    return os.path.splitext(os.path.basename(filename))[0][14:28]

def getNewDates(exclude_dates):
    '''
    Get new dates we want to try to fetch data for
    INPUT   exclude_dates: list of dates that we already have in GEE, in the format YYYYDDDYYYYDDD (list of strings)
    RETURN  new_dates: list of new dates we want to try to get, in the format YYYYDDDYYYYDDD (list of strings)
            new_datetime: list of new dates we want to try to get, in the format '%Y%m%d' (list of strings)
    '''
    # create empty list to store dates we want to fetch
    new_dates = []
    new_datetime = []
    # start with today's date
    date = datetime.date.today()    # example output: 2020-04-07
    for i in range(MAX_ASSETS):
        # get current month from today's date
        current_month = date.month        # example output: 4
        # replace day to be the first of the current month
        date = date.replace(day=1)  # example output: 2020-04-01
        # if the current month is January
        if current_month==1:
            current_year = date.year
            # subtract 1 year from date to go back to next previous year
            date = date.replace(year=current_year-1)
            # replace month to be the twelfth of the current year
            date = date.replace(month=12)
        # if the current month is anything other than January
        # the process of going back to previous month is different when current month is not January
        # since we do not have to change year in that case
        else:
            # subtract 1 month from date to go back to next previous month
            date = date.replace(month=current_month-1)
        # get the first day of the current month for start date
        startdate = date.replace(day=1)     # example output: 2020-03-01
        # get the last day of the current month for end date
        enddate = date.replace(day=calendar.monthrange(startdate.year, startdate.month)[1]) # example output: 2020-03-31
        
        # get equivalent julian day from the start date
        start_jday = str(startdate.timetuple().tm_yday) # example output: 61
        # julian days are 3 digit numbers
        # if start_jday is 2 digit then add a "0" in the begining to make it 3 digit
        if len(start_jday)==2:
            start_jday = '0'+start_jday                 # example output: 061
        # if start_jday is 1 digit then add two "0" in the begining to make it 3 digit
        elif len(start_jday)==1:
            start_jday = '00'+start_jday
        # get equivalent julian day from the end date
        end_jday = str(enddate.timetuple().tm_yday) # example output: 91
        if len(end_jday)==2:                        # example output: 091
            end_jday = '0'+end_jday
        elif len(end_jday)==1:
            end_jday = '00'+end_jday
        
        # combine year and days to get start and end date in the format YYYYDDD (julian format)
        start = str(startdate.year)+ start_jday     # example output: 2020061
        end = str(enddate.year)+ end_jday           # example output: 2020091
        # merge start and end date to match the file naming convention that the data source uses
        datestr = start+end                         # example output: 20200612020091

        # if the date string is not the list of dates we already have, add it to the list of new dates to try and fetch
        if datestr not in exclude_dates:
            new_dates.append(datestr)   
            # get enddate in the format '%Y%m%d' so that it can be interepreted by GEE
            # example output: (2020, 3, 31, 0, 0) for the input: 2020-03-31
            new_datetime.append(datetime.datetime.combine(enddate,datetime.datetime.min.time()))

    return new_dates,new_datetime

def convert(files):
    '''
    Convert netcdf files to tifs
    INPUT   files: list of file names for netcdfs that have already been downloaded (list of strings)
    RETURN  tifs: list of file names for tifs that have been generated (list of strings)
    '''

    # create and empty list to store the names of the tifs we generate
    tifs = []
    # go through each netcdf file and translate
    for f in files:
        # open the netcdf file in read mode
        dat = Dataset(f,'r+')
        # extract chlorophyll concentration data from netcdf file
        chlor = dat.variables['chlor_a']
        # apply natural logarithm to data, this is so that when interpolating colors in the SLD style, 
        # the difference between 0.01 and 0.03 is the same as 10 and 30 mg/m^3
        log = np.ma.log(chlor[:])
        chlor[:] = log
        # close the netcdf file 
        dat.close()
        # generate the subdatset name for current netcdf file
        sds_path = SDS_NAME.format(fname=f)
        # generate a name to save the tif file we will translate the netcdf file into
        tif = '{}.tif'.format(os.path.splitext(f)[0])
        # tranlate the netcdf into a tif
        cmd = ['gdal_translate','-q', '-a_nodata', str(NODATA_VALUE), '-a_srs', 'EPSG:4326', sds_path, tif] 
        logging.debug('Converting {} to {}'.format(f, tif))
        # using gdal from command line from inside python
        subprocess.call(cmd) 
        # add the new tif files to the list of tifs
        tifs.append(tif)
    return tifs

def fetch(dates):
    '''
    Fetch files by datestamp
    INPUT   dates: list of dates we want to try to fetch, in the format YYYYMMDD (list of strings)
    RETURN  files: list of file names for netcdfs that have been downloaded (list of strings)
    '''
    # make an empty list to store names of the files we downloaded
    files = []
    # go through each input date
    for date in dates:
        # get the url to download the file from the source for the given date
        url = getUrl(date)
        # get the filename we want to save the file under locally
        f = getFilename(date)
        logging.debug('Fetching {}'.format(url))
        try:
            # try to download the data
            urllib.request.urlretrieve(url, f)
            # if successful, add the file to the list of files we have downloaded
            files.append(f)
            logging.info('Successfully retrieved {}'.format(f))# gives us "Successully retrieved file name"

        except Exception as e:
            # if unsuccessful, log that the file was not downloaded
            # NASA does not upload the previous month's chlorophyll until the middle of the next month
            # error is raised when trying to access this file via URL as the file has not been uploaded by NASA
            logging.info('Unable to retrieve data from {}, most likely NASA has not uploaded file'.format(url))A
            logging.error('Unable to retrieve data from {}'.format(url))
            logging.debug(e)

    return files

def processNewData(existing_dates):
    '''
    fetch, process, upload, and clean new data
    INPUT   existing_dates: list of dates we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
    RETURN  assets: list of file names for netcdfs that have been downloaded (list of strings)
    '''
    # Get list of new dates we want to try to fetch data for
    new_dates,new_datetimes = getNewDates(existing_dates)

    # Fetch new files
    logging.info('Fetching files')
    files = fetch(new_dates)

    # If we have successfully been able to fetch new data files
    if files:
        # Convert new files from netcdf to tif files
        logging.info('Converting files to tifs')
        tifs = convert(files)

        logging.info('Uploading files')
        # Get a list of the dates we have to upload from the tif file names
        dates = [getDate(tif) for tif in tifs] 
        # Get a list of the names we want to use for the assets once we upload the files to GEE
        assets = [getAssetName(date) for date in dates]
        # Upload new files (tifs) to GEE
        eeUtil.uploadAssets(tifs, assets, GS_FOLDER, new_datetimes) 

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
    Get most recent data it
    INPUT   collection: GEE collection to check dates for (string)
    RETURN  most_recent_date: most recent date in GEE collection (datetime)
    '''
    # get list of assets in collection
    existing_assets = checkCreateCollection(collection)
    # get a list of strings of dates in the collection
    existing_dates = [getDate(a) for a in existing_assets]
    # sort these dates oldest to newest
    existing_dates.sort()
    # get the most recent date (last in the list)
    # get last 7 characters from most recent date in the format YYYYDDD ({end year}{julian day of the end of the month})
    most_recent_date_julian = existing_dates[-1][-7:]
    # turn the most recent date into a datetime in julian format
    most_recent_date = datetime.datetime.strptime(most_recent_date_julian, '%Y%j').date()
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

    logging.info('SUCCESS')
