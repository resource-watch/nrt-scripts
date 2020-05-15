from __future__ import unicode_literals

import os
import sys
import urllib
import datetime
import logging
import subprocess
import eeUtil
import urllib.request
import requests
from bs4 import BeautifulSoup
import copy
import numpy as np
import ee
import time

# This dataset owner has created a subset of the data specifically for our needs on Resource Watch.
# If you want to switch back to pulling from the original source, set the following variable to False.
rw_subset = True

# Version of model to use
# h0 version is 3-hourly data
# h3 version is 6-hourly data
VERSION = 'h3'

# get time intervals in each day - specific to version number
if VERSION == 'h0':
    TIME_HOURS = list(range(0, 24, 3))
elif VERSION == 'h3':
    TIME_HOURS = list(range(0, 24, 6))

if rw_subset==True:
    # url for air quality data
    SOURCE_URL = 'https://www.acom.ucar.edu/waccm/subsets/resourcewatch/f.e22.beta02.FWSD.f09_f09_mg17.cesm2_2_beta02.forecast.001.cam.%s.{date}_surface_subset.nc' % VERSION
    # list variables (as named in netcdf) that we want to pull
    VARS = ['NO2', 'CO', 'O3', 'SO2', 'PM25', 'bc_a4']
    # list of pressure levels available in the netcdf for each variable
    # the RW subset only contains surface level data
    NUM_AVAILABLE_LEVELS = [1, 1, 1, 1, 1, 1]
    # which pressure level do we want to use for each variable
    DESIRED_LEVELS = [1, 1, 1, 1, 1, 1]
else:
    # url for air quality data
    SOURCE_URL = 'https://www.acom.ucar.edu/waccm/DATA/f.e21.FWSD.f09_f09_mg17.forecast.001.cam.%s.{date}-00000.nc' % VERSION
    # list variables (as named in netcdf) that we want to pull
    VARS = ['NO2', 'CO', 'O3', 'SO2', 'PM25_SRF', 'bc_a4']
    # list of pressure levels available in the netcdf for each variable
    # most variables have 88 pressure levels; PM 2.5 only has one level (surface)
    # need to specify which pressure level of data we was for each (level 1 being the lowest pressure)
    # the highest level is the highest pressure (992.5 hPa), and therefore, closest to surface level
    NUM_AVAILABLE_LEVELS = [88, 88, 88, 88, 1, 88]
    # which pressure level do we want to use for each variable
    DESIRED_LEVELS = [88, 88, 88, 88, 1, 88]

# subdataset to be converted to tif
# should be of the format 'NETCDF:"filename.nc":variable'
SDS_NAME = 'NETCDF:"{fname}":{var}'

# nodata value for netcdf
NODATA_VALUE = None

# name of data directory in Docker container
DATA_DIR = 'data'

# name of collection in GEE where we will upload the final data
COLLECTION = '/projects/resource-watch-gee/cit_038_WACCM_atmospheric_chemistry_model'
# generate name for dataset's parent folder on GEE which will be used to store
# several collections - one collection per variable
PARENT_FOLDER = COLLECTION
# generate generic string that can be formatted to name each variable's GEE collection
EE_COLLECTION_GEN = PARENT_FOLDER + '/{var}'
# generate generic string that can be formatted to name each variable's asset name
FILENAME = PARENT_FOLDER.split('/')[-1] + '_{var}_{date}'
# specify Google Cloud Storage folder name
GS_FOLDER = COLLECTION[1:]

# do you want to delete everything currently in the GEE collection when you run this script?
CLEAR_COLLECTION_FIRST = True

# how many days of data do we want to use?
# MAXDAYS = 1 only fetches today
# maximum value of 10: today plus 9 days of forecast
MAX_DAYS = 2

# If we don't want to show the last time available for the last day, how many time steps before
# the last is the time we want to show?
# For now, we want to show 12:00. Because the version we use is on 6-hour intervals, we want to pull 1 time step
# before the last for the second day (last time would be 18:00 for this version)
TS_FROM_END = 1

# format of date used in GEE
DATE_FORMAT = '%y-%m-%d_%H%M'

# Resource Watch dataset API IDs
# Important! Before testing this script:
# Please change these IDs OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on different datasets on Resource Watch
DATASET_IDS = {
    'NO2':'2c2c614a-8678-443a-8874-33335771ecc0',
    'CO':'266ed113-396c-4c69-885a-ead30df95810',
    'O3':'ec011d66-a99b-425c-accd-d04e75966094',
    'SO2':'d82186a4-7885-4fa9-9e82-26799853093b',
    'PM25':'348e4d57-a345-411d-986e-5863fffebda7',
    'bc_a4':'fe0a0042-8430-419b-a60f-9b69ec81a0ec'
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

def getCollectionName(var):
    '''
    get GEE collection name
    INPUT   var: variable to be used in asset name (string)
    RETURN  GEE collection name for input date (string)
    '''
    return EE_COLLECTION_GEN.format(var=var)

def getAssetName(var, date):
    '''
    get asset name
    INPUT   var: variable to be used in asset name (string)
            date: date in the format of the DATE_FORMAT variable (string)
    RETURN  GEE asset name for input date (string)
    '''
    collection = getCollectionName(var)
    return os.path.join(collection, FILENAME.format(var=var, date=date))

def getTiffname(file, hour, var):
    '''
    generate names for tif files that we are going to create from netcdf
    INPUT   file: netcdf filename (string)
            hour: integer representing hour to be used in tif name, 0-23 (integer)
            var: variable to be used in tif name (string)
    RETURN  name: file name to save tif file created from netcdf (string)
    '''
    # generate time string to be used in tif file name
    # if hour is a single digit, add a zero before to make it a 4-digit time
    if hour < 10:
        time_str = '0' + str(hour) + '00'
    else:
        time_str = str(hour) + '00'
    # generate date string to be used in tif file name
    date = os.path.splitext(file)[0][-10:]
    # generate name for tif file
    name = os.path.join(DATA_DIR, FILENAME.format(var=var, date=date)) + '_' + time_str
    return name

def getFilename(date):
    '''
    generate file name to store the netcdf in after download
    INPUT   date: date of file in the format YYYY-MM-DD (string)
    RETURN  netcdf filename for date (string)
    '''
    return os.path.join(DATA_DIR, '{}.nc'.format(
        FILENAME.format(var='all_vars', date=date)))

def getDateTimeString(filename):
    '''
    get date and time from filename (last 13 characters of filename after removing extension)
    INPUT   filename: file name that ends in a date of the format YY-MM-DD_HHMM (string)
    RETURN  date in the format YY-MM-DD_HHMM (string)
    '''
    return os.path.splitext(os.path.basename(filename))[0][-13:]

def getDate_GEE(filename):
    '''
    get date from Google Earth Engine asset name
    INPUT   filename: asset name that ends in a date of the format YY-MM-DD_HHMM (string)
    RETURN  date in the format YY-MM-DD (string)
    '''
    return os.path.splitext(os.path.basename(filename))[0][-13:-5]

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

def getNewDates(existing_dates):
    '''
    Get new dates we want to try to fetch data for
    INPUT   existing_dates: list of dates that we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
    RETURN  new_dates: list of new dates we want to try to get, in the format of the DATE_FORMAT variable (list of strings)
            last_date: name of file for last date of forecast (string)
    '''
    # get a list of files available from the source
    url = os.path.split(SOURCE_URL)[0]
    available_files = list_available_files(url, ext='.nc')[-9:]
    # get the most recent available file
    recent_forecast_start = available_files[0]
    # pull the date that the most recent forecast was created on
    recent_forecast_start_date = recent_forecast_start[-26:-18]

    # sort and get the forecast start date for the data we already have
    if existing_dates:
        existing_dates.sort()
        existing_forecast_start_date = existing_dates[0]
    else:
        existing_forecast_start_date = None

    # if we have the most recent forecast, we don't need new data
    if existing_forecast_start_date==recent_forecast_start_date:
        new_dates = []
    # otherwise, we need to go get the days of interest
    else:
        # get start date of forecast through the day we want to show on RW
        recent_files = available_files[:MAX_DAYS]
        new_dates = [file[-28:-18] for file in recent_files]
    # get last date because this file only has one time output so we need to process it differently
    last_date = available_files[-1]
    return new_dates, last_date

def getBands(var_num, file, last_date):
    '''
    get bands for all available times in netcdf at the desired pressure level
    INPUT   var_num: index number for variable we are currently processing (integer)
            file: file we are currently processing (string)
            last_date: name of file for last date of forecast (string)
    RETURN  bands: bands in netcdf for all available times at desired pressure level (list of integers)
    '''
    # get specified pressure level for the current variable
    level = DESIRED_LEVELS[var_num]
    # the pressure and time dimensions are flattened into one dimension in the netcdfs
    # for the pressure level that we chose, we want all the times available
    # we will make a list of the bands that have the data we need
    if VERSION == 'h0':
        # h0 has 8 times - get all times at specified pressure level
        bands = [x * NUM_AVAILABLE_LEVELS[var_num] + level for x in
                 list(range(0, 8))]
    elif VERSION == 'h3':
        # h3 has 4 times - get all times at specified pressure level
        bands = [x * NUM_AVAILABLE_LEVELS[var_num] + level for x in
                 list(range(0, 4))]

    # if we are on the last file, only one time is available
    if file[-13:-3] == last_date:
        bands = [x * NUM_AVAILABLE_LEVELS[var_num] + level for x in
                 list(range(0, 1))]
    return bands

def convert(files, var_num, last_date):
    '''
    Convert netcdf files to tifs
    INPUT   files: list of file names for netcdfs that have already been downloaded (list of strings)
            var_num: index number for variable we are currently processing (integer)
            last_date: name of file for last date of forecast (string)
    RETURN  all_tifs: list of file names for tifs that have been generated - all available times (list of strings)
            tifs: list of file names for tifs that have been generated - through desired endpoint (list of strings)
    '''
    # get name of variable we are converting files for
    var = VARS[var_num]
    # make an empty list to store the names of tif files that we create
    all_tifs = []
    for f in files:
        # get list of bands in netcdf for all available times at desired pressure level
        bands = getBands(var_num, f, last_date)
        logging.info('Converting {} to tiff'.format(f))
        for band in bands:
            # generate the subdatset name for current netcdf file for a particular variable
            sds_path = SDS_NAME.format(fname=f, var=var)
            '''
            Google Earth Engine needs to get tif files with longitudes of -180 to 180.
            These files have longitudes from 0 to 360. I checked this using gdalinfo.
            I downloaded a file onto my local computer and in command line, ran:
                    gdalinfo NETCDF:"{file_loc/file_name}":{variable}
            with the values in {} replaced with the correct information.
            I looked at the 'Corner Coordinates' that were printed out.

            Since the longitude is in the wrong format, we will have to fix it. First,
            we will convert the files from netcdfs to tifs using gdal_translate,
            then we will fix the longitude values using gdalwarp.
            '''
            #generate names for tif files that we are going to create from netcdf
            file_name_with_time = getTiffname(file=f, hour=TIME_HOURS[bands.index(band)], variable=var)
            #create a file for the initial tif that is in the 0 to 360 longitude format
            tif_0_360 = '{}_0_360.tif'.format(file_name_with_time)
            # create a file name for the final tif that is in the -180 to 180 file format
            tif = '{}.tif'.format(file_name_with_time)

            # first we will translate this file from a netcdf to a tif
            cmd = ['gdal_translate', '-b', str(band), '-q', '-a_nodata', str(NODATA_VALUE), '-a_srs', 'EPSG:4326', sds_path, tif_0_360] #'-q' means quiet so you don't see it
            subprocess.call(cmd)

            # Now we will fix the longitude. To do this we need the x and y resolution.
            # I also got x and y res for data set using the gdalinfo command described above.
            xres='1.250000000000000'
            yres= '-0.942408376963351'
            cmd_warp = ['gdalwarp', '-t_srs', 'EPSG:4326', '-tr', xres, yres, tif_0_360, tif, '-wo', 'SOURCE_EXTRA=1000', '--config', 'CENTER_LONG', '0']
            subprocess.call(cmd_warp) #using the gdal from command line from inside python

            # add the new tif files to the list of tifs
            all_tifs.append(tif)
    # If we don't want to use all the times available, we should have set the TS_FROM_END parameter at the beginning.
    if TS_FROM_END>0:
        # from the list of all the tifs created, get a list of the tifs you actually want to upload
        # this should be all the files through the desired end point
        tifs = all_tifs[:-TS_FROM_END]
    return all_tifs, tifs

def fetch(new_dates, unformatted_source_url):
    '''
    Fetch files by datestamp
    INPUT   new_dates: list of dates we want to try to fetch, in the format YYYY-MM-DD (list of strings)
            unformatted_source_url: url for air quality data (string)
    RETURN  files: list of file names for netcdfs that have been downloaded (list of strings)
    '''
    # make an empty list to store names of the files we downloaded
    files = []
    # Loop over the new dates, check if there is data available, and download netcdfs
    for date in new_dates:
        # Set up the url of the filename to download
        url = unformatted_source_url.format(date=date)
        # Create a file name to store the netcdf in after download
        f = getFilename(date)
        # get file name of source file you are about to try to download
        file_name = os.path.split(url)[1]
        # get list of files available from the source
        file_list = list_available_files(os.path.split(url)[0], ext='.nc')
        # if the file is available, download it
        if file_name in file_list:
            logging.info('Retrieving {}'.format(file_name))
            # try to download file
            try:
                # download files from url and put in specified file location (f)
                urllib.request.urlretrieve(url, f)
                # add file name/location to list of files downloaded
                files.append(f)
                logging.info('Successfully retrieved {}'.format(file_name))# gives us "Successully retrieved file name"
            # if download fails, log an error
            except Exception as e:
                logging.error('Unable to retrieve data from {}'.format(url))
                logging.error(e)
                logging.debug(e)
        # if file is not available, log that
        else:
            logging.info('{} not available yet'.format(file_name))
    return files

def processNewData(files, var_num, last_date):
    '''
    Process and upload clean new data
    INPUT   files: list of file names for netcdfs that have been downloaded (list of strings)
            var_num: index number for variable we are currently processing (integer)
            last_date: name of file for last date of forecast (string)
    RETURN  assets: list of file names for netcdfs that have been downloaded (list of strings)
    '''
    # get name of variable we are processing files for
    var = VARS[var_num]
    # if files is empty list do nothing, otherwise, process data
    if files:
        logging.info('Converting files')
        # Convert netcdfs to tifs
        all_tifs, tifs = convert(files, var_num, last_date) # naming tiffs
        # get new list of date strings (in case order is different) from the tifs
        dates = [getDateTimeString(tif) for tif in tifs]
        # generate datetime objects for each tif date
        datestamps = [datetime.datetime.strptime(date, DATE_FORMAT) for date in dates]
        # Get a list of the names we want to use for the assets once we upload the files to GEE
        assets = [getAssetName(var, date) for date in dates]

        logging.info('Uploading files:')
        for asset in assets:
            logging.info(os.path.split(asset)[1])
        # Upload new files (tifs) to GEE
        eeUtil.uploadAssets(tifs, assets, GS_FOLDER, datestamps, timeout=3000)

        # Delete local tif files
        logging.info('Cleaning local TIFF files')
        delete_local(ext='.tif')

        return assets


def checkCreateCollection(VARS):
    '''
    List assets in collection if it exists, else create new collection
    INPUT   VARS: list variables (as named in netcdf) that we want to check collections for (list of strings)
    RETURN  existing_dates_all_vars: list of dates, in the format of the DATE_FORMAT variable, that exist for all variable collections in GEE (list of strings)
            existing_dates_by_var: list of dates, in the format of the DATE_FORMAT variable, that exist for each individual variable collection in GEE (list containing list of strings for each variable)
    '''
    # create a master list (not variable-specific) to store the dates for which all variables already have data for
    existing_dates = []
    # create an empty list to store the dates that we currently have for each AQ variable
    # will be used in case the previous script run crashed before completing the data upload for every variable.
    existing_dates_by_var = []
    # loop through each variables that we want to pull
    for var in VARS:
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
            dates = [getDate_GEE(a) for a in existing_assets]
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
        count = date_count / len(TIME_HOURS)
        # If this count is less than the number of variables we have, one of the variables did not finish
        # uploading for this date, and we need to re-upload this file.
        if count < len(VARS):
            # remove this from the list of existing dates for all variables
            existing_dates_all_vars.remove(date)
    return existing_dates_all_vars, existing_dates_by_var

def get_most_recent_date(all_assets):
    '''
    Get most recent data we have assets for
    INPUT   all_assets: list of all the assets currently in the GEE collection (list of strings)
    RETURN  most_recent_date: most recent date in GEE collection (datetime)
    '''
    # sort these dates oldest to newest
    all_assets.sort()
    # get the most recent date (last in the list) and turn it into a datetime
    most_recent_date = datetime.datetime.strptime(all_assets[-1][-13:], DATE_FORMAT)
    return most_recent_date

def get_forecast_run_date(var):
    '''
    Get the date that the most recent forecast was run from
    INPUT   var: variable for which we are pulling forecast run date (string)
    RETURN  most_recent_forecast_date: date of most recent forecast run (datetime)
    '''
    # pull existing assets in the collection
    collection = getCollectionName(var)
    existing_assets = eeUtil.ls(collection)
    # sort these dates oldest to newest
    existing_assets.sort()
    # get the forecast run date (first in the list) and turn it into a datetime
    most_recent_forecast_date = datetime.datetime.strptime(existing_assets[0][-13:], DATE_FORMAT)
    return most_recent_forecast_date

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

def initialize_ee():
    '''
    Initialize eeUtil and ee modules
    '''
    # get GEE credentials from env file
    GEE_JSON = os.environ.get("GEE_JSON")
    _CREDENTIAL_FILE = 'credentials.json'
    GEE_SERVICE_ACCOUNT = os.environ.get("GEE_SERVICE_ACCOUNT")
    with open(_CREDENTIAL_FILE, 'w') as f:
        f.write(GEE_JSON)
    auth = ee.ServiceAccountCredentials(GEE_SERVICE_ACCOUNT, _CREDENTIAL_FILE)
    ee.Initialize(auth)

def updateResourceWatch():
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date', flushing the tile cache, and updating any dates on layers
    '''
    for var_num in range(len(VARS)):
        # get variable we are updating layers for
        var = VARS[var_num]
        # Get most recent forecast run date
        most_recent_date = get_forecast_run_date(var)
        # Get the current 'last update date' from the dataset on Resource Watch
        current_date = getLastUpdate(DATASET_IDS[var])
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

def delete_local(ext=None):
    '''
    This function will delete local files in the Docker container with a specific extension, if specified.
    If no extension is specified, all local files will be deleted.
    INPUT   ext: optional, file extension for files you want to delete, ex: '.tif' (string)
    '''
    try:
        if ext:
            [file for file in os.listdir(DATA_DIR) if file.endswith(ext)]
        else:
            files = os.listdir(DATA_DIR)
        for f in files:
            logging.info('Removing {}'.format(f))
            os.remove(DATA_DIR+'/'+f)
    except NameError:
        logging.info('No local files to clean.')

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Initialize eeUtil and ee modules
    eeUtil.initJson()
    initialize_ee()

    # Clear collection in GEE if desired
    if CLEAR_COLLECTION_FIRST:
        clearCollectionMultiVar()

    # Check if collection exists. If not, create it.
    # Return a list of dates that exist for all variables collections in GEE (existing_dates),
    # as well as a list of which dates exist for each individual variable (existing_dates_by_var).
    # The latter will be used in case the previous script run crashed before completing the data upload for every variable.
    logging.info('Getting existing dates.')
    existing_dates, existing_dates_by_var = checkCreateCollection(VARS)

    # Get a list of the dates that are available, minus the ones we have already uploaded correctly for all variables.
    logging.info('Getting new dates to pull.')
    all_new_dates, last_date = getNewDates(existing_dates)

    # if new data is available, clear the collection because we want to store the most
    # recent forecast, not the old forecast
    if all_new_dates:
        logging.info('New forecast available.')
        clearCollectionMultiVar()
    else:
        logging.info('No new forecast.')

    # The Docker container isonly big enough to hold 3 files at once,
    # so break into groups to process
    new_date_groups = [all_new_dates[x:x+3] for x in range(0, len(all_new_dates), 3)]
    for new_dates in new_date_groups:
        # Fetch new files
        logging.info('Fetching files for {}'.format(new_dates))
        files = fetch(new_dates, SOURCE_URL)

        # Process data, one variable at a time
        for var_num in range(len(VARS)):
            # get variable name
            var = VARS[var_num]

            # Process new data files, delete all forecast assets currently in collection
            new_assets = processNewData(files, var_num, last_date)

            logging.info('New assets for {}: {}'.format(var, len(new_assets)))
            logging.info('SUCCESS for {}'.format(var))

    # Delete local netcdf files
    delete_local()

    # Update Resource Watch
    updateResourceWatch()

    logging.info('SUCCESS')
