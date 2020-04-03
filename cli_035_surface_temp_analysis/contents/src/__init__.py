from __future__ import unicode_literals

import os
import sys
import urllib.request
import datetime
from dateutil import parser
import logging
from netCDF4 import Dataset
import rasterio as rio
import numpy as np
import eeUtil
import requests
import time


# url for surface temperature analysis data
SOURCE_URL = 'ftp://ftp.cdc.noaa.gov/Datasets/gistemp/combined/250km/{target_file}'

# name of target_file to be joined with SOURCE_URL
SOURCE_FILENAME = 'air.2x2.250.mon.anom.comb.nc'

# filename format for GEE
FILENAME = 'cli_035_{date}'

# variable name in netcdf file for surface temperature analysis data
VAR_NAME = 'air'

# variable name in netcdf file for time range
TIME_NAME = 'time'

# nodata value for netcdf
NODATA_VALUE = None


# attribute name for missing value in netcdf file
MISSING_VALUE_NAME = "missing_value"

# name of data directory in Docker container
DATA_DIR = os.path.join(os.getcwd(),'data')

# name of folder to store data in Google Cloud Storage
GS_FOLDER = 'cli_035_surface_temp_analysis'

# name of collection in GEE where we will upload the final data
EE_COLLECTION = 'cli_035_surface_temp_analysis'

# do you want to delete everything currently in the GEE collection when you run this script?
CLEAR_COLLECTION_FIRST = False

# how many assets can be stored in the GEE collection before the oldest ones are deleted?
MAX_ASSETS = 36

# format of date (used in both the source data files and GEE)
DATE_FORMAT = '%Y%m15'

# time step for searching new data; if search starts with today's date then the next search will
# be for 30 days before today's date 
TIMESTEP = {'days': 30}

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = '0df6dbcd-ee6a-401b-bf1d-2a67dad09f05'

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
        "dataLastUpdated": date.isoformat()
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

def getUrl(target_file):
    '''
    format source url with target file name
    INPUT   target_file: name of target file (string)
    RETURN  source url to download data, formatted for target file name (string)
    '''
    return SOURCE_URL.format(target_file=SOURCE_FILENAME)


def getAssetName(date):
    '''
    get asset name
    INPUT   date: date in the format of the DATE_FORMAT variable (string)
    RETURN  GEE asset name for input date (string)
    '''
    return os.path.join(EE_COLLECTION, FILENAME.format(date=date))

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
    INPUT   exclude_dates: list of dates that we already have in GEE (list of strings)
    RETURN  new_dates: list of new dates we want to try to get in the format of the DATE_FORMAT variable (list of strings)
    '''
    # create empty list to store dates we want to fetch
    new_dates = []
    # start with today's date
    date = datetime.date.today()
    # replace day to be the fifteen of the current month
    date.replace(day=15)
    for i in range(MAX_ASSETS):
        # go back 30 day at a time; 30 was set by TIMESTEP variable
        date -= datetime.timedelta(**TIMESTEP)
        # replace day to be the fifteen of the current month
        date.replace(day=15)
        # generate a string from the date
        datestr = date.strftime(DATE_FORMAT)
        # if the date string is not the list of dates we already have, add it to the list of new dates to try and fetch
        if datestr not in exclude_dates + new_dates:
            new_dates.append(datestr)
    return new_dates

def fetch(filename):
    '''
    Fetch files by filename
    INPUT   filename: name for the netcdf we want to try to fetch(string)
    RETURN  filename: file name for netcdf that have been downloaded (string)
    '''
    # get the url to download the file from the source for the given file name
    url = getUrl(target_file)
    try:
        # try to download the data
        urllib.request.urlretrieve(url, filename)
    except Exception as e:
        # if unsuccessful, log that the file was not downloaded
        logging.warning('Could not fetch {}'.format(url))
        logging.error(e)
    return filename

def extract_metadata(nc_file):
    '''
    Fetch metadata from netcdf by filename
    INPUT   nc_file: file name for netcdf for which we want to try to fetch metadata(string)
    RETURN  dtype: data type of the input netcdf (string)
            nodata: nodata value for netcdf (float)
    '''
    # open the netcdf file and create an instance of the ncCDF4 class
    nc = Dataset(nc_file)
    # extract data from netcdf file
    logging.info(nc)
    logging.info(nc.variables)
    logging.info(nc[VAR_NAME])
    # Get data type of the netcdf 
    dtype = str(nc[VAR_NAME].dtype)
    # Get nodata value of the netcdf
    nodata = float(nc[VAR_NAME].getncattr(MISSING_VALUE_NAME))
    # delete the instance of the ncCDF4 class from memory
    del nc
    return dtype, nodata

def retrieve_formatted_dates(nc_file, date_pattern=DATE_FORMAT):
    '''
    Fetch date from netcdf by filename and format date to be used in GEE
    INPUT   nc_file: file name for netcdf for which we want to try to fetch date(string)
            date_pattern: format of date
    RETURN  formatted_dates: list of dates for which input netcdf is available (list of strings)
    '''
    # open the netcdf file and create an instance of the ncCDF4 class
    nc = Dataset(nc_file)
    # extract time variable range from netcdf
    time_displacements = nc[TIME_NAME]
    # delete the instance of the ncCDF4 class from memory
    del nc

    # get time units from the netcdf
    time_units = time_displacements.getncattr('units')
    logging.debug("Time units: {}".format(time_units))
    # fuzzy=True allows the parser to pick the date out from a string with other text
    ref_time = parser.parse(time_units, fuzzy=True)
    logging.debug("Reference time: {}".format(ref_time))

    # format times according to the variable DATE_FORMAT
    formatted_dates = [(ref_time + datetime.timedelta(days=int(time_disp))).strftime(date_pattern) for time_disp in time_displacements]
    logging.debug('Dates available: {}'.format(formatted_dates))
    return(formatted_dates)

def extract_subdata_by_date(nc_file, dtype, nodata, available_dates, target_dates):
    '''
    Create tifs from input netcdf file for available dates
    INPUT   nc_file: file name for netcdf that have already been downloaded (string)
            dtype: data type of the input netcdf (string)
            nodata: nodata value for netcdf (float)
            available_dates: list of dates for which input netcdf is available (list of strings)
            target_dates: list of new dates we want to try to get
    RETURN  sub_tifs: list of file names for tifs that have been generated (list of strings)
    '''
    # open the netcdf file and create an instance of the ncCDF4 class
    nc = Dataset(nc_file)
    # create and empty list to store the names of the tifs we generate
    sub_tifs = []
    #go through each dates we want to try to get and check if they are avaialble in netcdf
    for date in target_dates:
        # find index in available date, if not available, skip this date
        try:
            date_ix = available_dates.index(date)
            logging.info("Date {} found! Processing...".format(date))
        except:
            logging.info("Date {} not found in available dates".format(date))
            continue

        # Extract data from netcdf for the available date
        data_tmp = nc[VAR_NAME][date_ix,:,:]
        data = np.zeros(data_tmp.shape)
        data[:,:90] = data_tmp[:,90:]
        data[:,90:] = data_tmp[:,:90]

        # Create profile/tif metadata for the available date
        south_lat = -90
        north_lat = 90
        west_lon = -180
        east_lon = 180
        # return an Affine transformation using bounds, width and height
        transform = rio.transform.from_bounds(west_lon, south_lat, east_lon, north_lat, data.shape[1], data.shape[0])
        # generate profile for the tif file that we will create
        profile = {
            'driver':'GTiff',
            'height':data.shape[0],
            'width':data.shape[1],
            'count':1,
            'dtype':dtype,
            'crs':'EPSG:4326',
            'transform':transform,
            'compress':'lzw',
            'nodata':nodata
        }
        # generate a name to save the tif file we will create from the netcdf file
        sub_tif = os.path.join(DATA_DIR,'{}.tif'.format(FILENAME.format(date=date)))
        logging.info(sub_tif)
        # create tif file for the available date
        with rio.open(sub_tif, 'w', **profile) as dst:
            dst.write(data.astype(dtype), indexes=1)
        # add the new tif files to the list of tifs
        sub_tifs.append(sub_tif)

    # delete the instance of the ncCDF4 class from memory
    del nc
    return sub_tifs


def processNewData(existing_dates):
    '''
    fetch, process, upload, and clean new data
    INPUT   existing_dates: list of dates we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
    RETURN  assets: list of file names for netcdfs that have been downloaded (list of strings)
    '''
    # Get list of new dates we want to try to fetch data for
    target_dates = getNewDates(existing_dates)

    # Fetch new files
    logging.info('Fetching files')
    nc_file = fetch(os.path.join(DATA_DIR,'nc_file.nc'))
    # Fetch date from netcdf and format date according to DATE_FORMAT
    available_dates = retrieve_formatted_dates(nc_file)
    # Fetch metadata from netcdf 
    dtype, nodata = extract_metadata(nc_file)
    logging.info('type: ' + dtype)
    logging.info('nodata val: ' + str(nodata))

    # If we have successfully been able to fetch new data files
    if target_dates:
        # Create new tifs from netcdf file for available dates
        logging.info('Converting files')
        sub_tifs = extract_subdata_by_date(nc_file, dtype, nodata, available_dates, target_dates)
        logging.info(sub_tifs)

        logging.info('Uploading files')
        # Get a list of the dates we have to upload from the tif file names
        dates = [getDate(tif) for tif in sub_tifs]
        # Get a list of datetimes from these dates for each of the dates we are uploading
        datestamps = [datetime.datetime.strptime(date, DATE_FORMAT) for date in dates]
        # Get a list of the names we want to use for the assets once we upload the files to GEE
        assets = [getAssetName(date) for date in dates]
        # Upload new files (tifs) to GEE
        eeUtil.uploadAssets(sub_tifs, assets, GS_FOLDER, datestamps, timeout=900)

        # Delete local files
        logging.info('Cleaning local files')
        os.remove(nc_file)
        for tif in sub_tifs:
            logging.debug('deleting: ' + tif)
            os.remove(tif)

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
        eeUtil.createFolder(collection, imageCollection=True, public=True)
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
    # get the most recent date (last in the list) and turn it into a datetime
    most_recent_date = datetime.datetime.strptime(existing_dates[-1], '%Y%m%d')
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
