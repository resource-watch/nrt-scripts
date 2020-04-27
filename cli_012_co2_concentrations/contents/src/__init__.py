from __future__ import unicode_literals

import os
import glob
import sys
import datetime
import logging
import subprocess
import eeUtil
import rasterio as rio
from affine import Affine
import numpy as np
from rasterio.crs import CRS
import requests
import time
from dateutil.relativedelta import relativedelta

# url for CO₂ concentrations data
SOURCE_URL = 'https://acdisc.gesdisc.eosdis.nasa.gov/data/Aqua_AIRS_Level3/AIRS3C2M.005/{year}/'

# filename format for GEE
ASSET_NAME = 'cli_012_co2_concentrations_{date}'

# nodata value for hdf
NODATA_VALUE = -9999

# name of data directory in Docker container
DATA_DIR = 'data'

# name of folder to store data in Google Cloud Storage
GS_FOLDER = 'cli_012_co2_concentrations'

# name of collection in GEE where we will upload the final data
EE_COLLECTION = 'cli_012_co2_concentrations'

# do you want to delete everything currently in the GEE collection when you run this script?
CLEAR_COLLECTION_FIRST = False

# format of date (used in both source and GEE)
DATE_FORMAT = '%Y%m'

# how many assets can be stored in the GEE collection before the oldest ones are deleted?
# maximum assets is 60 in this case (5 years of monthly data)
MAX_ASSETS = 60

# get credentials to access the source data
NASA_USER = os.environ.get("EARTHDATA_USER")
NASA_PASS = os.environ.get("EARTHDATA_KEY")

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = '68455cb5-bfe3-4528-83a2-00fab1c52fb9'

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

def getAssetName(tif):
    '''
    get asset name
    INPUT   tif: file name for tif files that were created from downloaded hdf files (string)
    RETURN  GEE asset name for input tif (string)
    '''
    # get date from filename 
    date = getDate(tif)
    return os.path.join(EE_COLLECTION, ASSET_NAME.format(date=date))

def getDate(filename):
    '''
    get date from filename (last 6 characters of filename after removing extension)
    INPUT   filename: file name that ends in a date of the format YYYYMM (string)
    RETURN  date in the format YYYYMM (string)
    '''
    return os.path.splitext(os.path.basename(filename))[0][-6:]

def getNewDates(exclude_dates):
    '''
    Get new dates we want to try to fetch data for
    INPUT   exclude_dates: list of dates that we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
    RETURN  new_dates: list of new dates we want to try to get, in the format of the DATE_FORMAT variable (list of strings)
    '''
    # create empty list to store dates we want to fetch
    new_dates = []
    # get today's date
    date = datetime.date.today()
    # replace day to be the 15th of the current month
    date.replace(day=15)
    for i in range(MAX_ASSETS):
        # go back one month (according to TIMESTEP) at a time
        date -= relativedelta(months=1)
        # replace day to be the 15th of the current month
        date.replace(day=15)
        # change the format of date to match the format used in source data files
        datestr = date.strftime(DATE_FORMAT)
        # if the date string is not the list of dates we already have, add it to the list of new dates to try and fetch
        if datestr not in exclude_dates:
            # add to list of new dates
            new_dates.append(datestr)
    return new_dates

def fetch(year):
    '''
    Fetch files by datestamp
    INPUT   year: list of years we want to try to fetch, in the format YYYY (list of strings)
    RETURN  list of file names for hdfs that have been downloaded (list of strings)
    '''
    # try to download the data
    cmd = ' '.join(['wget','--user',NASA_USER,'--password',NASA_PASS,
                    '-r','-c','-nH','-nd','-np',
                    '-A','hdf,hdf.map.gz,hdf.xml',
                    SOURCE_URL.format(year=year)])

    subprocess.call(cmd, shell=True)
    logging.info('call to server: {}'.format(cmd))

def getDateFromSource(filename):
    '''
    Get year and month from filename
    INPUT   filename: file name for hdf that have been downloaded (string)
    RETURN  year and month from filename (string)
    '''
    # split filename to separate out year and month
    dateinfo = filename.split('.')
    year = dateinfo[1]
    month = dateinfo[2]
    return('{year}{month}'.format(year=year,month=month))

def convert(filename, date):
    '''
    Convert hdf files to tifs
    INPUT   filename: file name for hdf that have been downloaded (string)
            date: date for hdf that have been downloaded (string)
    RETURN  georef_filename: file name for georeferenced tif that have been generated (string)
    '''
    # filenmae format for GEE
    new_filename = ASSET_NAME.format(date=date)
    # filename for geotif created from hdf
    data_filename = new_filename+'_data.tif'
    # filename for georeferenced file created from geotif
    georef_filename = new_filename+'.tif'
    # tranlate the hdf into a geotif
    cmd = ' '.join(['gdal_translate','-of', 'GTIFF',
                    '\'HDF4_EOS:EOS_GRID:"{file}":CO2:mole_fraction_of_carbon_dioxide_in_free_troposphere\''.format(file=filename),
                    data_filename])
    subprocess.call(cmd, shell=True)

    # read the geotif file using rasterio
    with rio.open(data_filename, 'r') as src:
        data = src.read(indexes=1)
        # lats: -89.5, 88 to 60, in increments of 2
        # lons: -180 to 177.5, in increments of 2.5
        row_width=2.5
        column_height=-2
        row_rotation=0
        column_rotation=0
        upper_right_x=-180
        upper_right_y=90

        # return an Affine transformation using bounds, width and height
        transform = Affine(row_width,row_rotation,upper_right_x,
                            column_rotation, column_height, upper_right_y)
        # generate profile for the georeferenced tif file that we will create
        profile = {
            'driver': 'GTiff',
            'dtype': np.float32,
            'nodata': -9999,
            'width': data.shape[1],
            'height': data.shape[0],
            'count': 1,
            'crs': CRS({'init': 'EPSG:4326'}),
            'transform':transform,
            'tiled': True,
            'compress': 'lzw',
            'interleave': 'band'
        }
        # create the georeferenced tif file 
        with rio.open(georef_filename, "w", **profile) as dst:
            dst.write(data, indexes=1)

    return georef_filename

def clearDir():
    '''
    Delete local files
    '''
    files = glob.glob('*')
    for file in files:
        os.remove(file)

def processNewData(existing_dates):
    '''
    fetch, process, upload, and clean new data
    INPUT   existing_dates: list of dates we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
    RETURN  new_assets: list of file names for hdfs that have been downloaded (list of strings)
    '''
    # Get list of new dates we want to try to fetch data for
    target_dates = getNewDates(existing_dates) or []
    logging.debug('Target dates: {}'.format(target_dates))
    # fetch new files
    logging.info('Fetching files')
    # Create an empty list of new years we want to try to fetch data for
    years = []
    for date in target_dates:
        years.append(date[0:4])
    # ATTENTION AMELIA AMELIA AMELIA ATTENTION !!!
    # PLEASE VERIFY THE FOLLOWING COMMENT    
    # create a set for years to remove duplicate years from the list     
    years = set(years)
    logging.info(years)
    # create an empty list to store asset names that will be uploaded to GEE
    new_assets = []
    for year in years:
        # Delete local files
        clearDir()
        # Fetch new files 
        fetch(year)
        # Store fetched files to a list
        files = glob.glob('*.hdf')
        # create an empty list to store tif filenames that were created from hdf files
        tifs = []
        for _file in files:
            # get year and month from filename
            date = getDateFromSource(_file)
            logging.info(date)
            logging.info(existing_dates)
            # if we don't have this date already in GEE
            if date not in existing_dates:
                logging.info('Converting file: {}'.format(_file))
                # convert hdfs to tifs and store the tif filenames to a list
                tifs.append(convert(_file, date))

        logging.info('Uploading files')
        # Get a list of the dates we have to upload from the tif file names
        dates = [getDate(tif) for tif in tifs]
        # Get a list of the names we want to use for the assets once we upload the files to GEE
        assets = [getAssetName(tif) for tif in tifs]
        # Get a list of datetimes from each of the dates we are uploading
        # Upload new files (tifs) to GEE
        eeUtil.uploadAssets(tifs, assets, GS_FOLDER, dates=[datetime.datetime.strptime(date, DATE_FORMAT) for date in dates], public=True, timeout=3000)
        new_assets.extend(assets)
    # Delete local files
    clearDir()
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
    Get most recent data it
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
    existing_ids = checkCreateCollection(EE_COLLECTION)
    # Get a list of the dates of data we already have in the collection
    exclude_dates = [getDate(asset) for asset in existing_ids]
    logging.debug(exclude_dates)

    # Fetch, process, and upload the new data
    os.chdir('data')
    new_assets = processNewData(exclude_dates)

    logging.info('Existing assets: {}, new: {}, max: {}'.format(
        len(existing_ids), len(new_assets), MAX_ASSETS))

    # Delete excess assets
    deleteExcessAssets(existing_ids+new_assets, MAX_ASSETS)

    # Update Resource Watch
    updateResourceWatch()

    logging.info('SUCCESS')
