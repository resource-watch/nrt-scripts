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
import ee
import time
from string import ascii_uppercase
from netCDF4 import Dataset

# url for Berkeley Earth Surface Temperature data
SOURCE_URL = 'http://berkeleyearth.lbl.gov/auto/Global/Gridded/Land_and_Ocean_LatLong1.nc'

# subdataset to be converted to tif
# should be of the format 'NETCDF:"filename.nc":variable'
SDS_NAME = 'NETCDF:"{fname}":temperature'

# nodata value for netcdf
NODATA_VALUE = 9.969209968386869e+36

# name of data directory in Docker container
DATA_DIR = 'data'

# name of collection in GEE where we will upload the final data
EE_COLLECTION = '/projects/resource-watch-gee/cli_076_surface_temperature_monthly_avg'

# generate generic string that can be formatted to name each variable's asset name
FILENAME = 'cli_076_surface_temperature_monthly_avg_{date}'

# specify Google Cloud Storage folder name
GS_FOLDER = EE_COLLECTION[1:]

# do you want to delete everything currently in the GEE collection when you run this script?
CLEAR_COLLECTION_FIRST = False

# how many assets can be stored in the GEE collection before the oldest ones are deleted?
MAX_ASSETS = 150

# date format to use in GEE
DATE_FORMAT = '%Y'

# Resource Watch dataset API IDs
# Important! Before testing this script:
# Please change these IDs OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on different datasets on Resource Watch
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
def getAssetName(date):
    '''
    get asset name
    INPUT   date: date in the format YYYY (string)
    RETURN  GEE asset name for input date (string)
    '''
    return os.path.join(EE_COLLECTION, FILENAME.format(date=date))

def getDateTimeString(filename):
    '''
    get date from filename (last 4 characters of filename after removing extension)
    INPUT   filename: file name that ends in a date of the format YYYY (string)
    RETURN  date in the format YYYY (string)
    '''
    return os.path.splitext(os.path.basename(filename))[0][-4:]

def getFilename():
    '''
    get netcdf filename to save source file as
    RETURN  file name to save netcdf from source under (string)
    '''
    return os.path.join(DATA_DIR, '{}'.format(SOURCE_URL.split('/')[-1]))

def getDateTime(date):
    '''
    get datetime from date
    INPUT   date: date in the format YYYY (string)
    RETURN  datetime in the format of DATE_FORMAT (datetime)
    '''
    return datetime.datetime.strptime(date, DATE_FORMAT)

def monthly_avg(year, num_tifs, mnth_tifs):
    '''
    Calculate a yearly average tif file from all the monthly tif files
    INPUT   year: year we want to process (string)
            num_tifs: number of tifs we are averaging (integer)
            mnth_tifs: list of file names for tifs that were created from downloaded netcdfs (list of strings)
    RETURN  result_tif: file name for tif file created after averaging all the input tifs (string)
    '''
    # create a list to store the tifs to be used in gdal_calc
    gdal_tif_list=[]
    # set up calc input for gdal_calc
    calc = '--calc="('
    # go through each month in the year to be averaged
    for i in range(len(mnth_tifs)):
        # generate a letter variable for that tif to use in gdal_calc (A, B, C...)
        letter = ascii_uppercase[i]
        # add each letter to the list to be used in gdal_calc
        gdal_tif_list.append('-'+letter)
        # pull the tif name
        tif = mnth_tifs[i]
        # add each tif name to the list to be used in gdal_calc
        gdal_tif_list.append('"'+tif+'"')
        # add the variable to the calc input for gdal_calc
        if i==0:
            # for first tif, it will be like: --calc="(A
            calc= calc +letter
        else:
            # for second tif and onwards, keep adding each letter like: --calc="(A+B
            calc = calc+'+'+letter
    # finish creating calc input
    # since we are trying to find average, the algorithm is: (sum all tifs/number of tifs)
    calc= calc + ')/{}"'.format(num_tifs)
    # generate a file name for the monthly average tif
    result_tif = DATA_DIR+'/'+FILENAME.format(year=year)+'.tif'
    # create the gdal command to calculate the average by putting it all together
    cmd = ('gdal_calc.py {} --outfile="{}" {}').format(' '.join(gdal_tif_list), result_tif, calc)
    # using gdal from command line from inside python
    subprocess.check_output(cmd, shell=True)
    
    return result_tif

def convert(file, existing_dates):
    '''
    Convert netcdf file to tifs
    INPUT   file: file names for netcdf that have already been downloaded (list of strings)
            existing_dates: list of date ranges that we already have in GEE, in the format YYYY (list of strings)
    RETURN  yrly_avgd_tifs: list of file names for tifs that have been generated (list of strings)
    '''
    # open the netcdf file in read mode
    ds = Dataset(file)
    # get the list of years for which data is available as a numpy array
    ls_yrs = ds['time'][:]
    # convert the array into list of strings
    str_ls_yrs = list(map(str, ls_yrs))
    # the source contains data from 1850 but we are only interested in data starting 1950
    # find the first index of the 1950 data
    start_idx_1950 = [i for i, s in enumerate(str_ls_yrs) if s.startswith('1950.')][0]
    # find the index of last item in the list of all available years
    last_idx = len(str_ls_yrs) - 1
    # get only the year part from the whole decimal date
    sub_ls_yrs = [sub[:4] for sub in str_ls_yrs]
    # initialize a variable to identify which month is being processed
    prcs_mnth = 0
    # create an empty list to store filename for each monthly tif 
    mnth_tifs = []
    # create an empty list to store filename for each yearly tif
    yrly_avgd_tifs = []
    # generate the subdatset name for current netcdf file
    sds_path = SDS_NAME.format(fname=file)
    # loop through each available dates for which monthly temperature is available 
    # find the dates after 1950 and dates that we already don't have in our catalogue 
    # convert them to tifs
    for i, prcs_yr in enumerate(sub_ls_yrs): 
        if i >= start_idx_1950 and prcs_yr not in existing_dates:
            # increment the month being processed after each loop
            prcs_mnth += 1
            # generate a name for the monthly tif that will be produced from the netcdf
            mnth_tif = os.path.join(DATA_DIR, '{}.tif'.format(prcs_yr + '_' + str(prcs_mnth)))
            # translate the netcdf into a tif
            # make sure the index of the band is correct and matches exactly with the source netcdf
            # the band index in the gdal_translate command determines which month of data we are processing
            cmd = ['gdal_translate', '-b', str(i), '-q', '-a_nodata', str(NODATA_VALUE), '-a_srs', 'EPSG:4326', sds_path, mnth_tif]
            logging.debug('Converting {} to {}'.format(file, mnth_tif))
            # use subprocess to use gdal_translate in the command line from inside python
            subprocess.call(cmd)
            # add the new tif files to the list of monthly tifs for the year being processed
            mnth_tifs.append(mnth_tif)
            # if we have finished processing all 12 months or all available months in a year 
            if prcs_mnth == 12 or i == last_idx:
                # find the average for the year from the monthly tifs created so far
                yrly_avgd_tif = monthly_avg(prcs_yr, prcs_mnth, mnth_tifs)
                # add the averaged tif to the list of yearly tif
                yrly_avgd_tifs.append(yrly_avgd_tif)
                # reset the list for monthly tifs since we will process a new year in the next loop
                mnth_tifs = []
                # reset the month number to 1 as well for next loop
                prcs_mnth = 1

    return yrly_avgd_tifs

def fetch():
    '''
    Fetch file from source url
    RETURN  file: file name for netcdfs that have been downloaded (string)
    '''
    # construct the filename we want to save the file under locally
    file = getFilename()
    logging.info('Fetching {}'.format(SOURCE_URL))
    try:
        # try to download the data
        urllib.request.urlretrieve(SOURCE_URL, file)
        logging.info('Successfully retrieved {}'.format(file))
    except:
        # if unsuccessful, log that the file was not downloaded
        logging.error('Unable to retrieve data from {}'.format(SOURCE_URL))

    return file
    
def processNewData(existing_dates):
    '''
    fetch, process, and upload new data
    INPUT   existing_dates: list of dates we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
    RETURN  assets: list of file names for netcdfs that have been downloaded (list of strings)
    '''
    # Fetch data from source url
    logging.info('Fetching Monthly Land + Ocean temperature data from berkeleyearth')
    file = fetch()

    # If we have successfully been able to fetch new data file
    if file:
        # Convert new file from netcdf to tif files
        logging.info('Converting netcdf file to tifs')
        tifs = convert(file, existing_dates)
        # Get a list of the dates we have to upload from the tif file names
        dates = [getDateTimeString(tif) for tif in tifs] 
        # Get a list of the names we want to use for the assets once we upload the files to GEE
        assets = [getAssetName(date) for date in dates]
        # Get a list of the datetimes we have to upload from the tif file names
        new_datetimes = [getDateTime(date) for date in dates] 
        logging.info('Uploading files')
        # Upload new files (tifs) to GEE
        eeUtil.uploadAssets(tifs, assets, GS_FOLDER, new_datetimes, timeout=3000) 

        # Delete local files
        logging.info('Cleaning local files')
        for tif in tifs:
            os.remove(tif)
        os.remove(file)

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
    Get most recent data we have assets for
    INPUT   collection: GEE collection to check dates for (string)
    RETURN  most_recent_date: most recent date in GEE collection (datetime)
    '''
    # get list of assets in collection
    existing_assets = checkCreateCollection(collection)
    # get a list of strings of dates in the collection
    existing_dates = [getDateTimeString(a) for a in existing_assets]
    # sort these dates oldest to newest
    existing_dates.sort()
    # get the most recent date (last in the list) and turn it into a datetime
    most_recent_date = datetime.datetime.strptime(existing_dates[-1], DATE_FORMAT)

    return most_recent_date

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

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Initialize eeUtil and ee modules
    eeUtil.initJson()
    initialize_ee()

    # Clear the GEE collection, if specified above
    if CLEAR_COLLECTION_FIRST:
        if eeUtil.exists(EE_COLLECTION):
            eeUtil.removeAsset(EE_COLLECTION, recursive=True)

    # Check if collection exists, create it if it does not
    # If it exists return the list of assets currently in the collection
    existing_assets = checkCreateCollection(EE_COLLECTION)
    # Get a list of the dates of data we already have in the collection
    existing_dates = [getDateTimeString(a) for a in existing_assets]

    # Fetch, process, and upload the new data
    new_assets = processNewData(existing_dates)
    # Get the dates of the new data we have added
    new_dates = [getDateTimeString(a) for a in new_assets]

    logging.info('Previous assets: {}, new: {}, max: {}'.format(
        len(existing_dates), len(new_dates), MAX_ASSETS))

    # Delete excess assets
    deleteExcessAssets(existing_dates+new_dates, MAX_ASSETS)

    # Update Resource Watch
    updateResourceWatch()

    logging.info('SUCCESS')
