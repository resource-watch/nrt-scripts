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

# Sources for nrt data
#h0 version is 3 hourly data
#h3 version is 6-hourly data
VERSION = 'h3'
#Data set owner has created a subset of the data for our needs on Resouce Watch
#If you want to switch back to pulling from the original source, set the following
#variable to False
rw_subset = True

SDS_NAME = 'NETCDF:"{fname}":{var}'
NODATA_VALUE = None

DATA_DIR = 'data'
COLLECTION = '/projects/resource-watch-gee/cit_038_WACCM_atmospheric_chemistry_model'
CLEAR_COLLECTION_FIRST = True
DELETE_LOCAL = True

# MAXDAYS = 1 only fetches today
# maximum value of 10: today plus 9 days of forecast
MAX_DAYS = 2
DATE_FORMAT = '%y-%m-%d_%H%M'
TIMESTEP = {'days': 1}
#if we don't want to show the last time available for the last day, how many time steps before
#the last is the time we want to show?
#ex: for now, we want to show 12:00, which is 1 time step before 18:00
TS_FROM_END = 1
LOG_LEVEL = logging.INFO

DATASET_IDS = {
    'NO2':'2c2c614a-8678-443a-8874-33335771ecc0',
    'CO':'266ed113-396c-4c69-885a-ead30df95810',
    'O3':'ec011d66-a99b-425c-accd-d04e75966094',
    'SO2':'d82186a4-7885-4fa9-9e82-26799853093b',
    'PM25':'348e4d57-a345-411d-986e-5863fffebda7',
    'bc_a4':'fe0a0042-8430-419b-a60f-9b69ec81a0ec'
}
apiToken = os.getenv('apiToken')

if rw_subset==True:
    SOURCE_URL = 'https://www.acom.ucar.edu/waccm/subsets/resourcewatch/f.e22.beta02.FWSD.f09_f09_mg17.cesm2_2_beta02.forecast.001.cam.%s.{date}_surface_subset.nc' % VERSION
    VARS = ['NO2', 'CO', 'O3', 'SO2', 'PM25', 'bc_a4']
    NUM_AVAILABLE_LEVELS = [1, 1, 1, 1, 1, 1]
    DESIRED_LEVELS = [1, 1, 1, 1, 1, 1]
else:
    SOURCE_URL = 'https://www.acom.ucar.edu/waccm/DATA/f.e21.FWSD.f09_f09_mg17.forecast.001.cam.%s.{date}-00000.nc' % VERSION
    VARS = ['NO2', 'CO', 'O3', 'SO2', 'PM25_SRF', 'bc_a4']
    # most variables have 88 pressure levels; PM 2.5 only has one level (surface)
    # need to specify which pressure level of data we was for each (level 1 being the lowest pressure)
    # the highest level is the highest pressure (992.5 hPa), and therefore, closest to surface level
    NUM_AVAILABLE_LEVELS = [88, 88, 88, 88, 1, 88]
    DESIRED_LEVELS = [88, 88, 88, 88, 1, 88]

# get time intervals in each day - specific to version number
#h0 version is 3 hourly data
if VERSION == 'h0':
    TIME_HOURS = list(range(0, 24, 3))
# h3 version is 6-hourly data
elif VERSION == 'h3':
    TIME_HOURS = list(range(0, 24, 6))

#how many assets can be stored in the GEE collection before the oldest ones are deleted?
#number of days times number of time intervals in each day
MAX_ASSETS = len(TIME_HOURS) * MAX_DAYS

def getLastUpdate(dataset):
    apiUrl = 'http://api.resourcewatch.org/v1/dataset/{}'.format(dataset)
    r = requests.get(apiUrl)
    lastUpdateString=r.json()['data']['attributes']['dataLastUpdated']
    nofrag, frag = lastUpdateString.split('.')
    nofrag_dt = datetime.datetime.strptime(nofrag, "%Y-%m-%dT%H:%M:%S")
    lastUpdateDT = nofrag_dt.replace(microsecond=int(frag[:-1])*1000)
    return lastUpdateDT

def getLayerIDs(dataset):
    apiUrl = 'http://api.resourcewatch.org/v1/dataset/{}?includes=layer'.format(dataset)
    r = requests.get(apiUrl)
    layers = r.json()['data']['attributes']['layer']
    layerIDs =[]
    for layer in layers:
        if layer['attributes']['application']==['rw']:
            layerIDs.append(layer['id'])
    return layerIDs

def flushTileCache(layer_id):
    """
    This function will delete the layer cache built for a GEE tiler layer.
     """
    apiUrl = 'http://api.resourcewatch.org/v1/layer/{}/expire-cache'.format(layer_id)
    headers = {
    'Content-Type': 'application/json',
    'Authorization': os.getenv('apiToken')
    }
    try_num=1
    tries=4
    while try_num<tries:
        try:
            r = requests.delete(url = apiUrl, headers = headers, timeout=1000)
            if r.ok or r.status_code==504:
                logging.info('[Cache tiles deleted] for {}: status code {}'.format(layer_id, r.status_code))
                return r.status_code
            else:
                if try_num < (tries-1):
                    logging.info('Cache failed to flush: status code {}'.format(r.status_code))
                    time.sleep(60)
                    logging.info('Trying again.')
                else:
                    logging.error('Cache failed to flush: status code {}'.format(r.status_code))
                    logging.error('Aborting.')
            try_num += 1
        except Exception as e:
            logging.error('Failed: {}'.format(e))

def lastUpdateDate(dataset, date):
   apiUrl = 'http://api.resourcewatch.org/v1/dataset/{0}'.format(dataset)
   headers = {
   'Content-Type': 'application/json',
   'Authorization': apiToken
   }
   body = {
       "dataLastUpdated": date.isoformat()
   }
   try:
       r = requests.patch(url = apiUrl, json = body, headers = headers)
       logging.info('[lastUpdated]: SUCCESS, '+ date.isoformat() +' status code '+str(r.status_code))
       return 0
   except Exception as e:
       logging.error('[lastUpdated]: '+str(e))



def getAssetName(date):
    '''get asset name from datestamp'''# os.path.join('home', 'coming') = 'home/coming'
    return os.path.join(EE_COLLECTION, FILENAME.format(var=VAR, date=date))


def getFilename(date):
    '''get filename from datestamp CHECK FILE TYPE'''
    return os.path.join(DATA_DIR, '{}.nc'.format(
        FILENAME.format(var='all_vars', date=date)))

def getTiffname(file, hour, variable):
    '''get filename from datestamp CHECK FILE TYPE'''
    # get a string for that time
    if hour < 10:
        time_str = '0' + str(hour) + '00'
    else:
        time_str = str(hour) + '00'
    date = os.path.splitext(file)[0][-10:]
    name = os.path.join(DATA_DIR, FILENAME.format(var=variable, date=date)) + '_' + time_str
    return name

def getDateTime(filename):
    '''get last 8 chrs of filename CHECK THIS'''
    return os.path.splitext(os.path.basename(filename))[0][-13:]

def getDate_GEE(filename):
    '''get last 8 chrs of filename CHECK THIS'''
    return os.path.splitext(os.path.basename(filename))[0][-13:-5]

def list_available_files(url, ext=''):
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    return [node.get('href') for node in soup.find_all('a') if type(node.get('href'))==str and node.get('href').endswith(ext)]

def getNewDates(existing_dates):
    #get the date that the most recent forecast was created on
    url = os.path.split(SOURCE_URL)[0]
    available_files = list_available_files(url, ext='.nc')[-9:]
    recent_forecast_start = available_files[0]
    recent_forecast_start_date = recent_forecast_start[-26:-18]
    #sort and get the forecast start date for the data we already have
    if existing_dates:
        existing_dates.sort()
        existing_forecast_start_date = existing_dates[0]
    else:
        existing_forecast_start_date = None
    #if we have the most recent forecast, we don't need new data
    if existing_forecast_start_date==recent_forecast_start_date:
        new_dates = []
    #otherwise, we need to go get the days of interest
    else:
        #get start date of forecast through the day we want to show on RW
        recent_files = available_files[:MAX_DAYS]
        new_dates = [file[-28:-18] for file in recent_files]
    # get last date because this file only has one time output so we need to process it differently
    last_date = available_files[-1]
    return new_dates, last_date

def getBands(var_num, file, last_date):
    # get specified pressure level for the current variable
    level = DESIRED_LEVELS[var_num]
    # the pressure and time dimensions are flattened into one dimension in the netcdfs
    # for the pressure level that we want, we want all the times available
    # we will make a list of the BANDS that have data for the desired level at all times available
    # h0 has 8 times
    if VERSION == 'h0':
        bands = [x * NUM_AVAILABLE_LEVELS[var_num] + level for x in
                 list(range(0, 8))]  # gives all times at specified pressure level
    # h3 has 4 times
    elif VERSION == 'h3':
        bands = [x * NUM_AVAILABLE_LEVELS[var_num] + level for x in
                 list(range(0, 4))]  # gives all times at specified pressure level
    if file[-13:-3] == last_date:
        # if we are on the last file, only one time is available
        bands = [x * NUM_AVAILABLE_LEVELS[var_num] + level for x in
                 list(range(0, 1))]  # gives all times at specified pressure level
    return bands

def convert(files, var_num, last_date):
    '''convert netcdfs to tifs'''
    #create an empty list to store the names of tif files that we create
    all_tifs = []
    for f in files:
        #get bands that we want for the pressure level we are interested in at all times
        bands = getBands(var_num, f, last_date)
        logging.info('Converting {} to tiff'.format(f))
        for band in bands:
            # get command to call the netcdf file for a particular variable
            sds_path = SDS_NAME.format(fname=f, var=VAR)
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
            file_name_with_time = getTiffname(file=f, hour=TIME_HOURS[bands.index(band)], variable=VAR)
            #create a file for the initial tif that is in the 0 to 360 longitude format
            tif_0_360 = '{}_0_360.tif'.format(file_name_with_time)
            # create a file name for the final tif that is in the -180 to 180 file format
            tif = '{}.tif'.format(file_name_with_time)

            # first we will translate this file from a netcdf to a tif
            cmd = ['gdal_translate', '-b', str(band), '-q', '-a_nodata', str(NODATA_VALUE), '-a_srs', 'EPSG:4326', sds_path, tif_0_360] #'-q' means quiet so you don't see it
            subprocess.call(cmd) #using the gdal from command line from inside python

            # Now we will fix the longitude. To do this we need the x and y resolution.
            # I also got x and y res for data set using the gdalinfo command described above.
            xres='1.250000000000000'
            yres= '-0.942408376963351'
            cmd_warp = ['gdalwarp', '-t_srs', 'EPSG:4326', '-tr', xres, yres, tif_0_360, tif, '-wo', 'SOURCE_EXTRA=1000', '--config', 'CENTER_LONG', '0']
            subprocess.call(cmd_warp) #using the gdal from command line from inside python

            #add name of tif to our list of tif files
            all_tifs.append(tif)
    # If we don't want to use all the times available, we should have set the TS_FROM_END parameter at the beginning.
    if TS_FROM_END>0:
        # from the list of all the tifs created, get a list of the tifs you actually want to upload
        # this should be all the files through the desired end point
        tifs = all_tifs[:-TS_FROM_END]
    return all_tifs, tifs

def fetch(new_dates):
    # Create an empty list to store file locations of netcdfs that are downloaded.
    files = []
    # Loop over the new dates, check if there is data available, and download netcdfs
    for date in new_dates:
        # Set up the url of the filename to download
        url = SOURCE_URL.format(date=date)
        # Create a file name to store the netcdf in after download
        f = getFilename(date)
        #get file name of source file you are about to download
        file_name = os.path.split(url)[1]
        #get list of files available from the source
        file_list = list_available_files(os.path.split(url)[0], ext='.nc')
        #if the file is available, download it
        if file_name in file_list:
            logging.info('Retrieving {}'.format(file_name))
            #try to download file
            try:
                #download files from url and put in specified file location (f)
                urllib.request.urlretrieve(url, f)
                #add file name/location to list of files downloaded
                files.append(f)
                logging.info('Successfully retrieved {}'.format(file_name))# gives us "Successully retrieved file name"
            # if download fails, throw an error
            except Exception as e:
                logging.error('Unable to retrieve data from {}'.format(url))
                logging.error(e)
                logging.debug(e)
        else:
            logging.info('{} not available yet'.format(file_name))
    #return list of files just downloaded
    return files

def processNewData(files, var_num, last_date):
    '''process, upload, and clean new data'''
    if files: #if files is empty list do nothing, otherwise, process data
        logging.info('Converting files')
        # Convert netcdfs to tifs
        all_tifs, tifs = convert(files, var_num, last_date) # naming tiffs

        #get list of dates from the averaged tifs
        dates = [getDateTime(tif) for tif in tifs]
        #generate datetime objects for each data
        datestamps = [datetime.datetime.strptime(date, DATE_FORMAT) for date in dates]
        #create asset names for each data
        assets = [getAssetName(date) for date in dates]
        # Upload new files to GEE
        logging.info('Uploading files:')
        for asset in assets:
            logging.info(os.path.split(asset)[1])
        eeUtil.uploadAssets(tifs, assets, GS_FOLDER, datestamps, timeout=3000)

        # Delete local files
        if DELETE_LOCAL:
            logging.info('Cleaning local TIFF files')
            for tif in all_tifs:
                os.remove(tif)

        return assets
    #if no new assets, return empty list
    else:
        return []


def checkCreateCollection(VARS):
    # create a master list (not variable-specific) of which dates we already have data for
    existing_dates = []
    # create an empty list to store the dates that we currently have for each AQ variable
    existing_dates_by_var = []
    for VAR in VARS:
        # For one of the variables, get the date of the most recent data set
        # All variables come from the same file
        # If we have one for a particular data, we should have them all
        collection = EE_COLLECTION_GEN.format(var=VAR)

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
        # upload for this date, and we need to re-upload this file.
        if count < len(VARS):
            # remove this from the list of existing dates for all variables
            existing_dates_all_vars.remove(date)
    return existing_dates_all_vars, existing_dates_by_var

def deleteExcessAssets(all_assets, max_assets):
    '''Delete assets if too many'''
    if len(all_assets) > max_assets:
        # oldest first
        all_assets.sort()
        logging.info('Deleting excess assets.')
        #delete extra assets after the number we are expecting to see
        for asset in all_assets[:-max_assets]:
            eeUtil.removeAsset(EE_COLLECTION +'/'+ asset)

def get_most_recent_date(all_assets):
    all_assets.sort()
    most_recent_date = datetime.datetime.strptime(all_assets[-1][-13:], DATE_FORMAT)
    return most_recent_date

def get_forecast_run_date(all_assets):
    all_assets.sort()
    most_recent_date = datetime.datetime.strptime(all_assets[0][-13:], DATE_FORMAT)
    return most_recent_date

def clearCollection():
    logging.info('Clearing collections.')
    for var_num in range(len(VARS)):
        var = VARS[var_num]
        collection = EE_COLLECTION_GEN.format(var=var)
        if eeUtil.exists(collection):
            if collection[0] == '/':
                collection = collection[1:]
            a = ee.ImageCollection(collection)
            collection_size = a.size().getInfo()
            if collection_size > 0:
                list = a.toList(collection_size)
                for item in list.getInfo():
                    ee.data.deleteAsset(item['id'])

def initialize_ee():
    GEE_JSON = os.environ.get("GEE_JSON")
    _CREDENTIAL_FILE = 'credentials.json'
    GEE_SERVICE_ACCOUNT = os.environ.get("GEE_SERVICE_ACCOUNT")
    with open(_CREDENTIAL_FILE, 'w') as f:
        f.write(GEE_JSON)
    auth = ee.ServiceAccountCredentials(GEE_SERVICE_ACCOUNT, _CREDENTIAL_FILE)
    ee.Initialize(auth)

def main():
    # set logging levels
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    # create global variables that will be used in many functions
    global VAR
    global EE_COLLECTION
    global EE_COLLECTION_GEN
    global PARENT_FOLDER
    global FILENAME
    global GS_FOLDER

    # generate name for dataset's parent folder on GEE which will be used to store
    # several collections - one collection per variable
    PARENT_FOLDER = COLLECTION
    # generate generic string that can be formatted to name each variable's GEE collection
    EE_COLLECTION_GEN = COLLECTION + '/{var}'
    # generate generic string that can be formatted to name each variable's asset name
    FILENAME = COLLECTION.split('/')[-1]+'_{var}_{date}'

    '''Ingest new data into GEE and delete old data'''
    # Initialize eeUtil and ee modules
    eeUtil.initJson()
    initialize_ee()

    # Clear collection in GEE if desired
    if CLEAR_COLLECTION_FIRST:
        clearCollection()

    # Check if collection exists. If not, create it.
    # Return a list of dates that exist for all variables collections in GEE (existing_dates),
    # as well as a list of which dates exist for each individual variable (existing_dates_by_var).
    # The latter will be used in case the previous script run crashed before completing the data upload for every variable.
    existing_dates, existing_dates_by_var = checkCreateCollection(VARS)

    # Get a list of the dates that are available, minus the ones we have already uploaded correctly for all variables.
    all_new_dates, last_date = getNewDates(existing_dates)

    # if new data is available, clear the collection because we want to store the most
    # recent forecast, not the old forecast
    if all_new_dates:
        logging.info('New forecast available.')
        clearCollection()
    else:
        logging.info('No new forecast.')
    #container only big enough to hold 3 files at once, so break into groups to process
    new_date_groups = [all_new_dates[x:x+3] for x in range(0, len(all_new_dates), 3)]
    for new_dates in new_date_groups:
        # Fetch new files
        logging.info('Fetching files for {}'.format(new_dates))
        # Download files and get list of locations of netcdfs in docker container
        files = fetch(new_dates)
        for var_num in range(len(VARS)):
            # get variable name
            VAR = VARS[var_num]
            # specify GEE collection name
            EE_COLLECTION=EE_COLLECTION_GEN.format(var=VAR)
            # specify Google Cloud Storage folder name
            GS_FOLDER=COLLECTION[1:]+'_'+VAR

            # 2. Fetch, process, stage, ingest, clean
            new_assets = processNewData(files, var_num, last_date)
            # get list of new dates from the new assets
            new_dates = [getDateTime(a) for a in new_assets]

            # get list of all dates we now have data for by combining existing dates with new dates
            all_dates = existing_dates_by_var[var_num] + new_dates
            # get list of existing assets in current variable's GEE collection
            existing_assets = eeUtil.ls(EE_COLLECTION)
            # make list of all assets by combining existing assets with new assets
            all_assets = np.sort(np.unique(existing_assets + [os.path.split(asset)[1] for asset in new_assets]))
            logging.info('Existing assets for {}: {}, new: {}, max: {}'.format(
                VAR, len(all_dates), len(new_dates), MAX_ASSETS))
            #if we have shortened the time period we are interested in, we will need to delete the extra assets
            deleteExcessAssets(all_assets, MAX_ASSETS)
            logging.info('SUCCESS for {}'.format(VAR))

    for var_num in range(len(VARS)):
        VAR = VARS[var_num]
        EE_COLLECTION = EE_COLLECTION_GEN.format(var=VAR)
        existing_assets = eeUtil.ls(EE_COLLECTION)
        try:
            # Get most recent update date
            # to show most recent date in collection, instead of start date for forecast run
            # use get_most_recent_date(new_assets) function instead
            most_recent_date = get_forecast_run_date(existing_assets)
            current_date = getLastUpdate(DATASET_IDS[VAR])

            if current_date != most_recent_date:
                logging.info('Updating last update date and flushing cache.')
                # Update data set's last update date on Resource Watch
                lastUpdateDate(DATASET_IDS[VAR], most_recent_date)
                # get layer ids and flush tile cache for each
                layer_ids = getLayerIDs(DATASET_IDS[VAR])
                for layer_id in layer_ids:
                    flushTileCache(layer_id)
        except KeyError:
            continue

    # Delete local netcdf files
    if DELETE_LOCAL:
        try:
            for f in files:
                logging.info('Removing {}'.format(f))
                os.remove(f)
        except NameError:
            logging.info('No local files to clean.')

