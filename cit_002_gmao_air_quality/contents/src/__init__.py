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
from string import ascii_uppercase


# Sources for nrt data
SDS_NAME = 'NETCDF:"{fname}":{var}'
NODATA_VALUE = 9.9999999E14

DATA_DIR = 'data'
COLLECTION = '/projects/resource-watch-gee/cit_002_gmao_air_quality'
CLEAR_COLLECTION_FIRST = False
DELETE_LOCAL = True

#date format to use in GEE
DATE_FORMAT = '%Y-%m-%d'
TIMESTEP = {'days': 1}

#set last time you want to call
END_TIME = None

LOG_LEVEL = logging.INFO

DATASET_IDS = {
    'NO2':'ecce902d-a322-4d13-a3d6-e1a36fc5573e',
    'O3':'ebc079a1-51d8-4622-ba25-d8f3b4fcf8b3',
    'PM25_RH35_GCC':'645fe192-28db-4949-95b9-79d898f4226b',
}
apiToken = os.getenv('apiToken')

SOURCE_URL_HISTORICAL = 'https://portal.nccs.nasa.gov/datashare/gmao/geos-cf/v1/das/Y{year}/M{month}/D{day}/GEOS-CF.v01.rpl.chm_tavg_1hr_g1440x721_v1.{year}{month}{day}_{time}z.nc4'
SOURCE_URL_FORECAST = 'https://portal.nccs.nasa.gov/datashare/gmao/geos-cf/v1/forecast/Y{year}/M{month}/D{day}/H12/GEOS-CF.v01.fcst.chm_tavg_1hr_g1440x721_v1.{start_year}{start_month}{start_day}_12z+{year}{month}{day}_{time}z.nc4'
VARS = ['NO2', 'O3', 'PM25_RH35_GCC', 'PM25_RH35_GOCART']
# need to specify which pressure level of data we want for each variable (out of available levels)
# only has one pressure level available (surface)
NUM_AVAILABLE_LEVELS = [1, 1, 1, 1]
DESIRED_LEVELS = [1, 1, 1, 1]

#how many assets can be stored in the GEE collection before the oldest ones are deleted?
MAX_ASSETS = 14

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

def getTiffname(file, variable):
    '''get filename from datestamp CHECK FILE TYPE'''
    year = file.split('/')[1][-18:-14]
    month = file.split('/')[1][-14:-12]
    day = file.split('/')[1][-12:-10]
    time = file.split('/')[1][-9:-5]
    name = os.path.join(DATA_DIR, FILENAME.format(var=variable, date=year+'-'+month+'-'+day +'_'+time))+'.tif'
    return name

def getDateTime(filename):
    '''get last 8 chrs of filename CHECK THIS'''
    return os.path.splitext(os.path.basename(filename))[0][-10:]

def getDate_GEE(filename):
    '''get last 10 chrs of filename CHECK THIS'''
    return os.path.splitext(os.path.basename(filename))[0][-10:]

def list_available_files(url, file_start=''):
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    return [node.get('href') for node in soup.find_all('a') if type(node.get('href'))==str and node.get('href').startswith(file_start)]

def getNewDatesHistorical(existing_dates):
    #create empty list to store dates we should process
    new_dates = []

    # start with today
    date = datetime.datetime.utcnow()
    #generate date string in same format used in GEE collection
    date_str = datetime.datetime.strftime(date, DATE_FORMAT)

    #generate date to stop at if collection is empty
    last_date = date - datetime.timedelta(days=MAX_ASSETS)

    #if the date is not in our list of existing dates:
    while (date_str not in existing_dates) and (date!=last_date):
        #general source url for this day's data folder
        url = SOURCE_URL_HISTORICAL.split('/GEOS')[0].format(year=date.year, month=date.month, day=date.day)
        # check the files available for this day:
        files = list_available_files(url, file_start='GEOS-CF.v01.rpl.chm_tavg')
        #if all 24 hourly files are available, we can process this data - add it to the list
        if len(files) == 24:
            new_dates.append(date_str)
        # go back one more day
        date = date - datetime.timedelta(days=1)
        # generate new string in same format used in GEE collection
        date_str = datetime.datetime.strftime(date, DATE_FORMAT)
    #repeat until we reach something in our existing dates

    #reverse order so we pull oldest date first
    new_dates.reverse()
    return new_dates

def convert(files):
    '''convert netcdfs to tifs'''
    #create an empty list to store the names of tif files that we create
    tifs = []
    for f in files:
        logging.info('Converting {} to tiff'.format(f))
        # get command to call the netcdf file for a particular variable
        sds_path = SDS_NAME.format(fname=f, var=VAR)
        '''
        Google Earth Engine needs to get tif files with longitudes of -180 to 180.
        These files have longitudes in the correct format. I checked this using gdalinfo.
        I downloaded a file onto my local computer and in command line, ran:
                gdalinfo NETCDF:"{file_loc/file_name}":{variable}
        with the values in {} replaced with the correct information.
        I looked at the 'Corner Coordinates' that were printed out.

        '''
        #only one band available in each file, so we will pull band 1
        band = 1
        # generate names for tif files that we are going to create from netcdf
        tif = getTiffname(file=f, variable=VAR)
        # translate this file from a netcdf to a tif
        cmd = ['gdal_translate', '-b', str(band), '-q', '-a_nodata', str(NODATA_VALUE), '-a_srs', 'EPSG:4326', sds_path, tif] #'-q' means quiet so you don't see it
        subprocess.call(cmd) #using the gdal from command line from inside python

        # add name of tif to our list of tif files
        tifs.append(tif)
    return tifs

def fetch(new_dates, unformatted_source_url):
    #Create an empty list to store file locations of netcdfs that are downloaded.
    files = []
    # create a list of hours to pull (24 hours per day, on the half-hour)
    hours = ['0030', '0130', '0230', '0330', '0430', '0530', '0630', '0730', '0830', '0930', '1030', '1130', '1230', '1330', '1430', '1530', '1630', '1730', '1830', '1930', '2030', '2130', '2230', '2330']
    # Loop over all hours of the new dates, check if there is data available, and download netcdfs
    for date in new_dates:
        for hour in hours:
            # Set up the url of the filename to download
            url = unformatted_source_url.format(year=int(date[:4]), month=int(date[5:7]), day=int(date[8:]), time=hour)
            # Create a file name to store the netcdf in after download
            f = DATA_DIR+'/'+url.split('/')[-1]
            logging.info('Retrieving {}'.format(f))
            #try to download file
            tries = 0
            while tries <3:
                try:
                    #download files from url and put in specified file location (f)
                    urllib.request.urlretrieve(url, f)
                    #add file name/location to list of files downloaded
                    files.append(f)
                    break
                #if download fails, throw an error
                except Exception as e:
                    logging.info('Unable to retrieve data from {}'.format(url))
                    logging.info(e)
                    tries+=1
                    logging.info('try {}'.format(tries))
            if tries==3:
                logging.error('Unable to retrieve data from {}'.format(url))
                exit()
    #return list of files just downloaded
    return files

def daily_avg(hourly_tifs, dates):
    #create an empty list to store the names of the daily avg tifs we create
    daily_avg_tifs= []
    #process daily tifs, one day at a time
    for date in dates:
        #create and emply list to store the files that should be averaged for this date
        tifs_for_date = []
        #for each file in our list of hourly tifs, check if if is for the current data
        for file in hourly_tifs:
            if date in file:
                #if this file is for the current date, append it to the list of tifs to average for this day
                tifs_for_date.append(file)

        # Calculating the daily average:
        # create a list to store the tifs and variable names to be used in gdal_calc
        gdal_tif_list=[]
        #set up calc input for gdal_calc
        calc = '--calc="('
        #go through each hour in the day to be averaged
        for i in range(len(tifs_for_date)):
            #generate a letter variable for that tif to use in gdal_calc
            letter = ascii_uppercase[i]
            gdal_tif_list.append('-'+letter)
            #pull the tif name
            tif = tifs_for_date[i]
            gdal_tif_list.append('"'+tif+'"')
            #add the variable to the calc input for gdal_calc
            if i==0:
                calc= calc +letter
            else:
                calc = calc+'+'+letter
        #calculate the number of tifs we are averaging and finish creating calc input
        num_tifs = len(tifs_for_date)
        calc= calc + ')/{}"'.format(num_tifs)
        #generate a file name for the daily average tif
        result_tif = DATA_DIR+'/'+FILENAME.format(var=VAR, date=date)+'.tif'
        #create the gdal command to calculate the average by putting it all together
        cmd = ('gdal_calc.py {} --outfile="{}" {}').format(' '.join(gdal_tif_list), result_tif, calc)
        # using gdal from command line from inside python
        subprocess.check_output(cmd, shell=True)
        daily_avg_tifs.append(result_tif)
    return daily_avg_tifs

def processNewData(files, dates):
    '''process, upload, and clean new data'''
    if files: #if files is empty list do nothing, otherwise, process data
        logging.info('Converting files')
        # Convert netcdfs to tifs
        hourly_tifs = convert(files)

        #take daily average of hourly tif files
        tifs = daily_avg(hourly_tifs, dates)
        #get new list of dates (in case order is different) from the averaged tifs
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

        return assets, dates
    #if no new assets, return empty list
    else:
        return [], []

def checkCreateCollection(VARS):
    #create a master list (not variable-specific) of which dates we already have data for
    existing_dates = []
    #create an empty list to store the dates that we currently have for each AQ variable
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
            #get a list of the dates from these existing assets
            dates = [getDate_GEE(a) for a in existing_assets]
            #append this list of dates to our list of dates by variable
            existing_dates_by_var.append(dates)

            #for each of the dates that we have for this variable, append the date to the master
            # list of which dates we already have data for (if it isn't already in the list)
            for date in dates:
                if date not in existing_dates:
                    existing_dates.append(date)
        #If the GEE collection does not exist, append an empty list to our list of dates by variable
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
        #check how many times each date appears in our list of dates by variable
        date_count = sum(x.count(date) for x in existing_dates_by_var)
        # If this count is less than the number of variables we have, one of the variables did not finish
        # upload for this date, and we need to re-upload this file.
        if date_count < len(VARS):
            #remove this from the list of existing dates for all variables
            existing_dates_all_vars.remove(date)
    return existing_dates_all_vars, existing_dates_by_var

def deleteExcessAssets(all_assets, max_assets):
    '''Delete assets if too many'''
    if len(all_assets) > max_assets:
        # oldest first
        all_assets.sort()
        logging.info('Deleting excess assets.')
        #delete extra assets after the number we are expecting to see
        for asset in all_assets[max_assets:]:
            eeUtil.removeAsset(EE_COLLECTION +'/'+ asset)

def get_most_recent_date(all_assets):
    all_assets.sort()
    most_recent_date = datetime.datetime.strptime(all_assets[-1][-10:], DATE_FORMAT)
    return most_recent_date

def get_forecast_run_date(all_assets):
    all_assets.sort()
    most_recent_date = datetime.datetime.strptime(all_assets[0][-10:], DATE_FORMAT)
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
    #set logging levels
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    #create global variables that will be used in many functions
    global VAR
    global EE_COLLECTION
    global EE_COLLECTION_GEN
    global PARENT_FOLDER
    global FILENAME
    global GS_FOLDER

    '''
    Process Historical Data
    '''
    # generate name for dataset's parent folder on GEE which will be used to store
    # several collections - one collection per variable
    PARENT_FOLDER = COLLECTION+'_historical'
    # generate generic string that can be formatted to name each variable's GEE collection
    EE_COLLECTION_GEN = PARENT_FOLDER + '/{var}'
    # generate generic string that can be formatted to name each variable's asset name
    FILENAME = PARENT_FOLDER.split('/')[-1]+'_{var}_{date}'

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
    logging.info('Getting existing dates.')
    existing_dates, existing_dates_by_var = checkCreateCollection(VARS)

    # Get a list of the dates that are available, minus the ones we have already uploaded correctly for all variables.
    logging.info('Getting new dates to pull.')
    new_dates = getNewDatesHistorical(existing_dates)

    # if new data is available, clear the collection because we want to store the most
    # recent forecast, not the old forecast
    # ? want to do this for forecast, not historical
    # if all_new_dates:
    #     logging.info('New forecast available')
    #     clearCollection()


    # Fetch new files
    logging.info('Fetching files for {}'.format(new_dates))
    # Download files and get list of locations of netcdfs in docker container
    files = fetch(new_dates, SOURCE_URL_HISTORICAL)
    for var_num in range(len(VARS)):
        logging.info('Processing {}'.format(VARS[var_num]))
        # get variable name
        VAR = VARS[var_num]
        # specify GEE collection name
        EE_COLLECTION=EE_COLLECTION_GEN.format(var=VAR)
        # specify Google Cloud Storage folder name
        GS_FOLDER=COLLECTION[1:]+'_'+VAR

        # Process new data files
        new_assets, new_dates = processNewData(files, new_dates)

        # get list of all dates we now have data for by combining existing dates with new dates
        all_dates = existing_dates_by_var[var_num] + new_dates
        # get list of existing assets in current variable's GEE collection
        existing_assets = eeUtil.ls(EE_COLLECTION)
        # make list of all assets by combining existing assets with new assets
        all_assets = np.sort(np.unique(existing_assets + [os.path.split(asset)[1] for asset in new_assets]))
        logging.info('Existing assets for {}: {}, new: {}, max: {}'.format(
            VAR, len(all_dates), len(new_dates), MAX_ASSETS))
        # Delete extra assets, past our maximum number allowed that we have set
        deleteExcessAssets(all_assets, MAX_ASSETS)
        logging.info('SUCCESS for {}'.format(VAR))

    #Update Last Update Date and flush tile cache on RW
    for var_num in range(len(VARS)):
        VAR = VARS[var_num]
        EE_COLLECTION = EE_COLLECTION_GEN.format(var=VAR)
        existing_assets = eeUtil.ls(EE_COLLECTION)
        try:
            # Get most recent date to use as last update date
            # to show most recent date in collection, instead of start date for forecast run
            # use get_most_recent_date(new_assets) function instead
            most_recent_date = get_most_recent_date(existing_assets)
            logging.info(most_recent_date)
            current_date = getLastUpdate(DATASET_IDS[VAR]) #comment for testing

            if current_date != most_recent_date: #comment for testing
                logging.info('Updating last update date and flushing cache.') #comment for testing
                # Update data set's last update date on Resource Watch
                lastUpdateDate(DATASET_IDS[VAR], most_recent_date) #comment for testing
                # get layer ids and flush tile cache for each
                layer_ids = getLayerIDs(DATASET_IDS[VAR]) #comment for testing
                for layer_id in layer_ids: #comment for testing
                    flushTileCache(layer_id) #comment for testing
        except KeyError:
            continue

    # Delete local netcdf and tif files
    if DELETE_LOCAL:
        try:
            for f in os.listdir(DATA_DIR):
                logging.info('Removing {}'.format(f))
                os.remove(DATA_DIR+'/'+f)
        except NameError:
            logging.info('No local files to clean.')

