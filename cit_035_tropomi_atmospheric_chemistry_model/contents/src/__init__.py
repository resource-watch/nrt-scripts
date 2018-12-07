from __future__ import unicode_literals

import os
import sys
import datetime
import logging
import eeUtil
import ee
import time
import requests

# Sources for nrt data
SOURCE_URL = 'COPERNICUS/S5P/OFFL/L3_{var}'
VARS = ['NO2', 'CO', 'AER_AI']
BANDS = ['tropospheric_NO2_column_number_density', 'CO_column_number_density', 'absorbing_aerosol_index']
NODATA_VALUE = None
'''
GDAL: Assign a specified nodata value to output bands. Starting with GDAL 1.8.0, can be set to none to avoid setting
a nodata value to the output file if one exists for the source file. Note that, if the input dataset has a nodata 
value, this does not cause pixel values that are equal to that nodata value to be changed to the value specified 
with this option.
'''

DATA_DIR = 'data'
CLEAR_COLLECTION_FIRST = False

DAYS_TO_AVERAGE = 7
RESOLUTION = 3.5 #km
'''
If DAYS_TO_AVERAGE = 1, consider using a larger number of max assets (30) to ensure that you find a day 
with orbits that cover the entire globe. Data are not uploaded regularly, and some days have large gaps 
in data coverage.

When averaging, a near-complete global map is not required for a particular day's data to be used. However, 
because the data are not uploaded very regularly, a value of ~20 should be used for the MAX_ASSETS to ensure 
that you look back far enough to find the most recent data.
'''
MAX_ASSETS = 3
DATE_FORMAT_DATASET = '%Y-%m-%d'
DATE_FORMAT = '%Y-%m-%d'
TIMESTEP = {'days': 1}
TIMEOUT = 5000

COLLECTION = 'cit_035_tropomi_atmospheric_chemistry_model'

LOG_LEVEL = logging.INFO

DATASET_ID = '4eadb2ae-d47b-4171-988f-186c38989fdb'
def lastUpdateDate(dataset, date):
   apiUrl = 'http://api.resourcewatch.org/v1/dataset/{0}'.format(dataset)
   headers = {
   'Content-Type': 'application/json',
   'Authorization': os.getenv('apiToken')
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
    if DAYS_TO_AVERAGE==1:
        return os.path.join(EE_COLLECTION, FILENAME.format(var=VAR, date=date))
    else:
        return os.path.join(EE_COLLECTION, FILENAME.format(days=DAYS_TO_AVERAGE, var=VAR, date=date))

def getDate(filename):
    '''get last 8 chrs of filename CHECK THIS'''
    return os.path.splitext(os.path.basename(filename))[0][-10:]

def getNewDates(exclude_dates):
    '''Get new dates excluding existing'''
    new_dates = []
    date = datetime.date.today()
    # if anything is in the collection, check back until last uploaded date
    if len(exclude_dates) > 0:
        while (date.strftime(DATE_FORMAT) not in exclude_dates):
            datestr = date.strftime(DATE_FORMAT_DATASET)
            new_dates.append(datestr)  #add to new dates
            date -= datetime.timedelta(**TIMESTEP)
    #if the collection is empty, make list of most recent 45 days to check
    else:
        for i in range(45):
            datestr = date.strftime(DATE_FORMAT_DATASET)
            new_dates.append(datestr)  #add to new dates
            date -= datetime.timedelta(**TIMESTEP)
    return new_dates

def getDateBounds(new_date):
    new_date_dt = datetime.datetime.strptime(new_date, DATE_FORMAT_DATASET)
    #add one day to the date of interest to make sure that day is included in the average
    #google earth engine does not include the end date specified when filtering dates
    end_date = (new_date_dt + datetime.timedelta(**TIMESTEP)).strftime(DATE_FORMAT_DATASET)
    start_date = (new_date_dt - (DAYS_TO_AVERAGE - 1) * datetime.timedelta(**TIMESTEP)).strftime(DATE_FORMAT_DATASET)
    return end_date, start_date

def fetch_single_day(new_dates):
    # Loop over the new dates, check which dates have good global coverage, and add them to a list
    dates = []
    daily_images = []
    for date in new_dates:
        try:
            IC = ee.ImageCollection(SOURCE_URL.format(var=VAR))
            end_date = datetime.datetime.strptime(date,'%Y-%m-%d')+datetime.timedelta(**TIMESTEP)
            end_date_str = end_date.strftime(DATE_FORMAT_DATASET)
            IC_1day = IC.filterDate(date, end_date_str).select([BAND])
            if IC_1day.size().getInfo() > 10:
                mosaicked_image = IC_1day.mosaic()
                daily_images.append(mosaicked_image)
                dates.append(date)
                logging.info('Successfully retrieved {}'.format(date))
            else:
                logging.info('Poor global coverage for {}, discarding'.format(date))
            # stop if the list exceeds our max assets
            if len(dates) >= MAX_ASSETS:
                break
        except Exception as e:
            logging.error('Unable to retrieve data from {}'.format(date))
            logging.debug(e)
    return dates, daily_images

def fetch_multi_day_avg(new_dates):
    # Loop over the new dates, check if there is data available, add them to a list
    averages = []
    dates = []
    for new_date in new_dates:
        try:
            end_date, start_date = getDateBounds(new_date)
            IC = ee.ImageCollection(SOURCE_URL.format(var=VAR))
            #get band of interest
            IC_band = IC.select([BAND])
            # check if any data available for new date yet
            new_date_IC = IC_band.filterDate(new_date, end_date)
            if new_date_IC.size().getInfo() > 0:
                dates.append(new_date)
                #get dates to average
                IC_dates_to_average = IC_band.filterDate(start_date, end_date)
                average = IC_dates_to_average.mean()
                averages.append(average)
                logging.info('Successfully retrieved {}'.format(new_date))
            else:
                logging.info('No data available for {}'.format(new_date))
            # stop if the list exceeds our max assets
            if len(dates) >= MAX_ASSETS:
                break
        except Exception as e:
            logging.error('Unable to retrieve data from {}'.format(new_date))
            logging.debug(e)
    return dates, averages

def processNewData(existing_dates):
    '''fetch, process, upload, and clean new data'''
    # 1. Determine which files to fetch
    new_dates = getNewDates(existing_dates)

    # 2. Fetch new files
    logging.info('Fetching files')
    if DAYS_TO_AVERAGE == 1:
        dates, images = fetch_single_day(new_dates)
    else:
        dates, images = fetch_multi_day_avg(new_dates)

    if dates: #if files is an empty list do nothing, if something in it:
        # 4. Upload new files
        logging.info('Uploading files')
        assets = [('users/resourcewatch_wri/' + getAssetName(date)) for date in dates]
        lon = 179.999
        lat = 89.999
        scale = RESOLUTION*1000
        geometry = [[[-lon, lat], [lon, lat], [lon, -lat], [-lon, -lat], [-lon, lat]]]
        for i in range(len(dates)):
            logging.info('Uploading ' + assets[i])
            task = ee.batch.Export.image.toAsset(images[i],
                                                 assetId=assets[i],
                                                 region=geometry, scale=scale, maxPixels=1e13)
            task.start()
            state = 'RUNNING'
            start = time.time()
            #wait for task to complete
            #check if task was successful
            while state == 'RUNNING' and (time.time() - start) < TIMEOUT:
                time.sleep(60)
                status = task.status()['state']
                logging.info('Current Status: ' + status +', run time (min): ' + str((time.time() - start)/60))
                if status == 'COMPLETED':
                    state = status
                    logging.info(status)
                elif status == 'FAILED':
                    state = status
                    logging.error(task.status()['error_message'])
                    logging.debug(task.status())

        return assets
    else:
        return []


def checkCreateCollection(collection):
    '''List assests in collection else create new collection'''
    if not eeUtil.exists(PARENT_FOLDER):
        logging.info('{} does not exist, creating'.format(PARENT_FOLDER))
        eeUtil.createFolder(PARENT_FOLDER, public=True)
    if eeUtil.exists(collection):
        return eeUtil.ls(collection)
    else:
        logging.info('{} does not exist, creating'.format(collection))
        eeUtil.createFolder(collection, True, public=True)
        return []


def deleteExcessAssets(dates, max_assets):
    '''Delete assets if too many'''
    # oldest first
    dates.sort()
    if len(dates) > max_assets:
        for date in dates[:-max_assets]:
            eeUtil.removeAsset(getAssetName(date))

def initialize_ee():
    GEE_JSON = os.environ.get("GEE_JSON")
    _CREDENTIAL_FILE = 'credentials.json'
    GEE_SERVICE_ACCOUNT = os.environ.get("GEE_SERVICE_ACCOUNT")
    with open(_CREDENTIAL_FILE, 'w') as f:
        f.write(GEE_JSON)
    auth = ee.ServiceAccountCredentials(GEE_SERVICE_ACCOUNT, _CREDENTIAL_FILE)
    ee.Initialize(auth)

def main():
    global VAR
    global BAND
    global EE_COLLECTION
    global PARENT_FOLDER
    global FILENAME
    if DAYS_TO_AVERAGE == 1:
        PARENT_FOLDER = COLLECTION
        EE_COLLECTION_GEN = COLLECTION + '/{var}'
        FILENAME = COLLECTION+'_{var}_{date}'
    else:
        PARENT_FOLDER = COLLECTION + '_{days}day_avg'.format(days=DAYS_TO_AVERAGE)
        EE_COLLECTION_GEN = COLLECTION + '_%sday_avg/{var}' %DAYS_TO_AVERAGE
        FILENAME = COLLECTION+'_{days}day_avg_{var}_{date}'
    '''Ingest new data into EE and delete old data'''
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    # Initialize eeUtil and ee
    eeUtil.initJson()
    initialize_ee()
    for i in range(len(VARS)):
        VAR = VARS[i]
        logging.info('STARTING {var}'.format(var=VAR))
        BAND = BANDS[i]
        EE_COLLECTION=EE_COLLECTION_GEN.format(var=VAR)
        # Clear collection in GEE if desired
        if CLEAR_COLLECTION_FIRST:
            if eeUtil.exists(EE_COLLECTION):
                eeUtil.removeAsset(EE_COLLECTION, recursive=True)
        # 1. Check if collection exists and create
        existing_assets = checkCreateCollection(EE_COLLECTION) #make image collection if doesn't have one
        existing_dates = [getDate(a) for a in existing_assets]
        # 2. Fetch, process, stage, ingest, clean
        new_assets = processNewData(existing_dates)
        new_dates = [getDate(a) for a in new_assets]
        # 3. Delete old assets
        existing_dates = existing_dates + new_dates
        logging.info('Existing assets: {}, new: {}, max: {}'.format(
            len(existing_dates), len(new_dates), MAX_ASSETS))
        deleteExcessAssets(existing_dates, MAX_ASSETS)
        existing_assets = checkCreateCollection(EE_COLLECTION) #make image collection if doesn't have one
        existing_dates = [getDate(a) for a in existing_assets]
        existing_dates.sort()
        most_recent_date = datetime.datetime.strptime(existing_dates[-1], DATE_FORMAT)
        lastUpdateDate(DATASET_ID, most_recent_date)
        logging.info('SUCCESS for {var}'.format(var=VAR))
