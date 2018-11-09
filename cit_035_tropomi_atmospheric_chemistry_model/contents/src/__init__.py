from __future__ import unicode_literals

import os
import sys
import datetime
import logging
import eeUtil
import ee

# Sources for nrt data
SOURCE_URL = 'COPERNICUS/S5P/OFFL/L3_{var}'
VARS = ['NO2', 'CO', 'AER_AI']
BANDS = ['tropospheric_NO2_column_number_density', 'CO_column_number_density', 'absorbing_aerosol_index']
FILENAME = 'cit_035_tropomi_atmospheric_chemistry_model_{var}_{date}'
NODATA_VALUE = None
'''
GDAL: Assign a specified nodata value to output bands. Starting with GDAL 1.8.0, can be set to none to avoid setting
a nodata value to the output file if one exists for the source file. Note that, if the input dataset has a nodata 
value, this does not cause pixel values that are equal to that nodata value to be changed to the value specified 
with this option.
'''

DATA_DIR = 'data'
GS_FOLDER_GEN = 'cit_035_tropomi_atmospheric_chemistry_model_{var}'
EE_COLLECTION_GEN = 'cit_035_tropomi_atmospheric_chemistry_model_{var}'
CLEAR_COLLECTION_FIRST = False

MAX_ASSETS = 7
DATE_FORMAT_DATASET = '%Y-%m-%d'
DATE_FORMAT = '%Y%m%d'
TIMESTEP = {'days': 1}

LOG_LEVEL = logging.INFO

def getAssetName(date):
    '''get asset name from datestamp'''# os.path.join('home', 'coming') = 'home/coming'
    return os.path.join(EE_COLLECTION, FILENAME.format(var=VAR, date=date))


def getFilename(date):
    '''get filename from datestamp CHECK FILE TYPE'''
    return os.path.join(DATA_DIR, '{}.nc'.format(
        FILENAME.format(var=VAR, date=date)))
        
def getDate(filename):
    '''get last 8 chrs of filename CHECK THIS'''
    return os.path.splitext(os.path.basename(filename))[0][-10:]

def getNewDates(exclude_dates):
    '''Get new dates excluding existing'''
    new_dates = []
    date = datetime.date.today()
    for i in range(MAX_ASSETS): #updates every day
        date -= datetime.timedelta(**TIMESTEP) #subtraction and assignments in one step
        datestr = date.strftime(DATE_FORMAT_DATASET)#of NETCDF because looking for new data in old format
        if date.strftime(DATE_FORMAT) not in exclude_dates:
            new_dates.append(datestr) #add to new dates if have not already seen
    return new_dates


def fetch(new_dates):
    # 2. Loop over the new dates, check if there is data available, and attempt to download the hdfs
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
                logging.info('Successfully retrieved {}'.format(date))# gives us "Successully retrieved file name"
        except Exception as e:
            logging.error('Unable to retrieve data from {}'.format(date))
            logging.debug(e)
    return dates, daily_images

def processNewData(existing_dates):
    '''fetch, process, upload, and clean new data'''
    # 1. Determine which files to fetch
    new_dates = getNewDates(existing_dates)

    # 2. Fetch new files
    logging.info('Fetching files')
    dates, daily_images = fetch(new_dates) #get list of locations of netcdfs in docker container

    if dates: #if files is an empty list do nothing, if something in it:
        # 4. Upload new files
        logging.info('Uploading files')
        assets = [('users/resourcewatch_wri/' + getAssetName(date)) for date in dates]
        lon = 179.999
        lat = 89.999
        scale = 500
        geometry = [[[-lon, lat], [lon, lat], [lon, -lat], [-lon, -lat], [-lon, lat]]]
        for i in range(len(dates)):
            task = ee.batch.Export.image.toAsset(daily_images[i],
                                                 assetId=assets[i],
                                                 region=geometry, scale=scale, maxPixels=1e13)
            task.start()
        return assets
    else:
        return []


def checkCreateCollection(collection):
    '''List assests in collection else create new collection'''
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
    global GS_FOLDER
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
        GS_FOLDER=GS_FOLDER_GEN.format(var=VAR)
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
        logging.info('SUCCESS for {var}'.format(var=VAR))
