from __future__ import unicode_literals

import os
import sys
import datetime
import logging
import eeUtil
import ee
import time
import requests

# url for air quality data
SOURCE_URL = 'COPERNICUS/S5P/OFFL/L3_{var}'

# list variables (as named in GEE) that we want to pull
VARS = ['NO2', 'CO', 'AER_AI', 'O3']

# define band to use for each compound
BAND_BY_COMPOUND = {
    'NO2': 'tropospheric_NO2_column_number_density',
    'CO': 'CO_column_number_density',
    'AER_AI': 'absorbing_aerosol_index',
    'O3': 'O3_column_number_density',
}

# name of data directory in Docker container
DATA_DIR = 'data'

# name of collection in GEE where we will upload the final data
COLLECTION = 'projects/resource-watch-gee/cit_035_tropomi_atmospheric_chemistry_model'
# generate name for dataset's parent folder on GEE which will be used to store
# several collections - one collection per variable
PARENT_FOLDER = COLLECTION + f'_{DAYS_TO_AVERAGE}day_avg'
# generate generic string that can be formatted to name each variable's GEE collection
EE_COLLECTION_GEN = PARENT_FOLDER + '/{var}'
# generate generic string that can be formatted to name each variable's asset name
FILENAME = PARENT_FOLDER.split('/')[-1] + '_{var}_{date}'
# specify Google Cloud Storage folder name
GS_FOLDER = COLLECTION[1:]

# do you want to delete everything currently in the GEE collection when you run this script?
CLEAR_COLLECTION_FIRST = False

# How many days of data do you want to average together to create the processed image?
# note: If DAYS_TO_AVERAGE = 1, consider using a larger number of assets (30) to ensure that you find a day with orbits
# that cover the entire globe. Data are not uploaded regularly, and some days have large gaps in data coverage.
DAYS_TO_AVERAGE = 30

# at what resolution should the processed image be calculated?
RESOLUTION = 3.5 #km

# how many assets can be stored in the GEE collection before the oldest ones are deleted?
MAX_ASSETS = 3

# format of date (used in both the source data files and GEE)
DATE_FORMAT = '%Y-%m-%d'

# Resource Watch dataset API IDs
# Important! Before testing this script:
# Please change these IDs OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on different datasets on Resource Watch
DATASET_IDS = {
    'NO2': 'b75d8398-34f2-447d-832d-ea570451995a',
    'CO': 'f84ce519-8128-4a24-b637-89711b9e4713',
    'AER_AI': '793e4cc9-c060-4b7f-a4a2-0b1fbbe71b69',
    'O3': 'ada81921-28ff-4fbb-b971-7aa1f3ccdb22'
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
    if DAYS_TO_AVERAGE==1:
        return os.path.join(collection, FILENAME.format(var=var, date=date))
    else:
        return os.path.join(collection, FILENAME.format(days=DAYS_TO_AVERAGE, var=var, date=date))

def getDate_GEE(filename):
    '''
    get date from Google Earth Engine asset name (last 10 characters of filename after removing extension)
    INPUT   filename: asset name that ends in a date of the format YYYY-MM-DD (string)
    RETURN  date in the format YYYY-MM-DD (string)
    '''
    return os.path.splitext(os.path.basename(filename))[0][-10:]

def getNewDates(existing_dates):
    '''
    Get new dates we want to try to fetch data for
    INPUT   existing_dates: list of dates that we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
    RETURN  new_dates: list of new dates we want to try to get, in the format of the DATE_FORMAT variable (list of strings)
    '''
    # create empty list to store dates we should process
    new_dates = []

    # start with today's date and time
    date = datetime.date.today()
    # if anything is in the collection, check back until last uploaded date
    if len(exclude_dates) > 0:
        while (date.strftime(DATE_FORMAT) not in existing_dates):
            # generate date string in same format used in GEE collection
            datestr = date.strftime(DATE_FORMAT)
            # add to list of new dates
            new_dates.append(datestr)
            # go back one more day
            date -= datetime.timedelta(days=1)
    # if the collection is empty, make list of most recent 45 days to check
    else:
        for i in range(45):
            # generate date string in same format used in GEE collection
            datestr = date.strftime(DATE_FORMAT)
            # add to list of new dates
            new_dates.append(datestr)
            # go back one more day
            date -= datetime.timedelta(days=1)
    return new_dates

def getDateBounds(new_date):
    new_date_dt = datetime.datetime.strptime(new_date, DATE_FORMAT)
    #add one day to the date of interest to make sure that day is included in the average
    #google earth engine does not include the end date specified when filtering dates
    end_date = (new_date_dt + datetime.timedelta(days=1)).strftime(DATE_FORMAT)
    start_date = (new_date_dt - (DAYS_TO_AVERAGE - 1) * datetime.timedelta(days=1)).strftime(DATE_FORMAT)
    return end_date, start_date

def fetch_single_day(var, new_dates):
    # Loop over the new dates, check which dates have good global coverage, and add them to a list
    dates = []
    daily_images = []
    for date in new_dates:
        try:
            IC = ee.ImageCollection(SOURCE_URL.format(var=var))
            end_date = datetime.datetime.strptime(date,'%Y-%m-%d')+datetime.timedelta(days=1)
            end_date_str = end_date.strftime(DATE_FORMAT)
            IC_1day = IC.filterDate(date, end_date_str).select([BAND_BY_COMPOUND[var]])
            if IC_1day.size().getInfo() > 10:
                mean_image = IC_1day.mean()
                #copy most recent system start time from that day's images
                sorted = IC_1day.sort(prop='system:time_start', opt_ascending=False);
                most_recent_image = ee.Image(sorted.first())
                mean_image = mean_image.copyProperties(most_recent_image, ['system:time_start'])
                #add image to list for upload
                daily_images.append(mean_image)
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

def fetch_multi_day_avg(var, new_dates):
    # Loop over the new dates, check if there is data available, add them to a list
    averages = []
    dates = []
    for new_date in new_dates:
        try:
            end_date, start_date = getDateBounds(new_date)
            IC = ee.ImageCollection(SOURCE_URL.format(var=var))
            #get band of interest
            IC_band = IC.select([BAND_BY_COMPOUND[var]])
            # check if any data available for new date yet
            new_date_IC = IC_band.filterDate(new_date, end_date)
            if new_date_IC.size().getInfo() > 0:
                dates.append(new_date)
                #get dates to average
                IC_dates_to_average = IC_band.filterDate(start_date, end_date)
                average = IC_dates_to_average.mean()
                #copy most recent system start time from time period images
                sorted = IC_dates_to_average.sort(prop='system:time_start', opt_ascending=False);
                most_recent_image = ee.Image(sorted.first())
                average = average.copyProperties(most_recent_image, ['system:time_start'])
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

def processNewData(var, existing_dates):
    '''fetch, process, upload, and clean new data'''
    # 1. Determine which files to fetch
    new_dates = getNewDates(existing_dates)

    # 2. Fetch new files
    logging.info('Fetching files')
    if DAYS_TO_AVERAGE == 1:
        dates, images = fetch_single_day(var, new_dates)
    else:
        dates, images = fetch_multi_day_avg(var, new_dates)

    if dates: #if files is an empty list do nothing, if something in it:
        # 4. Upload new files
        logging.info('Uploading files')
        assets = [getAssetName(var, date) for date in dates]
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
            while state == 'RUNNING' and (time.time() - start) < 5000:
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
    if not eeUtil.exists('/'+PARENT_FOLDER):
        logging.info('{} does not exist, creating'.format(PARENT_FOLDER))
        eeUtil.createFolder('/'+PARENT_FOLDER, public=True)
    if eeUtil.exists(collection):
        return eeUtil.ls(collection)
    else:
        logging.info('{} does not exist, creating'.format(collection))
        eeUtil.createFolder(collection, True, public=True)
        return []


def deleteExcessAssets(var, dates, max_assets):
    '''Delete assets if too many'''
    # oldest first
    dates.sort()
    if len(dates) > max_assets:
        for date in dates[:-max_assets]:
            eeUtil.removeAsset('/'+getAssetName(var, date))

def get_most_recent_date(collection):
    '''
    Get most recent date from the data in the GEE collection
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
    most_recent_date = datetime.datetime.strptime(existing_dates[-1], DATE_FORMAT)
    return most_recent_date

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

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Initialize eeUtil and ee modules
    eeUtil.initJson()
    initialize_ee()

    # Clear collection in GEE if desired
    if CLEAR_COLLECTION_FIRST:
        clearCollectionMultiVar()

    for i in range(len(VARS)):
        var = VARS[i]
        logging.info('STARTING {var}'.format(var=var))
        collection = getCollectionName(var)

        # 1. Check if collection exists and create
        existing_assets = checkCreateCollection('/'+collection) #make image collection if doesn't have one
        existing_dates = [getDate(a) for a in existing_assets]
        # 2. Fetch, process, stage, ingest, clean
        new_assets = processNewData(var, existing_dates)
        new_dates = [getDate(a) for a in new_assets]
        # 3. Delete old assets
        existing_dates = existing_dates + new_dates
        logging.info(existing_dates)
        logging.info('Existing assets: {}, new: {}, max: {}'.format(
            len(existing_dates), len(new_dates), MAX_ASSETS))
        deleteExcessAssets(var, existing_dates, MAX_ASSETS)
        # Get most recent update date
        most_recent_date = get_most_recent_date(collection)
        current_date = getLastUpdate(DATASET_IDS[var])

        if current_date != most_recent_date:
            logging.info('Updating last update date and flushing cache.')
            # Update data set's last update date on Resource Watch
            lastUpdateDate(DATASET_IDS[var], most_recent_date)
            # get layer ids and flush tile cache for each
            layer_ids = getLayerIDs(DATASET_IDS[var])
            for layer_id in layer_ids:
                flushTileCache(layer_id)
        logging.info('SUCCESS for {var}'.format(var=var))
