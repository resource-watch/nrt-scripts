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
VARS = ['NO2', 'CO', 'AER_AI', 'O3']
BANDS = ['tropospheric_NO2_column_number_density', 'CO_column_number_density', 'absorbing_aerosol_index', 'O3_column_number_density']

DATA_DIR = 'data'

DAYS_TO_AVERAGE = 30
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

DATASET_IDS = {
    'NO2': 'b75d8398-34f2-447d-832d-ea570451995a',
    'CO': 'f84ce519-8128-4a24-b637-89711b9e4713',
    'AER_AI': '793e4cc9-c060-4b7f-a4a2-0b1fbbe71b69',
    'O3': 'ada81921-28ff-4fbb-b971-7aa1f3ccdb22'
}

apiToken = os.getenv('apiToken') or os.environ.get('rw_api_token') or os.environ.get('RW_API_KEY')

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

def getCollectionName(var):
    '''
    get GEE collection name
    INPUT   var: variable to be used in asset name (string)
    RETURN  GEE collection name for input date (string)
    '''
    return EE_COLLECTION_GEN.format(var=var)

def getAssetName(var, date):
    '''get asset name from datestamp'''# os.path.join('home', 'coming') = 'home/coming'
    collection = getCollectionName(var)
    if DAYS_TO_AVERAGE==1:
        return os.path.join(collection, FILENAME.format(var=var, date=date))
    else:
        return os.path.join(collection, FILENAME.format(days=DAYS_TO_AVERAGE, var=var, date=date))

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

def fetch_single_day(var, new_dates):
    # Loop over the new dates, check which dates have good global coverage, and add them to a list
    dates = []
    daily_images = []
    for date in new_dates:
        try:
            IC = ee.ImageCollection(SOURCE_URL.format(var=var))
            end_date = datetime.datetime.strptime(date,'%Y-%m-%d')+datetime.timedelta(**TIMESTEP)
            end_date_str = end_date.strftime(DATE_FORMAT_DATASET)
            IC_1day = IC.filterDate(date, end_date_str).select([BAND])
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
            IC_band = IC.select([BAND])
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

def initialize_ee():
    GEE_JSON = os.environ.get("GEE_JSON")
    _CREDENTIAL_FILE = 'credentials.json'
    GEE_SERVICE_ACCOUNT = os.environ.get("GEE_SERVICE_ACCOUNT")
    with open(_CREDENTIAL_FILE, 'w') as f:
        f.write(GEE_JSON)
    auth = ee.ServiceAccountCredentials(GEE_SERVICE_ACCOUNT, _CREDENTIAL_FILE)
    ee.Initialize(auth)

def get_most_recent_date(collection):
    existing_assets = checkCreateCollection('/'+collection)  # make image collection if doesn't have one
    existing_dates = [getDate(a) for a in existing_assets]
    existing_dates.sort()
    most_recent_date = datetime.datetime.strptime(existing_dates[-1], DATE_FORMAT)
    return most_recent_date

def main():
    global BAND
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    # Initialize eeUtil and ee
    eeUtil.initJson()
    initialize_ee()
    for i in range(len(VARS)):
        var = VARS[i]
        logging.info('STARTING {var}'.format(var=var))
        BAND = BANDS[i]
        collection = getCollectionName(var)
        # Clear collection in GEE if desired
        if CLEAR_COLLECTION_FIRST:
            if eeUtil.exists(collection):
                eeUtil.removeAsset('/'+collection, recursive=True)
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