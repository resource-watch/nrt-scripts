from __future__ import unicode_literals

import os
import sys
import datetime
import logging
import eeUtil
import ee
import time
import requests
import json

# url for air quality data
SOURCE_URL = 'COPERNICUS/S5P/OFFL/L3_{var}'

# list variables (as named in GEE) that we want to pull
# VARS = ['NO2', 'CO', 'AER_AI', 'O3']
VARS = ['O3']

# define band to use for each compound
BAND_BY_COMPOUND = {
    # 'NO2': 'tropospheric_NO2_column_number_density',
    # 'CO': 'CO_column_number_density',
    # 'AER_AI': 'absorbing_aerosol_index',
    'O3': 'O3_column_number_density',
}

# How many days of data do you want to average together to create the processed image?
# note: If DAYS_TO_AVERAGE = 1, consider using a larger number of assets (30) to ensure that you find a day with orbits
# that cover the entire globe. Data are not uploaded regularly, and some days have large gaps in data coverage.
DAYS_TO_AVERAGE = 30

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
    # 'NO2': 'b75d8398-34f2-447d-832d-ea570451995a',
    # 'CO': 'f84ce519-8128-4a24-b637-89711b9e4713',
    # 'AER_AI': '793e4cc9-c060-4b7f-a4a2-0b1fbbe71b69',
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
    if len(existing_dates) > 0:
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
    '''
    get start and end dates used to filter out period we are averaging over
    INPUT   new_date: date we are processing, in the format of the DATE_FORMAT variable (string)
    RETURN  end_date: end date of days we will average, in the format of the DATE_FORMAT variable (string)
            start_date: start date of days we will average, in the format of the DATE_FORMAT variable (string)
    '''
    # turn the input date into a datetime object
    new_date_dt = datetime.datetime.strptime(new_date, DATE_FORMAT)
    # add one day to the date we are processing data for to make sure that day is included in the average
    # because google earth engine does not include the end date specified when filtering dates
    # this will be the last day of our date range
    end_date = (new_date_dt + datetime.timedelta(days=1)).strftime(DATE_FORMAT)
    # go back from the date we are processing the number of days we want to average to get the start date
    start_date = (new_date_dt - (DAYS_TO_AVERAGE - 1) * datetime.timedelta(days=1)).strftime(DATE_FORMAT)
    return end_date, start_date

def getDateRange(date):
    '''
    get start date from end date we are averaging over
    INPUT   date: end date of days we will average, in the format of the DATE_FORMAT variable (string)
    RETURN  end_date: end date of days we will average, in the format of the DATE_FORMAT variable (datetime)
            start_date: start date of days we will average, in the format of the DATE_FORMAT variable (datetime)
    '''
    # turn the input date into a datetime object
    end_date = datetime.datetime.strptime(date, DATE_FORMAT)
    # go back from the date we are processing the number of days we want to average to get the start date
    start_date = (end_date - (DAYS_TO_AVERAGE) * datetime.timedelta(days=1))

    return end_date, start_date

def fetch_multi_day_avg(var, new_dates):
    '''
    Pull data for new dates and take an average over the number of days specified in the DAYS_TO_AVERAGE variable
    INPUT   var: variable we are taking an average for (string)
            new_dates: new dates we are trying to process, in the format of the DATE_FORMAT variable (list of strings)
    RETURN  dates: dates for each image we have processed, in the format of the DATE_FORMAT variable (list of strings)
            averages: processed images, averaged over the number of days specified in the DAYS_TO_AVERAGE variable (list of GEE image objects)
    '''
    # create an empty list to store averaged GEE images
    averages = []
    # create an empty list to store dates for each image
    dates = []
    # go through each of the new dates we want to try to process data for
    for new_date in new_dates:
        try:
            # get start and end dates for time period that we are averaging over
            # (end date that comes out of this will not be included in filtered data)
            end_date, start_date = getDateBounds(new_date)
            # pull the image collection for the variable of interest
            IC = ee.ImageCollection(SOURCE_URL.format(var=var))
            # get band of interest for the current variable
            IC_band = IC.select([BAND_BY_COMPOUND[var]])
            # check if any data available for new date yet
            new_date_IC = IC_band.filterDate(new_date, end_date)
            if new_date_IC.size().getInfo() > 0:
                # if data available, add to list of dates
                dates.append(new_date)
                # get dates to average
                IC_dates_to_average = IC_band.filterDate(start_date, end_date)
                # find the mean of all the images
                average = IC_dates_to_average.mean()
                # copy most recent system start time from time period images
                sorted = IC_dates_to_average.sort(prop='system:time_start', opt_ascending=False)
                most_recent_image = ee.Image(sorted.first())
                average = average.copyProperties(most_recent_image, ['system:time_start'])
                # add the averaged image to the list of processed images
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
    '''
    Fetch, process, and upload clean new data
    INPUT   var: variable that we are processing data for (string)
            existing_dates: list of dates that we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
    RETURN  assets: list of file names for processed assets that have been saved (list of strings)
    '''
    # Get a list of the dates that are available, minus the ones we have already uploaded correctly for all variables.
    new_dates = getNewDates(existing_dates)

    # Fetch new files and get average images over number of days specified
    logging.info('Fetching files')
    dates, images = fetch_multi_day_avg(var, new_dates)

    # if there are new dates available to process
    if dates:
        logging.info('Uploading files')
        # Get a list of the names we want to use for the assets once we upload the files to GEE
        assets = [getAssetName(var, date) for date in dates]
        # set the lat and lon bounds of the image we will upload to GEE
        # note: the process will faill if you use 180 and 90
        lon = 179.999
        lat = 89.999
        # change the resolution from km to m
        scale = RESOLUTION*1000
        # create the geometry bounds for the image we want to upload
        geometry = [[[-lon, lat], [lon, lat], [lon, -lat], [-lon, -lat], [-lon, lat]]]
        # upload each image
        for i in range(len(dates)):
            logging.info('Uploading ' + assets[i])
            # export the averaged image to a new asset in GEE
            task = ee.batch.Export.image.toAsset(images[i],
                                                 assetId=assets[i],
                                                 region=geometry, scale=scale, maxPixels=1e13)
            task.start()
            # set the state to 'RUNNING' because we have started the task
            state = 'RUNNING'
            # set a start time to track the time it takes to upload the image
            start = time.time()
            # wait for task to complete, but quit if it takes more than 5000 seconds
            while state == 'RUNNING' and (time.time() - start) < 5000:
                # wait for a minute before checking the state
                time.sleep(60)
                # check the status of the upload
                status = task.status()['state']
                logging.info('Current Status: ' + status +', run time (min): ' + str((time.time() - start)/60))
                # log if the task is completed and change the state
                if status == 'COMPLETED':
                    state = status
                    logging.info(status)
                # log an error if the task fails and change the state
                elif status == 'FAILED':
                    state = status
                    logging.error(task.status()['error_message'])
                    logging.debug(task.status())
    # if no new assets, return empty list
    else:
        assets = []
    return assets

def checkCreateCollection(collection):
    '''
    List assests in collection if it exists, else create new collection
    INPUT   collection: GEE collection to check or create (string)
    RETURN  list of assets in collection (list of strings)
    '''
    # if parent folder does not exist, create it
    if not eeUtil.exists('/'+PARENT_FOLDER):
        logging.info('{} does not exist, creating'.format(PARENT_FOLDER))
        eeUtil.createFolder('/'+PARENT_FOLDER, public=True)
    # if collection exists, return list of assets in collection
    if eeUtil.exists(collection):
        return eeUtil.ls(collection)
    # if collection does not exist, create it and return an empty list (because no assets are in the collection)
    else:
        logging.info('{} does not exist, creating'.format(collection))
        eeUtil.createFolder(collection, True, public=True)
        return []

def deleteExcessAssets(var, dates, max_assets):
    '''
    Delete oldest assets, if more than specified in max_assets variable
    INPUT   var: variable we are processing data for (string)
            dates: dates for all the assets currently in the GEE collection; dates should be in the format specified
                    in DATE_FORMAT variable (list of strings)
            max_assets: maximum number of assets allowed in the collection (int)
    '''
    # sort the list of dates so that the oldest is first
    dates.sort()
    # if we have more dates of data than allowed,
    if len(dates) > max_assets:
        # go through each date, starting with the oldest, and delete until we only have the max number of assets left
        for date in dates[:-max_assets]:
            eeUtil.removeAsset('/'+getAssetName(var, date))

def get_most_recent_date(collection):
    '''
    Get most recent date from the data in the GEE collection
    INPUT   collection: GEE collection to check dates for (string)
    RETURN  most_recent_date: most recent date in GEE collection (datetime)
    '''
    # get list of assets in collection
    existing_assets = checkCreateCollection('/'+collection)
    # get a list of strings of dates in the collection
    existing_dates = [getDate_GEE(a) for a in existing_assets]
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

def create_headers():
    '''
    Create headers to perform authorized actions on API

    '''
    return {
        'Content-Type': "application/json",
        'Authorization': "{}".format(os.getenv('apiToken')),
    }

def pull_layers_from_API(dataset_id):
    '''
    Pull dictionary of current layers from API
    INPUT   dataset_id: Resource Watch API dataset ID (string)
    RETURN  layer_dict: dictionary of layers (dictionary of strings)
    '''
    # generate url to access layer configs for this dataset in back office
    rw_api_url = 'https://api.resourcewatch.org/v1/dataset/{}/layer'.format(dataset_id)
    # request data
    r = requests.get(rw_api_url)
    # convert response into json and make dictionary of layers
    layer_dict = json.loads(r.content.decode('utf-8'))['data']
    return layer_dict

def update_layer(var, layer, end_date, start_date):
    '''
    Update layers in Resource Watch back office.
    INPUT   var: variable for which we are updating layers (string)
            layer: layer that will be updated (string)
            end_date: end date of days we will average, in the format of the DATE_FORMAT variable (datetime)
            start_date: start date of days we will average, in the format of the DATE_FORMAT variable (datetime)
    '''
    # convert end_date to string and get name of asset using the end_date
    asset = getAssetName(var, end_date.strftime(DATE_FORMAT))

    # get previous date being used from
    old_date = getDate_GEE(layer['attributes']['layerConfig']['assetId'])
    # get old start and end dates for time period that we are averaging over
    old_end_date, old_start_date = getDateRange(old_date)    
    # convert old datetimes to string
    old_end_date_text = old_end_date.strftime("%B %d, %Y")
    old_start_date_text = old_start_date.strftime("%B %d, %Y")
    # generate text for old date range
    old_date_text = old_start_date_text + ' - ' + old_end_date_text

    # convert new datetimes to string
    end_date_text = end_date.strftime("%B %d, %Y")
    start_date_text = start_date.strftime("%B %d, %Y")
    # generate text for new date range
    new_date_text = start_date_text + ' - ' + end_date_text

    # replace date in layer's title with new date range
    layer['attributes']['name'] = layer['attributes']['name'].replace(old_date_text, new_date_text)

    # replace the asset id in the layer def with new asset id
    layer['attributes']['layerConfig']['assetId'] = asset

    # replace the asset id in the interaction config with new asset id
    old_asset = getAssetName(var, old_date)
    layer['attributes']['interactionConfig']['config']['url'] = layer['attributes']['interactionConfig']['config']['url'].replace(old_asset,asset)
    layer['attributes']['interactionConfig']['pulseConfig']['url'] = layer['attributes']['interactionConfig']['pulseConfig']['url'].replace(old_asset,asset)

    # send patch to API to replace layers
    # generate url to patch layer
    rw_api_url_layer = "https://api.resourcewatch.org/v1/dataset/{dataset_id}/layer/{layer_id}".format(
        dataset_id=layer['attributes']['dataset'], layer_id=layer['id'])
    # create payload with new title and layer configuration
    payload = {
        'application': ['rw'],
        'layerConfig': layer['attributes']['layerConfig'],
        'name': layer['attributes']['name'],
        'interactionConfig': layer['attributes']['interactionConfig']
    }
    # patch API with updates
    r = requests.request('PATCH', rw_api_url_layer, data=json.dumps(payload), headers=create_headers())
    # check response
    # if we get a 200, the layers have been replaced
    # if we get a 504 (gateway timeout) - the layers are still being replaced, but it worked
    if r.ok or r.status_code==504:
        logging.info('Layer replaced: {}'.format(layer['id']))
    else:
        logging.error('Error replacing layer: {} ({})'.format(layer['id'], r.status_code))

def updateResourceWatch(new_dates):
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date', flushing the tile cache, and updating any dates on layers
    '''
    # sort new dates oldest to newest
    new_dates.sort()
    # get the most recent date (last in the list) 
    new_date = new_dates[-1]

    # Update the dates on layer legends
    logging.info('Updating Resource Watch Layers')
    for var, ds_id in DATASET_IDS.items():
        logging.info('Updating {}'.format(var))
        # pull dictionary of current layers from API
        layer_dict = pull_layers_from_API(ds_id)
        # go through each layer, pull the definition and update
        for layer in layer_dict:
            # get start and end dates for time period that we are averaging over
            end_date, start_date = getDateRange(new_date)
            # replace layer asset and title date with new
            update_layer(var, layer, end_date, start_date)

    for var_num in range(len(VARS)):
        # get variable we are updating layers for
        var = VARS[var_num]
        # Get most recent date in GEE
        most_recent_date = get_most_recent_date(getCollectionName(var))
        # Get the current 'last update date' from the dataset on Resource Watch
        current_date = getLastUpdate(DATASET_IDS[var])
        # If the most recent date from the GEE collection does not match the 'last update date' on the RW API, update it
        if current_date != most_recent_date:
            logging.info('Updating last update date and flushing cache.')
            # Update dataset's last update date on Resource Watch
            lastUpdateDate(DATASET_IDS[var], most_recent_date)
            # get layer ids and flush tile cache for each
            layer_ids = getLayerIDs(DATASET_IDS[var])
            for layer_id in layer_ids:
                flushTileCache(layer_id)

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Initialize eeUtil and ee modules
    eeUtil.initJson()
    initialize_ee()

    # Clear collection in GEE if desired
    if CLEAR_COLLECTION_FIRST:
        clearCollectionMultiVar()

    # Process data, one variable at a time
    for i in range(len(VARS)):
        # get variable name
        var = VARS[i]
        logging.info('STARTING {var}'.format(var=var))

        # Check if collection exists, create it if it does not
        # If it exists return the list of assets currently in the collection
        existing_assets = checkCreateCollection('/'+getCollectionName(var)) #make image collection if doesn't have one
        existing_dates = [getDate_GEE(a) for a in existing_assets]

        # Fetch, process, and upload the new data
        new_assets = processNewData(var, existing_dates)
        # Get the dates of the new data we have added
        new_dates = [getDate_GEE(a) for a in new_assets]

        logging.info('Previous assets: {}, new: {}, max: {}'.format(
            len(existing_dates), len(new_dates), MAX_ASSETS))

        # Delete excess assets
        deleteExcessAssets(var, existing_dates+new_dates, MAX_ASSETS)
        logging.info('SUCCESS for {var}'.format(var=var))

    # Update Resource Watch
    updateResourceWatch(new_dates)

    logging.info('SUCCESS')
