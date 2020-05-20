from __future__ import unicode_literals

import os
import sys
import urllib.request
import shutil
from contextlib import closing
import datetime
from dateutil.relativedelta import relativedelta
import logging
import subprocess
import eeUtil
import requests
import time
import LMIPy as lmi

# do you want to delete everything currently in the GEE collection when you run this script?
CLEAR_COLLECTION_FIRST = False

# url for sea ice extent data
# example file url name: ftp://sidads.colorado.edu/DATASETS/NOAA/G02135/north/monthly/geotiff/02_Feb/N_201902_extent_v3.0.tif
SOURCE_URL = 'ftp://sidads.colorado.edu/DATASETS/NOAA/G02135/{north_or_south}/monthly/geotiff/{month}/{target_file}'
# unformatted file name for source data
SOURCE_FILENAME = '{N_or_S}_{date}_extent_v3.0.tif'

# name of data directory in Docker container
DATA_DIR = 'data'

# name of folder to store data in Google Cloud Storage
GS_FOLDER = 'cli_005_polar_sea_ice_extent'

# name of collection in GEE where we will upload the final data for current sea ice extent
EE_COLLECTION = 'cli_005_{arctic_or_antarctic}_sea_ice_extent_{orig_or_reproj}'
# name of collection in GEE where we will upload the final data for historical min/max sea ice
EE_COLLECTION_BY_MONTH = '/projects/resource-watch-gee/cli_005_historical_sea_ice_extent/cli_005_{arctic_or_antarctic}_sea_ice_extent_{orig_or_reproj}_month{month}_hist'

# filename format for GEE
FILENAME = 'cli_005_{arctic_or_antarctic}_sea_ice_{date}'

# Which months do we want to keep historical annual records for? (by month number, ex: 3=March)
# we will keep the months in which sea ice reaches a max or min in either the norther or southern hemisphere
HISTORICAL_MONTHS = [2,3,9]

# how many assets can be stored in each GEE collection before the oldest ones are deleted?
MAX_ASSETS = 12

# format of date (used in both the source data files and GEE)
DATE_FORMAT = '%Y%m'

# Resource Watch dataset API ID for current sea ice extent
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = {
    'cli_005_antarctic_sea_ice_extent_reproj':'e740efec-c673-431a-be2c-b214613f641a',
    'cli_005_arctic_sea_ice_extent_reproj': '484fbba1-ac34-402f-8623-7b1cc9c34f17',
}

# Resource Watch dataset API ID for historical sea ice maximums and minimums
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
HIST_DATASET_ID = {
    # min antarctic
    '/projects/resource-watch-gee/cli_005_historical_sea_ice_extent/cli_005_antarctic_sea_ice_extent_reproj_month02_hist':
        '05fd2614-325b-460a-8b52-3155fa9dd98f',
    # max antarctic
    '/projects/resource-watch-gee/cli_005_historical_sea_ice_extent/cli_005_antarctic_sea_ice_extent_reproj_month09_hist':
        '7667bdd8-9adb-44de-b51c-d2d26e461af1',
    # min arctic
    '/projects/resource-watch-gee/cli_005_historical_sea_ice_extent/cli_005_arctic_sea_ice_extent_reproj_month09_hist':
        'a99c5cf5-f141-4bed-a36d-b04c8e171dfa',
    # max arctic
    '/projects/resource-watch-gee/cli_005_historical_sea_ice_extent/cli_005_arctic_sea_ice_extent_reproj_month03_hist':
        '15a0b176-8313-4859-af90-5c198e50a605'
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
    # specify the maximum number of attempt we will make
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

def getAssetName(tif, orig_or_reproj, new_or_hist, arctic_or_antarctic=''):
    '''
    get asset name
    INPUT   tif: name of tif file downloaded from source (string)
            orig_or_reproj: is this asset name for the original or reprojected data? (string)
            new_or_hist: is this asset name for historical max/min data or current extent data? (string)
            arctic_or_antarctic: optional, is this asset name for the arctic or antarctic data? (string)
    RETURN  GEE asset name for input date (string)
    '''
    # if the arctic_or_antarctic parameter is specified, use it for the asset name
    if len(arctic_or_antarctic):
        location = arctic_or_antarctic
    # otherwise, pull the location from the original tif file name
    else:
        if orig_or_reproj=='orig':
            location = tif.split('_')[2]
        else:
            location = tif.split('_')[4]

    # pull the date from the tif file name
    date = getDate(tif)

    # create an asset name, based on if this asset will be used in the historical sea ice max/min data or current extent data
    if new_or_hist=='new':
        asset = os.path.join(EE_COLLECTION.format(arctic_or_antarctic=location, orig_or_reproj=orig_or_reproj),
                        FILENAME.format(arctic_or_antarctic=location, date=date))
    elif new_or_hist=='hist':
        month = date[-2:]
        asset = os.path.join(EE_COLLECTION_BY_MONTH.format(arctic_or_antarctic=location, orig_or_reproj=orig_or_reproj, month=month),
                        FILENAME.format(arctic_or_antarctic=location, date=date))
    return asset

def getFilename(arctic_or_antarctic, date):
    '''
    get tif filename to save source file as
    INPUT   arctic_or_antarctic: is this file name for the arctic or antarctic data? (string)
            date: date in the format of the DATE_FORMAT variable (string)
    RETURN  file name to save tif from source under (string)
    '''
    return '{}.tif'.format(FILENAME.format(arctic_or_antarctic=arctic_or_antarctic,date=date))

def getDate(filename):
    '''
    get date from filename (last 6 characters of filename after removing extension)
    INPUT   filename: file name that ends in a date of the format YYYYDD (string)
    RETURN  date in the format YYYYDD (string)
    '''
    return os.path.splitext(os.path.basename(filename))[0][-6:]

def getNewTargetDates(exclude_dates):
    '''
    Get new dates we want to try to fetch data for
    INPUT   exclude_dates: list of dates that we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
    RETURN  new_dates: list of new dates we want to try to get, in the format of the DATE_FORMAT variable (list of strings)
    '''
    # create empty list to store dates we want to fetch
    new_dates = []
    # start with the fifteenth of the current month
    date = datetime.date.today()
    date = date.replace(day=15)
    for i in range(MAX_ASSETS):
        # go back one month at a time
        date = date - relativedelta(months=1)
        # generate a string from the date
        datestr = date.strftime(DATE_FORMAT)
        # if the date string is not the list of dates we already have, add it to the list of new dates to try and fetch
        if datestr not in exclude_dates:
            new_dates.append(datestr)
    return new_dates

def getHistoricalTargetDates(exclude_dates, month):
    '''
    Get new dates we want to try to fetch data for
    INPUT   exclude_dates: list of dates that we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
            month:  month we are processing historical data for (integer)
    RETURN  new_dates: list of new dates we want to try to get, in the format of the DATE_FORMAT variable (list of strings)
    '''
    # create empty list to store dates we want to fetch
    new_dates = []
    # start with the fifteenth of the current month
    date = datetime.date.today()
    date = date.replace(day=15)
    date = date - relativedelta(months=1)  # subtract 1 month from data

    # go back each year through the earliest year of data (1979)
    for i in range(date.year-1979):
        # if the month we are checking for data in has not happened yet this year,
        # start with last year's data
        if month>date.month:
            date -= relativedelta(years=1)
        # go to the 15th of the month we want to check data for
        date = date.replace(day=15).replace(month=month)
        # generate a string from the date
        datestr = date.strftime(DATE_FORMAT)
        # if the date string is not the list of dates we already have, add it to the list of new dates to try and fetch
        if datestr not in exclude_dates:
            new_dates.append(datestr)
        # go back one year
        date -= relativedelta(years=1)
    return new_dates

def format_month(datestring):
    '''
    get the name of the month for the date we are processing data for, in the format used in the source filename
    INPUT   datestring: date we want to fetch in the format of the DATE_FORMAT variable (string)
    RETURN  month naming convention used in the source data file for current date (string)
    '''
    # pull the month from the input date string
    month = datestring[-2:]
    # find the corresponding month name to use in the source file name
    names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    name = names[int(month)-1]
    return('_'.join([month, name]))

def fetch(url, arctic_or_antarctic, datestring):
    '''
    Fetch files by datestamp and region
    INPUT   url: url where we can find data file to download (string)
            arctic_or_antarctic: is the file we are fetching for the arctic or antarctic data? (string)
            datestring: date we want to fetch in the format of the DATE_FORMAT variable (string)
    RETURN  filename: file name for tif that has been downloaded (strings)
    '''
    # New data may not yet be posted
    month = format_month(datestring)
    north_or_south = 'north' if (arctic_or_antarctic=='arctic') else 'south'

    target_file = SOURCE_FILENAME.format(N_or_S=north_or_south[0].upper(), date=datestring)
    _file = url.format(north_or_south=north_or_south,month=month,target_file=target_file)
    filename = getFilename(arctic_or_antarctic=arctic_or_antarctic, date=datestring)
    try:
        with closing(urllib.request.urlopen(_file)) as r:
            with open(os.path.join(DATA_DIR, filename), 'wb') as f:
                shutil.copyfileobj(r, f)
                logging.debug('Copied: {}'.format(_file))
    except Exception as e:
        logging.warning('Could not fetch {}'.format(_file))
        logging.error(e)
    return filename

def reproject(filename, s_srs='EPSG:4326', extent='-180 -89.75 180 89.75'):
    '''
    reproject tif file from source
    INPUT   filename: tif file downloaded from source (string)
            s_srs: spatial reference of source data file (string)
            extent: extent of output file to be created (string)
    RETURN  new_filename: name of reprojected tif file (string)
    '''
    # create a filename to save the reprojected tif file under
    tmp_filename = ''.join(['reprojected_',filename])
    # reproject the data
    cmd = ' '.join(['gdalwarp','-overwrite','-s_srs',s_srs,'-t_srs','EPSG:4326',
                    '-te',extent,'-multi','-wo','NUM_THREADS=val/ALL_CPUS',
                    os.path.join(DATA_DIR, filename),
                    os.path.join(DATA_DIR, tmp_filename)])
    subprocess.check_output(cmd, shell=True)

    # create a filename to save compressed data under
    new_filename = ''.join(['compressed_reprojected_',filename])
    # compress the data
    cmd = ' '.join(['gdal_translate','-co','COMPRESS=LZW','-stats',
                    os.path.join(DATA_DIR, tmp_filename),
                    os.path.join(DATA_DIR, new_filename)])
    subprocess.check_output(cmd, shell=True)

    # remove the intermediary files that we don't need anymore
    os.remove(os.path.join(DATA_DIR, tmp_filename))
    os.remove(os.path.join(DATA_DIR, tmp_filename+'.aux.xml'))

    logging.debug('Reprojected {} to {}'.format(filename, new_filename))
    return new_filename

def processNewData(existing_dates, arctic_or_antarctic, new_or_hist, month=None):
    '''
    fetch, process, upload, and clean new data
    INPUT   existing_dates: list of dates we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
            arctic_or_antarctic: are we processing the arctic or antarctic data? (string)
            new_or_hist: are we processing historical max/min sea data or current extent data? (string)
            month: optional (use if processing historical data), month we are processing data for (integer)
    RETURN  orig_assets: list of original assets that have been uploaded to GEE (list of strings)
            reproj_assets: list of reprojected assets that have been uploaded to GEE (list of strings)
    '''
    # Get list of new dates we want to try to fetch data for
    if new_or_hist=='new':
        target_dates = getNewTargetDates(existing_dates)
    elif new_or_hist=='hist':
        target_dates = getHistoricalTargetDates(existing_dates, month=month)

    # define spatial reference and geographic extent of source data files
    if arctic_or_antarctic == 'arctic':
        s_srs = 'EPSG:3411'
        extent = '-180 50 180 89.75'
    else:
        s_srs = 'EPSG:3412'
        extent = '-180 -89.75 180 -50'

    # create empty lists to store original and reprojected tifs to upload to GEE
    orig_tifs = []
    reproj_tifs = []

    # Fetch new files
    logging.info('Fetching files')
    for date in target_dates:
        # fetch files
        orig_file = fetch(SOURCE_URL, arctic_or_antarctic, date)
        orig_tifs.append(os.path.join(DATA_DIR, orig_file))
        # reproject files
        reproj_file = reproject(orig_file, s_srs=s_srs, extent=extent)
        reproj_tifs.append(os.path.join(DATA_DIR, reproj_file))

    # 3. Upload new files
    logging.info('Uploading {} files'.format(arctic_or_antarctic))

    # Get a list of the names we want to use for the assets once we upload the files to GEE
    orig_assets = [getAssetName(tif, 'orig', new_or_hist) for tif in orig_tifs]
    reproj_assets = [getAssetName(tif, 'reproj', new_or_hist) for tif in reproj_tifs]

    # Get a list of the dates we have to upload from the tif file names
    dates = [getDate(tif) for tif in reproj_tifs]
    # Get a list of datetimes from these dates for each of the dates we are uploading
    datestamps = [datetime.datetime.strptime(date, DATE_FORMAT)
                  for date in dates]  # returns list of datetime object
    # Upload new files (tifs) to GEE
    eeUtil.uploadAssets(orig_tifs, orig_assets, GS_FOLDER, datestamps, timeout=3000)
    eeUtil.uploadAssets(reproj_tifs, reproj_assets, GS_FOLDER, datestamps, timeout=3000)

    # Delete local files
    logging.info('Cleaning local files')
    for tif in orig_tifs:
        logging.debug('Deleting: {}'.format(tif))
        os.remove(tif)
    for tif in reproj_tifs:
        logging.debug('Deleting: {}'.format(tif))
        os.remove(tif)

    return orig_assets, reproj_assets

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

def deleteExcessAssets(dates, orig_or_reproj, arctic_or_antarctic, max_assets, new_or_hist):
    '''
    Delete oldest assets, if more than specified in max_assets variable
    INPUT   dates: dates for all the assets currently in the GEE collection; dates should be in the format specified
                    in DATE_FORMAT variable (list of strings)
            orig_or_reproj: is this asset name for the original or reprojected data? (string)
            arctic_or_antarctic: is this asset name for the arctic or antarctic data? (string)
            max_assets: maximum number of assets allowed in the collection (int)
            new_or_hist: is this asset name for historical max/min data or current extent data? (string)
    '''
    # sort the list of dates so that the oldest is first
    dates.sort()
    # if we have more dates of data than allowed,
    if len(dates) > max_assets:
        # go through each date, starting with the oldest, and delete until we only have the max number of assets left
        for date in dates[:-max_assets]:
            eeUtil.removeAsset(getAssetName(date, orig_or_reproj, new_or_hist, arctic_or_antarctic=arctic_or_antarctic))

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

def create_layers_new_years(dataset_id, new_year):
    '''
    This function will create a new layers for a dataset for the year indicated
    INPUT   dataset_id: Resource Watch API dataset ID (string)
            new_year: year that you want to create a new layer for (integer)
    '''
    # pull in the dataset we are making a new layer for
    dataset = lmi.Dataset(dataset_id)
    # pull the first layer to used as a template when creating the new layer
    layer_to_clone = dataset.layers[0]

    # Gather the layer attributes that need to change
    name = layer_to_clone.attributes['name']
    description = layer_to_clone.attributes['description']
    appConfig = layer_to_clone.attributes['layerConfig']
    assetId = appConfig['assetId']
    order = str(appConfig['order'])
    timeLineLabel = appConfig['timelineLabel']

    # Find they year in the example layer - we will replace this with the year we are making a new layer for
    replace_string = name[:4]

    # Generate layer attributes for the new year's layer
    new_layer_name = name.replace(replace_string, str(new_year))
    new_description = description.replace(replace_string, str(new_year))
    new_assetId = assetId.replace(replace_string, str(new_year))
    new_timeline_label = timeLineLabel.replace(replace_string, str(new_year))
    new_order = int(order.replace(replace_string, str(new_year)))

    # Clone layer
    clone_attributes = {
        'name': new_layer_name,
        'description': new_description
    }
    new_layer = layer_to_clone.clone(token=os.getenv('apiToken')[7:], env='production', layer_params=clone_attributes,
                                     target_dataset_id=dataset_id)

    # Replace layer attributes with new values
    appConfig = new_layer.attributes['layerConfig']
    appConfig['assetId'] = new_assetId
    appConfig['order'] = new_order
    appConfig['timelineLabel'] = new_timeline_label
    payload = {
        'layerConfig': {
            **appConfig
        }
    }
    new_layer = new_layer.update(update_params=payload, token=os.getenv('apiToken')[7:])

def updateResourceWatch():
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date', flushing the tile cache, and updating any dates on layers
    '''
    # update datasets on current ice extents
    for dataset, id in DATASET_ID.items():
        # Get the most recent date from the data in the GEE collection
        most_recent_date = get_most_recent_date(dataset)
        # Get the current 'last update date' from the dataset on Resource Watch
        current_date = getLastUpdate(id)
        # If the most recent date from the GEE collection does not match the 'last update date' on the RW API, update it
        if current_date != most_recent_date:
            logging.info('Updating last update date and flushing cache.')
            # Update dataset's last update date on Resource Watch
            lastUpdateDate(id, most_recent_date)
            # get layer ids and flush tile cache for each
            layer_ids = getLayerIDs(id)
            for layer_id in layer_ids:
                flushTileCache(layer_id)
        # Update the dates on layer legends - TO BE ADDED IN FUTURE

    # update datasets on historical ice maximums and minimums
    for dataset, id in HIST_DATASET_ID.items():
        # Get the most recent date from the data in the GEE collection
        most_recent_date = get_most_recent_date(dataset)
        # Get the current 'last update date' from the dataset on Resource Watch
        current_date = getLastUpdate(id)
        # If the most recent date from the GEE collection does not match the 'last update date' on the RW API, update it
        if current_date != most_recent_date:
            logging.info('Updating last update date and flushing cache.')
            # Update dataset's last update date on Resource Watch
            lastUpdateDate(id, most_recent_date)
            # Add new layers if new years of data are available
            create_layers_new_years(id, most_recent_date.year)

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Initialize eeUtil
    eeUtil.initJson()

    '''
    Process current sea ice extent data
    '''
    # Create collection names
    arctic_collection_orig = EE_COLLECTION.format(arctic_or_antarctic='arctic', orig_or_reproj='orig')
    arctic_collection_reproj = EE_COLLECTION.format(arctic_or_antarctic='arctic', orig_or_reproj='reproj')
    antarctic_collection_orig = EE_COLLECTION.format(arctic_or_antarctic='antarctic', orig_or_reproj='orig')
    antarctic_collection_reproj = EE_COLLECTION.format(arctic_or_antarctic='antarctic', orig_or_reproj='reproj')

    # Clear the GEE collections, if specified above
    if CLEAR_COLLECTION_FIRST:
        # Put collection names into a list to loop through for processing
        collections = [arctic_collection_orig, arctic_collection_reproj,
                       antarctic_collection_orig, antarctic_collection_reproj]
        for collection in collections:
            if eeUtil.exists(collection):
                eeUtil.removeAsset(collection, recursive=True)

    # Check if arctic collections exist, create them if they do not
    # If they exist return the list of assets currently in the collections
    arctic_assets_orig = checkCreateCollection(arctic_collection_orig)
    arctic_assets_reproj = checkCreateCollection(arctic_collection_reproj)
    # Get a list of the dates of data we already have in each collection
    arctic_dates_orig = [getDate(a) for a in arctic_assets_orig]
    arctic_dates_reproj = [getDate(a) for a in arctic_assets_reproj]

    # Fetch, process, and upload the new arctic data
    new_arctic_assets_orig, new_arctic_assets_reproj = processNewData(arctic_dates_reproj, 'arctic', new_or_hist='new')
    # Get the dates of the new data we have added to each collection
    new_arctic_dates_orig = [getDate(a) for a in new_arctic_assets_orig]
    new_arctic_dates_reproj = [getDate(a) for a in new_arctic_assets_reproj]

    logging.info('Previous Arctic assets: {}, new: {}, max: {}'.format(
        len(arctic_dates_reproj), len(new_arctic_dates_reproj), MAX_ASSETS))

    # Check if antarctic collections exists, create them if they do not
    # If they exist return the list of assets currently in the collections
    antarctic_assets_orig = checkCreateCollection(antarctic_collection_orig)
    antarctic_assets_reproj = checkCreateCollection(antarctic_collection_reproj)
    # Get a list of the dates of data we already have in each collection
    antarctic_dates_orig = [getDate(a) for a in antarctic_assets_orig]
    antarctic_dates_reproj = [getDate(a) for a in antarctic_assets_reproj]

    # Fetch, process, and upload the new antarctic data
    new_antarctic_assets_orig, new_antarctic_assets_reproj  = processNewData(antarctic_dates_reproj, 'antarctic', new_or_hist='new')
    # Get the dates of the new data we have added to each collection
    new_antarctic_dates_orig = [getDate(a) for a in new_antarctic_assets_orig]
    new_antarctic_dates_reproj = [getDate(a) for a in new_antarctic_assets_reproj]

    logging.info('Previous Antarctic assets: {}, new: {}, max: {}'.format(
        len(antarctic_dates_reproj), len(new_antarctic_dates_reproj), MAX_ASSETS))

    # Create a list of each collection of old asset dates
    e_dates = [arctic_dates_orig, arctic_dates_reproj,
                     antarctic_dates_orig, antarctic_dates_reproj]
    # Create a list each collection of new asset dates
    n_dates = [new_arctic_dates_orig, new_arctic_dates_reproj,
                new_antarctic_dates_orig, new_antarctic_dates_reproj]

    # Loop through each processed data collection and delete the excess assets
    for i in range(4):
        # determine if we are deleting original data or reprojected data
        orig_or_reproj = 'orig' if i%2==0 else 'reproj'
        # determine if we are deleting arctic data or antarctic data
        arctic_or_antarctic = 'arctic' if i < 2 else 'antarctic'
        # get a list of the existing dates for this collection
        e = e_dates[i]
        # get a list of the new dates for this collection
        n = n_dates[i]
        # get a list of all the dates now in the collection
        total = e + n
        # delete any excess assets
        deleteExcessAssets(total,orig_or_reproj,arctic_or_antarctic,MAX_ASSETS,'new')

    '''
    Process historical sea ice max/min data
    '''
    for month in HISTORICAL_MONTHS:
        logging.info('Processing historical data for month {}'.format(month))

        # Create collection names
        arctic_collection_orig = EE_COLLECTION_BY_MONTH.format(arctic_or_antarctic='arctic', orig_or_reproj='orig', month="{:02d}".format(month))
        arctic_collection_reproj = EE_COLLECTION_BY_MONTH.format(arctic_or_antarctic='arctic', orig_or_reproj='reproj', month="{:02d}".format(month))
        antarctic_collection_orig = EE_COLLECTION_BY_MONTH.format(arctic_or_antarctic='antarctic', orig_or_reproj='orig', month="{:02d}".format(month))
        antarctic_collection_reproj = EE_COLLECTION_BY_MONTH.format(arctic_or_antarctic='antarctic', orig_or_reproj='reproj', month="{:02d}".format(month))

        # Check if arctic collections exist, create them if they do not
        # If they exist return the list of assets currently in the collections
        arctic_assets_orig = checkCreateCollection(arctic_collection_orig)
        arctic_assets_reproj = checkCreateCollection(arctic_collection_reproj)
        # Get a list of the dates of data we already have in each collection
        arctic_dates_orig = [getDate(a) for a in arctic_assets_orig]
        arctic_dates_reproj = [getDate(a) for a in arctic_assets_reproj]

        # Fetch, process, and upload the new arctic data
        new_arctic_assets_orig, new_arctic_assets_reproj = processNewData(arctic_dates_reproj, 'arctic',
                                                                          new_or_hist=='hist', month=month)
        # Get the dates of the new data we have added to each collection
        new_arctic_dates_orig = [getDate(a) for a in new_arctic_assets_orig]
        new_arctic_dates_reproj = [getDate(a) for a in new_arctic_assets_reproj]

        logging.info('Previous historical Arctic assets: {}, new: {}, max: {}'.format(
            len(arctic_dates_reproj), len(new_arctic_dates_reproj), MAX_ASSETS))

        # Check if antarctic collections exists, create them if they do not
        # If they exist return the list of assets currently in the collections
        antarctic_assets_orig = checkCreateCollection(antarctic_collection_orig)
        antarctic_assets_reproj = checkCreateCollection(antarctic_collection_reproj)
        # Get a list of the dates of data we already have in each collection
        antarctic_dates_orig = [getDate(a) for a in antarctic_assets_orig]
        antarctic_dates_reproj = [getDate(a) for a in antarctic_assets_reproj]

        # Fetch, process, and upload the new antarctic data
        new_antarctic_assets_orig, new_antarctic_assets_reproj = processNewData(antarctic_dates_reproj, 'antarctic',
                                                                                new_or_hist=='hist', month=month)
        # Get the dates of the new data we have added to each collection
        new_antarctic_dates_orig = [getDate(a) for a in new_antarctic_assets_orig]
        new_antarctic_dates_reproj = [getDate(a) for a in new_antarctic_assets_reproj]

        logging.info('Previous historical Antarctic assets: {}, new: {}, max: {}'.format(
            len(antarctic_dates_reproj), len(new_antarctic_dates_reproj), MAX_ASSETS))

    # Update Resource Watch
    updateResourceWatch()

    logging.info('SUCCESS')
