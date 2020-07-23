from __future__ import unicode_literals

import os
import sys
import datetime
import logging
import subprocess
import eeUtil
import requests
import time
import urllib
import urllib.request

# url for NOAA wave height data
SOURCE_URL = 'ftp://ftpprd.ncep.noaa.gov/pub/data/nccf/com/wave/prod/multi_1.{date}/'

# filename format for GEE
FILENAME = 'ocn_002_wave_height_{time}_{date}'

# name of data directory in Docker container
DATA_DIR = os.path.join(os.getcwd(),'data')

# name of folder to store data in Google Cloud Storage
GS_FOLDER = 'ocn_002_wave_height'

# name of collection in GEE where we will upload the final data
EE_COLLECTION = 'ocn_002_wave_height'

# do you want to delete everything currently in the GEE collection when you run this script?
CLEAR_COLLECTION_FIRST = False

# how many assets can be stored in the GEE collection before the oldest ones are deleted?
# keeping 4 cycle of data; (current, 12th, 24th, 48th forecast)*4 = 16
MAX_ASSETS = 16

# format of date used in both source and GEE
DATE_FORMAT = '%Y%m%d'

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = 'ebdab869-5869-4965-a5b1-d1f9b0f83330'

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

def getUrl(date):
     '''
     format source url with date
     INPUT   date: date in the format YYYYMMDD (string)
     RETURN  source url to download data, formatted for the input date (string)
     '''
     return SOURCE_URL.format(date=date)

def getsubFilename(grib):
     '''
     generate grib filename to save individual source file as 
     INPUT   grib: file name for the grib file (string)
     RETURN  file name to save individual grib from source under (string)
     '''
     return os.path.join(DATA_DIR, grib)

def getAssetName(time_step, date):
     '''
     get asset name
     INPUT   time_step: time step for which we downloaded data (string)
             date: date in the format of the DATE_FORMAT variable (string)
     RETURN  GEE asset name for input date (string)
     '''
     return os.path.join(EE_COLLECTION, FILENAME.format(time=time_step,date=date))

def getDate(filename):
     '''
     get date from asset name (last 8 characters of filename after removing extension)
     INPUT   filename: file name that ends in a date of the format YYYYMMDD (string)
     RETURN  existing_dates: dates in the format YYYYMMDD (string)
             existing_timesteps: time steps in the format t**z (string)
     '''
     existing_dates = os.path.splitext(os.path.basename(filename))[0][-8:]
     existing_timesteps = os.path.splitext(os.path.basename(filename))[0][-13:-9]

     return existing_dates, existing_timesteps

def find_latest_date():
    '''
    Fetch the latest date for which ocean wave height data is available
    RETURN  latest_available_date: latest date available for download from source website (string)
    '''   
    # get rid of date from SOURCE_URL to get the parent directory where 
    # inidividual folder for each year is present
    url = SOURCE_URL.split('multi_1.')[0]
    try:
      # open the url
      response = urllib.request.urlopen(url)
      # read the opened url
      content = response.read()
      # use string manipulation to get all the available links in source url
      links = [url + line.split()[-1] for line in content.decode().splitlines()]
      # get all the folders that contain the global wave height data
      folders = [link for link in links if 'multi_' in link]
      # split the folder names on '.' and get the last elements after each split to retrieve dates
      split_dates = ([s.split('.')[-1] for s in folders])
      # get rid of unnecessary '/' from every dates 
      available_dates = ([s.strip('/') for s in split_dates])
      # convert available dates to datetime according to format set by DATE_FORMAT
      formt_available_dates = [datetime.datetime.strptime(date, DATE_FORMAT) for date in available_dates]
      # sort these dates oldest to newest
      formt_available_dates.sort()
      # get the most recent date (last in the list) and convert it to a string
      latest_available_date = datetime.datetime.strftime(formt_available_dates[-1], DATE_FORMAT)

      return latest_available_date

    except Exception as e:
      # if unsuccessful, log that no data were found from the source url
      logging.debug('No data found from url {})'.format(url))
      logging.debug(e)

def find_latest_time(date):
    '''
    Fetch the latest time step for which ocean wave height data is available
    INPUT   date: date we want to try to fetch, in the format YYYYMMDD (string)
    RETURN  time_step: latest timestep available for download from source website (string)
            latest_grib: latest grib file from source (string)
    '''   
     # get the url where data for the given date is stored at the source
    url = getUrl(date)
    try:
         # open the url
         response = urllib.request.urlopen(url)
         # read the opened url
         content = response.read()
         # use string manipulation to get all the links
         links = [url + line.split()[-1] for line in content.decode().splitlines()]
         # create an empty list to store the name of all grib files available for the input date
         gribs = []
         # loop through each available link
         for link in links:
             # get only the grib files with global data
             if 'multi_1.glo_30m.t' in link and link.endswith('.grib2'):
                 # get the name of the grib file
                 grib = link.split('/')[-1]
                 # add the grib file name to the list of files to download
                 gribs.append(grib)
         # The model is run four times a day: 00Z, 06Z, 12Z, and 18Z
         # we put these time into a list with latest time as the first entry
         # this is ncessary to make sure we start by searching for latest data
         time_steps = ['t18z','t12z','t06z','t00z']
         # loop through each time step starting with latest time step
         for time_step in time_steps:
             # search data for input time step
             latest_grib = [step for step in gribs if time_step in step]
             # we only want the data from latest run. once we find data for a timestep,
             # break out of the loop
             if latest_grib:
                 logging.info('Latest available time step {}'.format(time_step))
                 break
    except Exception as e:
         # if unsuccessful, log that no data were found for the input date
         logging.debug('No data found for date {})'.format(date))
         logging.debug(e)
    
    return time_step, latest_grib

def fetch(date, latest_grib):
     '''
     Fetch latest grib files by datestamp
     INPUT   date: date we want to try to fetch, in the format YYYYMMDD (string)
             latest_grib: latest grib files that we want to fetch (list of strings)
     RETURN  files: list of file names for gribs that have been downloaded (list of strings)
     '''
     # make an empty list to store names of the files we downloaded
     files = []

     # get the url where data for the given date is stored at the source
     url = getUrl(date)

     # the model forecasts from 000 to 180 hours, 
     # we only want current, 12th, 24th, 48th hour forecast
     req_frcsts = ['f000', 'f012', 'f024', 'f048']

     # create an empty list to store available current & forecast data
     required_files = []
     
     # loop through each forecast step we care about
     for req_frcst in req_frcsts:
         # append the available files to the final list
         required_files += [frcst for frcst in latest_grib if req_frcst in frcst]
    
     # loop through each grib file from the final list
     for file in required_files:
         # join the source url with the id of each grib file to generate complete 
         # URLs for each grib file download
         sub_url = os.path.join(url, file)
         # get the filename we want to save the individual file under locally
         sub_filename = getsubFilename(file)
         try:
              # try to download the data
              urllib.request.urlretrieve(sub_url, sub_filename)
              # if successful, add the file to the list of files we have downloaded
              files.append(sub_filename)
              # if successful, log that the file was downloaded successfully
              logging.info('Successfully retrieved {}'.format(sub_filename))
         except Exception as e:
              # if unsuccessful, log an error that the file was not downloaded
              logging.error('Unable to retrieve data from {}'.format(sub_url))
              logging.debug(e)

     return files

def convert(file):
     '''
     Convert grib file to tif
     INPUT   file: file name for grib that have already been downloaded (string)
     RETURN  final_tif: file name for tif that have been generated (string)
     '''
     
     '''
     Google Earth Engine needs to get tif files with longitudes of -180 to 180.
     These files have longitudes from 0 to 360. I checked this using gdalinfo.
     I downloaded a file onto my local computer and in command line, ran:
            !gdalinfo grib_file_name
     I looked at the 'Corner Coordinates' that were printed out.

     Since the longitude is in the wrong format, we will have to fix it. First,
     we will convert the files from grib to tifs using gdal_translate,
     then we will fix the longitude values using gdalwarp.
     '''
     # generate name for tif file that we are going to create from grib
     temp_tif = file.split('.grib2')[0] + '_temp.tif'
     # translate the grib file into a tif
     cmd = ['gdal_translate','-b', '5', '-a_srs', 'EPSG:4326', file, temp_tif] 
     subprocess.call(cmd) 
     # Now we will fix the longitude. To do this we need the x and y resolution.
     # I also got x and y res for data set using the gdalinfo command described above.
     xres='0.500000000000000'
     yres= '-0.500000000000000'
     # generate name for the final corrected tif
     final_tif = file.split('.grib2')[0] + '.tif'
     # fix bounds
     cmd_warp = ['gdalwarp', '-t_srs', 'EPSG:4326', '-tr', xres, yres, temp_tif, final_tif, '-wo', 
                'SOURCE_EXTRA=1000', '--config', 'CENTER_LONG', '0']
     # using the gdal from command line from inside python
     subprocess.call(cmd_warp) 

     return final_tif

def processNewData(existing_dates_steps):
    '''
    fetch, process, upload, and clean new data
    INPUT   existing_dates_steps: list of dates and timesteps we already have in GEE (list of tuple of strings)
    RETURN  latest_date_step: list of tuple of dates and timesteps for which we have downloaded data (list of tuple of strings)
            asset: file name for asset that have been uploaded to GEE (string)
    '''

    # Get latest available date that is availble on the source
    available_date = find_latest_date()
    logging.debug('Latest available date: {}'.format(available_date))
    # Get latest available time step and grib file that is availble on the source for latest date
    available_time_step, latest_grib = find_latest_time(available_date)
    # create a tuple of lateste date and time step
    latest_date_step = (available_date, available_time_step)

    # if we don't have this date and time step already in GEE
    if latest_date_step not in existing_dates_steps:
        # fetch grib files for new data
        logging.info('Fetching files')        
        files = fetch(available_date, latest_grib)
        # create an empty list to store tifs that will be generated from fetched grib file
        tifs = []
        # loop through each grib file and convert them to tif format
        for _file in files:
            logging.info('Converting file: {}'.format(_file))
            # convert gribs to tifs and store the tif filenames to a list
            tifs.append(convert(_file))

        logging.info('Merging all forecast data to a single tif as separate bands')
        # generate a name to save the tif file that will be produced by merging all forecast data   
        merged_tif = 'merged_' + available_time_step + '_' + available_date + '.tif'
        # merge all forecast data from grib into a single tif by adding each forecast as 
        # separate bands
        merge_cmd = ['gdal_merge.py', '-a_nodata', '9999', '-seperate'] + tifs + ['-o', merged_tif]
        subprocess.call(merge_cmd)

        logging.info('Uploading files')
        # Generate a name we want to use for the asset once we upload the file to GEE
        asset = [getAssetName(available_time_step, available_date)]
        # Get a datetime from the date we are uploading
        datestamp = [datetime.datetime.strptime(available_date, DATE_FORMAT)]
        # Upload new file (tif) to GEE
        eeUtil.uploadAssets([merged_tif], asset, GS_FOLDER, dates=datestamp, timeout=3000)

        return [latest_date_step], asset
    else:
        logging.info('Data already up to date')
        #if no new assets, return empty list
        return [],()

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
        eeUtil.createFolder(collection, imageCollection=True, public=True)
        return []

def deleteExcessAssets(dates, max_assets):
    '''
    Delete oldest assets, if more than specified in max_assets variable
    INPUT   dates: dates for all the assets currently in the GEE collection; 
               dates should be in the format specified in DATE_FORMAT variable (list of strings)
            max_assets: maximum number of assets allowed in the collection (int)
    '''
    # sort the list of dates so that the oldest is first
    dates.sort()
    # if we have more dates of data than allowed,
    if len(dates) > max_assets:
        # go through each date, starting with the oldest, and 
        # delete until we only have the max number of assets left
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
    existing_dates = [getDate(a)[0] for a in existing_assets]
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
    existing_assets = checkCreateCollection(EE_COLLECTION)
    
    # Get a list of tuples of the dates and timesteps of data we already have in the collection
    existing_dates_steps = [getDate(asset) for asset in existing_assets]
    logging.debug(existing_dates_steps)

    # Fetch, process, and upload the new data
    os.chdir(DATA_DIR)
    new_date_step, new_asset = processNewData(existing_dates_steps)

    logging.info('Existing assets: {}, new: {}, max: {}'.format(
        len(existing_assets), len(new_asset), MAX_ASSETS))

    # Delete excess assets
    deleteExcessAssets(existing_dates_steps+new_date_step, MAX_ASSETS)

    # Update Resource Watch
    updateResourceWatch()

    logging.info('SUCCESS')
