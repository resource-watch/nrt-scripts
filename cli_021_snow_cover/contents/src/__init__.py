from __future__ import unicode_literals

import sys
import urllib
import datetime
import logging
import subprocess
import eeUtil
import urllib.request
from bs4 import BeautifulSoup
import os
from http.cookiejar import CookieJar
import requests
import time
from dateutil.relativedelta import relativedelta

# constants for bleaching alerts
#Old values for 8 day dataset
#SOURCE_URL = 'https://n5eil01u.ecs.nsidc.org/MOST/MOD10C2.006/{date}'
#SDS_NAME = 'HDF4_EOS:EOS_GRID:"{fname}":MOD_CMG_Snow_5km:Eight_Day_CMG_Snow_Cover'
SOURCE_URL = 'https://n5eil01u.ecs.nsidc.org/MOST/MOD10CM.006/{date}'
SDS_NAME = 'HDF4_EOS:EOS_GRID:"{fname}":MOD_CMG_Snow_5km:Snow_Cover_Monthly_CMG'
FILENAME = 'cli_021_{date}'
NODATA_VALUE = 255

DATA_DIR = 'data'
#Old values for 8 day dataset
#GS_FOLDER = 'cli_021_snow_cover'
#EE_COLLECTION = 'cli_021_snow_cover'
GS_FOLDER = 'cli_021_snow_cover_monthly'
EE_COLLECTION = 'cli_021_snow_cover_monthly'
CLEAR_COLLECTION_FIRST = False
DELETE_LOCAL = True

MAX_ASSETS = 8
DATE_FORMAT_HDF = '%Y.%m.%d'
DATE_FORMAT = '%Y%m%d'

LOG_LEVEL = logging.INFO
DATASET_ID = '23f29e9a-ca07-4c08-a018-28a25af14b49'

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

DATASET_ID = '23f29e9a-ca07-4c08-a018-28a25af14b49'

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

def getUrl(date):
    '''get source url from datestamp'''
    return SOURCE_URL.format(date=date)


def getAssetName(date):
    '''get asset name from datestamp'''# os.path.join('home', 'coming') = 'home/coming'
    return os.path.join(EE_COLLECTION, FILENAME.format(date=date))


def getFilename(date):
    '''get filename from datestamp CHECK FILE TYPE'''
    return os.path.join(DATA_DIR, '{}.hdf'.format(
        FILENAME.format(date=date)))


def getDate(filename):
    '''get last 8 chrs of filename CHECK THIS'''
    return os.path.splitext(os.path.basename(filename))[0][-8:]


def getNewDates(exclude_dates):
    '''Get new dates excluding existing'''
    new_dates = []
    #get today's date, then replace day to be the first of the current month
    date = datetime.date.today().replace(day=1)
    exclude_datestr = date.strftime(DATE_FORMAT)  # of HDF because looking for new data in old format
    while exclude_datestr not in exclude_dates:
        datestr = date.strftime(DATE_FORMAT_HDF)#of HDF because looking for new data in old format
        new_dates.append(datestr) #add to new dates if have not already seen
        #go back to next previous month
        date=date - relativedelta(months=1) #subtract 1 month from data
        exclude_datestr = date.strftime(DATE_FORMAT)
    return new_dates


def convert(files):
    '''convert snow cover hdfs to tifs'''
    tifs = []
    for f in files:
        # extract subdataset by name
        sds_path = SDS_NAME.format(fname=f)
        #temp = '{}temp.tif'.format(os.path.splitext(f)[0]) #naming tiffs
        tif = '{}.tif'.format(os.path.splitext(f)[0]) #naming tiffs
        #os.path.splitext gets rids of .hdf because it makes a list of file name[0] and ext [1]
        #and only takes the file name (splits on last period)
        # nodata value -5 equals 251 for Byte type?
        cmd = ['gdal_translate','-q', '-a_nodata', '255', sds_path, tif] #'-q' means quite so dont see it
        logging.debug('Converting {} to {}'.format(f, tif))
        subprocess.call(cmd) #using the gdal from command line from inside python
        #cmd_no_vals = ['gdal_calc.py', '-A', temp,  '--outfile={}'.format(tif), '--calc="A>107"', '--NoDataValue=255']
        #subprocess.call(cmd_no_vals)
        tifs.append(tif)
    return tifs


def fetch(new_dates):
    # 1. Set up authentication with the urllib.request library
    username = os.environ.get('EARTHDATA_USER')
    password = os.environ.get('EARTHDATA_PASS')

    password_manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    password_manager.add_password(None, "https://urs.earthdata.nasa.gov", username, password)

    cookie_jar = CookieJar()

    opener = urllib.request.build_opener(
        urllib.request.HTTPBasicAuthHandler(password_manager), #tab bc part of opener
        #urllib2.HTTPHandler(debuglevel=1),    # Uncomment these two lines to see
        #urllib2.HTTPSHandler(debuglevel=1),   # details of the requests/responses
        urllib.request.HTTPCookieProcessor(cookie_jar))

    urllib.request.install_opener(opener) #install opener to library

    # 2. Loop over the new dates, check if there is data available, and attempt to download the hdfs
    files = []
    for date in new_dates:
        # Setup the url of the folder to look for data, and the filename to download to if available
        url = getUrl(date)
        file_date = datetime.datetime.strptime(date, DATE_FORMAT_HDF).strftime(DATE_FORMAT)
        #starts as string, strptime changes to datetime object, strfttime reformats into string)

        f = getFilename(file_date)
        try:
            # Actually look in the folder
            response = urllib.request.urlopen(url)
            content = response.read()
            # request has ok attribute that true if went through correctly
            soup = BeautifulSoup(content, 'html.parser')
            hdfs = []
            for a in soup.find_all('a'):
                str_a = str(a)
                if 'hdf' in str_a:
                    ext = str_a.index('.hdf')
                    hdf = str_a[9:ext+4]
                    hdfs.append(hdf)

            hdfs = list(set(hdfs))
            hdf = hdfs[0]

            url = os.path.join(url, hdf)

            try:
                urllib.request.urlretrieve(url, f)
                files.append(f)
                logging.info('Successfully retrieved {}'.format(f))# gives us "Successully retrieved file name"

            except Exception as e:
                logging.error('Unable to retrieve data from {}'.format(url))
                logging.debug(e)

        except Exception as e:
            logging.debug('No data found for date {}, could be one of the days not covered by this data set (reminder, only updates once every 8 days)'.format(date))
            logging.debug(e)

    return files

def processNewData(existing_dates):
    '''fetch, process, upload, and clean new data'''
    # 1. Determine which files to fetch
    new_dates = getNewDates(existing_dates)

    # 2. Fetch new files
    logging.info('Fetching files')
    files = fetch(new_dates) #get list of locations of hdfs in docker container

    if files: #if files is empty list do nothing, if something in, convert hdfs
        # 3. Convert new files
        logging.info('Converting files')
        tifs = convert(files) # naming tiffs

        # 4. Upload new files
        logging.info('Uploading files')
        dates = [getDate(tif) for tif in tifs] #finding date for naming tiffs, returns string
        datestamps = [datetime.datetime.strptime(date, DATE_FORMAT) #list comprehension/for loop
                      for date in dates] #returns list of datetime object
        assets = [getAssetName(date) for date in dates] #create asset nema (imagecollect +tiffname)
        eeUtil.uploadAssets(tifs, assets, GS_FOLDER, datestamps) #puts on GEE

        # 5. Delete local files
        if DELETE_LOCAL:
            logging.info('Cleaning local files')
            for tif in tifs:
                os.remove(tif)
            for f in files:
                os.remove(f)

        return assets
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

def get_most_recent_date(collection):
    existing_assets = checkCreateCollection(collection)  # make image collection if doesn't have one
    existing_dates = [getDate(a) for a in existing_assets]
    existing_dates.sort()
    most_recent_date = datetime.datetime.strptime(existing_dates[-1], DATE_FORMAT)
    return most_recent_date

def main():
    '''Ingest new data into EE and delete old data'''
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    # Initialize eeUtil and clear collection in GEE if desired
    eeUtil.initJson()

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

    logging.info(new_dates)

    # Get most recent update date
    most_recent_date = get_most_recent_date(EE_COLLECTION)
    current_date = getLastUpdate(DATASET_ID)

    if current_date != most_recent_date:
        logging.info('Updating last update date and flushing cache.')
        # Update data set's last update date on Resource Watch
        lastUpdateDate(DATASET_ID, most_recent_date)
        # get layer ids and flush tile cache for each
        layer_ids = getLayerIDs(DATASET_ID)
        for layer_id in layer_ids:
            flushTileCache(layer_id)

    logging.info('SUCCESS')
