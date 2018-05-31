from __future__ import unicode_literals

import os
import sys
import urllib
import datetime
import logging
import subprocess
import eeUtil
import requests
import urllib.request
from bs4 import BeautifulSoup
import os
from http.cookiejar import CookieJar

# constants for bleaching alerts
SOURCE_URL = 'https://n5eil01u.ecs.nsidc.org/MOST/MOD10C2.006/{date}'
SDS_NAME = 'HDF4_EOS:EOS_GRID:"{fname}":MOD_CMG_Snow_5km:Eight_Day_CMG_Snow_Cover'
FILENAME = 'cli_021_{date}'
NODATA_VALUE = 255

DATA_DIR = 'data'
GS_FOLDER = 'cli_021_snow_cover'
EE_COLLECTION = 'cli_021_snow_cover'
CLEAR_COLLECTION_FIRST = False
DELETE_LOCAL = True

MAX_ASSETS = 8
DATE_FORMAT_HDF = '%Y.%m.%d'
DATE_FORMAT = '%Y%m%d'
TIMESTEP = {'days': 1} #check everyday so don't start on day without and miss

LOG_LEVEL = logging.INFO

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
    date = datetime.date.today()
    for i in range(MAX_ASSETS*8): #because only updates every 8 days
        date -= datetime.timedelta(**TIMESTEP) #substraction and assignments in one step
        datestr = date.strftime(DATE_FORMAT_HDF)#of HDF because looking for new data in old format
        if date.strftime(DATE_FORMAT) not in exclude_dates:
            new_dates.append(datestr) #add to new dates if have not already seen
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
    username = os.environ.get('NASA_USER')
    password = os.environ.get('NASA_PASS')

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

    logging.info('SUCCESS')
