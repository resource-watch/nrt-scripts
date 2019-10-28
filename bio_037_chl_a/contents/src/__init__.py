from __future__ import unicode_literals

import sys
import urllib
import datetime
import logging
import subprocess
import eeUtil
import urllib.request
from netCDF4 import Dataset
import os
import calendar
import numpy as np
import requests
import time

#The file naming convention that MODIS-Aqua uses is as follows:
#A{start year}{julian day of the first of the month}{end year}{julian day of the end of the month}.L3m_MO_CHL_chlor_a_9km
#Example: A20181822018212.L3m_MO_CHL_chlor_a_9km.nc
#Julian day 182 of 2018 corresponds to July 1st
#Julian day 212 of 2018 corresponds to July 31st
#This file has the monthly chlorophyll for July 2018

#The files are uploaded to GEE as:
# "users/resourcewatch_wri/bio_037_chl_a/bio_037_chl_a_{start year}{julian day of the first of the month}{end year}{julian day of the end of the month}"
#Example: users/resourcewatch_wri/bio_037_chl_a/bio_037_chl_a_20181822018212

# Sources for nrt data
SOURCE_URL = 'https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A{date}.L3m_MO_CHL_chlor_a_4km.nc'


SDS_NAME = 'NETCDF:"{fname}":chlor_a'
FILENAME = 'bio_037_chl_a_{date}'
NODATA_VALUE = -32767.0

DATA_DIR = 'data'
GS_FOLDER = 'bio_037_chl_a'
EE_COLLECTION = 'bio_037_chl_a'
CLEAR_COLLECTION_FIRST = False
DELETE_LOCAL = True

MAX_ASSETS = 8
DATE_FORMAT = '%Y%m%d'
LOG_LEVEL = logging.INFO
DATASET_ID = 'd4e91298-b994-4e2c-947c-4f6486a66f30'

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


def getUrl(date):
    '''get source url from datestamp'''
    return SOURCE_URL.format(date=date)

def getAssetName(date):
    '''get asset name from datestamp'''# os.path.join('home', 'coming') = 'home/coming'
    return os.path.join(EE_COLLECTION, FILENAME.format(date=date))


def getFilename(date):
    '''get filename from datestamp CHECK FILE TYPE'''
    return os.path.join(DATA_DIR, '{}.nc'.format(
        FILENAME.format(date=date)))
        
def getDate(filename):
    '''get last 8 chrs of filename CHECK THIS'''
    return os.path.splitext(os.path.basename(filename))[0][14:28]


def getNewDates(exclude_dates):
    '''Get new dates excluding existing'''
    new_dates = []
    new_datetime = []
    date = datetime.date.today()
    for i in range(MAX_ASSETS):
        current_month = date.month
        date = date.replace(day=1)
        if current_month==1:
            current_year = date.year
            date = date.replace(year=current_year-1)
            date = date.replace(month=12)
        else:
            date = date.replace(month=current_month-1)
        startdate = date.replace(day=1)
        enddate = date.replace(day=calendar.monthrange(startdate.year, startdate.month)[1])
        
        start_jday = str(startdate.timetuple().tm_yday)
        if len(start_jday)==2:
            start_jday = '0'+start_jday
        elif len(start_jday)==1:
            start_jday = '00'+start_jday
            
        end_jday = str(enddate.timetuple().tm_yday)
        if len(end_jday)==2:
            end_jday = '0'+end_jday
        elif len(end_jday)==1:
            end_jday = '00'+end_jday
            
        start = str(startdate.year)+ start_jday
        end = str(enddate.year)+ end_jday
        datestr = start+end
        if datestr not in exclude_dates:
            new_dates.append(datestr)
            new_datetime.append(datetime.datetime.combine(enddate,datetime.datetime.min.time()))
    return new_dates,new_datetime

#https://gis.stackexchange.com/questions/6669/converting-projected-geotiff-to-wgs84-with-gdal-and-python
def convert(files):
    '''convert snow cover hdfs to tifs'''
    tifs = []
    for f in files:
        #Apply natural logarithm to data, this is so that when interpolating colors in the SLD style, the difference between 0.01 and 0.03 is the same as 10 and 30 mg/m^3
        #Chlorophyll-a is most often displayed on the log scale
        dat = Dataset(f,'r+')
        chlor = dat.variables['chlor_a']
        log = np.ma.log(chlor[:])
        chlor[:] = log
        dat.close()
        # extract subdataset by name
        sds_path = SDS_NAME.format(fname=f)
        tif = '{}.tif'.format(os.path.splitext(f)[0]) #naming tiffs
        #os.path.splitext gets rids of .hdf because it makes a list of file name[0] and ext [1]
        #and only takes the file name (splits on last period)
        # nodata value -5 equals 251 for Byte type?
        cmd = ['gdal_translate','-q', '-a_nodata', str(NODATA_VALUE), '-a_srs', 'EPSG:4326', sds_path, tif] #'-q' means quite so dont see it
        logging.debug('Converting {} to {}'.format(f, tif))
        subprocess.call(cmd) #using the gdal from command line from inside python
        tifs.append(tif)
    return tifs





def fetch(new_dates):

    # 2. Loop over the new dates, check if there is data available, and attempt to download the hdfs
    files = []
    for date in new_dates:
        # Setup the url of the folder to look for data, and the filename to download to if available
        url = getUrl(date)
        #starts as string, strptime changes to datetime object, strfttime reformats into string)
        f = getFilename(date)
        try:
            urllib.request.urlretrieve(url, f)
            files.append(f)
            logging.info('Successfully retrieved {}'.format(f))# gives us "Successully retrieved file name"

        except Exception as e:
            logging.info('Unable to retrieve data from {}, most likely NASA has not uploaded file'.format(url))
            #NASA does not upload the previous month's chlorophyll until the middle of the next month
            #Error is raised when trying to access this file via URL as the file has not been uploaded by NASA
            #Send this error to log file instead of paper trails
            #logging.error('Unable to retrieve data from {}'.format(url))
            #logging.debug(e)

    return files

def processNewData(existing_dates):
    '''fetch, process, upload, and clean new data'''
    # 1. Determine which files to fetch
    new_dates,new_datetimes = getNewDates(existing_dates)

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
        #datestamps = [datetime.datetime.strptime(date, DATE_FORMAT) #list comprehension/for loop
        #              for date in new_datetimes] #returns list of datetime object
        assets = [getAssetName(date) for date in dates] #create asset nema (imagecollect +tiffname)
        eeUtil.uploadAssets(tifs, assets, GS_FOLDER, new_datetimes) #puts on GEE
        #eeUtil.uploadAssets(tifs, assets, GS_FOLDER) #puts on GEE

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
    most_recent_date_julian = existing_dates[-1][-7:]
    most_recent_date = datetime.datetime.strptime(most_recent_date_julian, '%Y%j').date()
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

    # 4. Check most recent asset and report it as the most recent update on Resource Watch
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
