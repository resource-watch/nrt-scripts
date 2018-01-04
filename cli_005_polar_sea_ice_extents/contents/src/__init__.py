from __future__ import unicode_literals

import os
import sys
import urllib.request
import shutil
from contextlib import closing
import gzip
import datetime
from dateutil import parser
import logging
import subprocess
from netCDF4 import Dataset
import rasterio as rio
from . import eeUtil

LOG_LEVEL = logging.DEBUG
CLEAR_COLLECTION_FIRST = False
VERSION = '3.0'

# constants for bleaching alerts
SOURCE_URL = 'ftp://sidads.colorado.edu/DATASETS/NOAA/G02135/{north_or_south}/monthly/geotiff/{month}/{target_file}'
SOURCE_FILENAME = '{n_or_s}_{date}_extent_v{version}.tif'
ASSET_NAME = 'cli_005_{arctic_or_antarctic}_sea_ice_{date}'

# Read from data
NODATA_VALUE = 0
DATA_TYPE = 'Byte' # Byte/Int16/UInt16/UInt32/Int32/Float32/Float64/CInt16/CInt32/CFloat32/CFloat64

# For NetCDF
DATA_DIR = 'data'
GS_PREFIX = 'cli_005_polar_sea_ice_extent'
EE_COLLECTION = 'cli_005_polar_sea_ice_extent'

# Times two because of North / South parallels
MAX_ASSETS = 36*2
DATE_FORMAT = '%Y%m'
TIMESTEP = {'days': 30}

# environmental variables
GEE_SERVICE_ACCOUNT = os.environ.get("GEE_SERVICE_ACCOUNT")
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS")
GEE_STAGING_BUCKET = os.environ.get("GEE_STAGING_BUCKET")
GCS_PROJECT = os.environ.get("CLOUDSDK_CORE_PROJECT")

def getAssetName(date, location):
    '''get asset name from datestamp'''
    return os.path.join(EE_COLLECTION, ASSET_NAME.format(arctic_or_antarctic=location, date=date))

def getDate(filename):
    '''get last 8 chrs of filename'''
    return os.path.splitext(os.path.basename(filename))[0][-6:]

def getNewTargetDates(exclude_dates):
    '''Get new dates excluding existing'''
    new_dates = []
    date = datetime.date.today()
    date.replace(day=15)
    for i in range(MAX_ASSETS):
        date -= datetime.timedelta(**TIMESTEP)
        date.replace(day=15)
        datestr = date.strftime(DATE_FORMAT)
        if datestr not in exclude_dates:
            new_dates.append(datestr)
    return new_dates

def format_month(month):
    names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    name = name[month]
    if len(str(month))==1:
        num = '0' + str(month)
    else:
        num = str(month)
    return('_'.join[num, name])

def fetch(url, north_or_south, date):
    '''Fetch files by datestamp'''
    # New data may not yet be posted
    month = format_month(date.month)
    target_file = SOURCE_FILENAME.format(n_or_s=north_or_south[0], date=date, version=VERSION)
    _file = url.format(north_or_south=north_or_south,month=month,target_file=target_file)
    arctic_or_antarctic = 'arctic' if (north_or_south=='north') else 'antarctic'
    filename = ASSET_NAME.format(arctic_or_antarctic=arctic_or_antarctic, date=date)
    try:
        with closing(urllib.request.urlopen(_file)) as r:
            with open(filename, 'wb') as f:
                shutil.copyfileobj(r, f)
    except Exception as e:
        logging.warning('Could not fetch {}'.format(_file))
        logging.error(e)
    return filename

def processNewData(existing_dates):
    '''fetch, process, upload, and clean new data'''
    # 1. Determine which years to read from the netCDF file
    target_dates = getNewTargetDates(existing_dates) or []

    # 2. Fetch datafile
    logging.info('Fetching files')
    tifs = []
    for date in target_dates:
        logging.info('Converting files')
        tifs.append(fetch(SOURCE_URL, 'north', date))
        tifs.append(fetch(SOURCE_URL, 'south', date))

    # 3. Upload new files
    logging.info('Uploading files')
    dates = [getDate(tif) for tif in tifs]
    assets = [getAssetName(date) for date in dates]
    eeUtil.uploadAssets(tifs, assets, GS_PREFIX, dates, public=True, timeout=3000)

    # 4. Delete local files
    logging.info('Cleaning local files')
    for tif in tifs:
        os.remove(tif)

    return assets


def checkCreateCollection(collection):
    '''List assests in collection else create new collection'''
    if eeUtil.exists(collection):
        return eeUtil.ls(collection)
    else:
        logging.info('{} does not exist, creating'.format(collection))
        eeUtil.createFolder(collection, ImageCollection=True, public=True)
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

    # Initialize eeUtil
    eeUtil.init(GEE_SERVICE_ACCOUNT, GOOGLE_APPLICATION_CREDENTIALS,
                GCS_PROJECT, GEE_STAGING_BUCKET)

    if CLEAR_COLLECTION_FIRST:
        eeUtil.removeAsset(EE_COLLECTION, recursive=True)

    # 1. Check if collection exists and create
    existing_assets = checkCreateCollection(EE_COLLECTION)
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
