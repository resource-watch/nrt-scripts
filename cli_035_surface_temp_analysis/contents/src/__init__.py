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

LOG_LEVEL = logging.INFO
CLEAR_COLLECTION_FIRST = False

# constants for bleaching alerts
SOURCE_URL = 'https://data.giss.nasa.gov/pub/gistemp/{target_file}'
SOURCE_FILENAME = 'gistemp250.nc.gz'
FILENAME = 'cli_035_{date}'
NODATA_VALUE = None
DATA_TYPE = 'Byte' # Byte/Int16/UInt16/UInt32/Int32/Float32/Float64/CInt16/CInt32/CFloat32/CFloat64
VAR_NAME = 'tempanomaly'
TIME_NAME = 'time'
# For NetCDF
SDS_NAME = 'NETCDF:\"{nc_name}\":{var_name}'

DATA_DIR = 'data'
GS_PREFIX = 'cli_035_surface_temp_analysis'
EE_COLLECTION = 'cli_035_surface_temp_analysis'

MAX_ASSETS = 36
DATE_FORMAT = '%Y%m15'
TIMESTEP = {'days': 30}

# environmental variables
GEE_SERVICE_ACCOUNT = os.environ.get("GEE_SERVICE_ACCOUNT")
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS")
GEE_STAGING_BUCKET = os.environ.get("GEE_STAGING_BUCKET")
GCS_PROJECT = os.environ.get("CLOUDSDK_CORE_PROJECT")

def getAssetName(date):
    '''get asset name from datestamp'''
    return os.path.join(EE_COLLECTION, FILENAME.format(date=date))

def getDate(filename):
    '''get last 8 chrs of filename'''
    return os.path.splitext(os.path.basename(filename))[0][-8:]

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

def fetch(url, target_file, filename):
    '''Fetch files by datestamp'''
    # New data may not yet be posted
    _file = url.format(target_file=target_file)
    try:
        with closing(urllib.request.urlopen(_file)) as r:
            with gzip.open(r, "rb") as unzipped:
                with open(filename, 'wb') as f:
                    shutil.copyfileobj(unzipped, f)
        #urllib.request.urlretrieve(_file, filename)
        #cmd = ['head', filename]
        #subprocess.call(cmd)
        #cmd = ['gdalinfo', filename]
        #subprocess.call(cmd)
    except Exception as e:
        logging.warning('Could not fetch {}'.format(_file))
        logging.error(e)
    return filename

def extract_metadata(nc_file, data_var_name):
    nc = Dataset(nc_file)
    DATA_TYPE = str(nc[data_var_name].dtype)
    NODATA_VALUE = str(nc[data_var_name].getncattr("_FillValue"))
    del nc
    return DATA_TYPE, NODATA_VALUE

def retrieve_formatted_dates(nc, time_var_name, date_pattern=DATE_FORMAT):
    '''
    Inputs:
    * pointer to a netcdf file
    * name of the time variable
    Outputs:
    * dates formatted according to DATE_FORMAT
    '''
    # Extract time variable range
    nc = Dataset(nc)
    time_displacements = nc[time_var_name]
    # Clean up reference to nc object
    del nc

    # Identify time units
    # fuzzy=True allows the parser to pick the date out from a string with other text
    time_units = time_displacements.getncattr('units')
    logging.debug("Time units: {}".format(time_units))
    ref_time = parser.parse(time_units, fuzzy=True)
    logging.debug("Reference time: {}".format(ref_time))

    # Format times to DATE_FORMAT
    formatted_dates = [(ref_time + datetime.timedelta(days=int(time_disp))).strftime(date_pattern) for time_disp in time_displacements]
    logging.debug('Dates available: {}'.format(formatted_dates))
    return(formatted_dates)

def extract_subdata_by_date(nc_file, data_var_name, data_type, nodata_value, available_dates, target_dates):
    '''
    new_dates should be a list of tuples of form (date, index_in_netcdf)
    '''
    nc = Dataset(nc_file)
    sub_tifs = []
    for date in target_dates:
        # Find index in available dates, if not available, skip this date
        try:
            date_ix = available_dates.index(date)
            logging.info("Date {} found! Processing...".format(date))
        except:
            logging.error("Date {} not found in available dates".format(date))
            continue

        # Extract data
        data = nc[data_var_name][date_ix,:,:]
        # Create profile/tif metadata
        south_lat = -90
        north_lat = 90
        west_lon = -180
        east_lon = 180
        # Transformation function
        transform = rio.transform.from_bounds(west_lon, south_lat, east_lon, north_lat, data.shape[1], data.shape[0])
        # Profile
        profile = {
            'driver':'GTiff',
            'height':data.shape[0],
            'width':data.shape[1],
            'count':1,
            'dtype':data_type,
            'crs':'EPSG:4326',
            'transform':transform,
            'compress':'lzw',
            'nodata':nodata_value
        }
        # Set filename
        sub_tif = '{}.tif'.format(FILENAME.format(date=date))
        logging.info(sub_tif)

        with rio.open(sub_tif, 'w', **profile) as dst:
            dst.write(data.astype(data_type), indexes=1)
        sub_tifs.append(sub_tif)

    del nc
    return sub_tifs


def processNewData(existing_dates):
    '''fetch, process, upload, and clean new data'''
    # 1. Determine which years to read from the netCDF file
    target_dates = getNewTargetDates(existing_dates)

    # 2. Fetch datafile
    logging.info('Fetching files')
    nc_file = fetch(SOURCE_URL, SOURCE_FILENAME, 'nc_file.nc')
    available_dates = retrieve_formatted_dates(nc_file, TIME_NAME)
    DATA_TYPE, NODATA_VALUE = extract_metadata(nc_file, VAR_NAME)

    if target_dates:
        # 3. Convert new files
        logging.info('Converting files')
        sub_tifs = extract_subdata_by_date(nc_file,VAR_NAME,DATA_TYPE,NODATA_VALUE,
                                            available_dates, target_dates)

        # 4. Upload new files
        logging.info('Uploading files')
        dates = [getDate(tif) for tif in sub_tifs]
        assets = [getAssetName(date) for date in dates]
        eeUtil.uploadAssets(sub_tifs, assets, GS_PREFIX, dates, public=True, timeout=3000)

        # 5. Delete local files
        logging.info('Cleaning local files')
        os.remove(nc_file)
        for tif in sub_tifs:
            os.remove(tif)

        return assets
    return []


def checkCreateCollection(collection):
    '''List assests in collection else create new collection'''
    if eeUtil.exists(collection):
        return eeUtil.ls(collection)
    else:
        logging.info('{} does not exist, creating'.format(collection))
        eeUtil.createFolder(collection, imageCollection=True, public=True)
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
        if eeUtil.exists(EE_COLLECTION):
            eeUtil.removeAsset(EE_COLLECTION, recursive=True)

    # 1. Check if collection exists and create
    existing_assets = checkCreateCollection(EE_COLLECTION)
    existing_dates = [getDate(a) for a in existing_assets]

    # 2. Fetch, process, stage, ingest, clean
    new_assets =  processNewData(existing_dates)
    new_dates = [getDate(a) for a in new_assets]

    # 3. Delete old assets
    existing_dates = existing_dates + new_dates
    logging.info('Existing assets: {}, new: {}, max: {}'.format(
        len(existing_dates), len(new_dates), MAX_ASSETS))
    deleteExcessAssets(existing_dates, MAX_ASSETS)

    ###

    logging.info('SUCCESS')
