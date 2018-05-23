from __future__ import unicode_literals

import os
import sys
import urllib.request
import shutil
from contextlib import closing
#import gzip
import datetime
from dateutil import parser
import logging
#import subprocess
from netCDF4 import Dataset
import rasterio as rio
import eeUtil
import numpy as np

LOG_LEVEL = logging.INFO
CLEAR_COLLECTION_FIRST = False
DOWNLOAD_FILE = True

# constants for bleaching alerts
SOURCE_URL = 'http://soton.eead.csic.es/spei/maps/../nc/{filename}'
SOURCE_FILENAME = 'spei{month_lag}.nc'
FILENAME = 'cli_039_lag{lag}_{date}'
SDS_NAME = 'NETCDF:\"{nc_name}\":{var_name}'

VAR_NAME = 'spei'
TIME_NAME = 'time'
TIMELAGS = ['06']

# Read from dataset
NODATA_VALUE = None
DATA_TYPE = 'Byte' # Byte/Int16/UInt16/UInt32/Int32/Float32/Float64/CInt16/CInt32/CFloat32/CFloat64
MISSING_VALUE_NAME = "missing_value"

DATA_DIR = 'data/'
GS_FOLDER = 'cli_039_spei'
EE_COLLECTION = 'cli_039_spei'

MAX_ASSETS = 36
DATE_FORMAT = '%Y%m15'
TIMESTEP = {'days': 30}

def getAssetName(date, lag):
    '''get asset name from datestamp'''
    return os.path.join(EE_COLLECTION, FILENAME.format(date=date, lag=lag))

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
        if datestr not in exclude_dates + new_dates:
            new_dates.append(datestr)
    return new_dates

def fetch(filename, lag):
    '''Fetch files by datestamp'''
    # New data may not yet be posted
    _file = SOURCE_URL.format(filename=SOURCE_FILENAME.format(month_lag=lag))
    try:
        if DOWNLOAD_FILE:
            with closing(urllib.request.urlopen(_file)) as r:
                #with gzip.open(r, "rb") as unzipped:
                with open(filename, 'wb') as f:
                    #shutil.copyfileobj(unzipped, f)
                    shutil.copyfileobj(r, f)

        #urllib.request.urlretrieve(_file, filename)
        #cmd = ['head', filename]
        #subprocess.call(cmd)
        #cmd = ['gdalinfo', filename]
        #subprocess.call(cmd)

    except Exception as e:
        logging.warning('Could not fetch {}'.format(_file))
        logging.error(e)
    return filename

def extract_metadata(nc_file):
    nc = Dataset(nc_file)
    logging.info(nc)
    logging.info(nc.variables)
    logging.info(nc[VAR_NAME])

    dtype = str(nc[VAR_NAME].dtype)
    nodata = float(nc[VAR_NAME].getncattr("_FillValue"))
    #nodata = float(nc[VAR_NAME].getncattr(MISSING_VALUE_NAME))

    del nc
    return dtype, nodata

def retrieve_formatted_dates(nc_file, date_pattern=DATE_FORMAT):
    '''
    Inputs:
    * pointer to a netcdf file
    Outputs:
    * dates formatted according to DATE_FORMAT
    '''
    # Extract time variable range
    nc = Dataset(nc_file)
    time_displacements = nc[TIME_NAME]
    del nc

    # Identify time units
    # fuzzy=True allows the parser to pick the date out from a string with other text
    time_units = time_displacements.getncattr('units')
    logging.debug("Time units: {}".format(time_units))
    ref_time = parser.parse(time_units, fuzzy=True)
    logging.debug("Reference time: {}".format(ref_time))

    # Format times to DATE_FORMAT

    ###
    ## REPLACE W/ MAP FUNCTION
    ###

    formatted_dates = [(ref_time + datetime.timedelta(days=int(time_disp))).strftime(date_pattern) for time_disp in time_displacements]
    logging.debug('Dates available: {}'.format(formatted_dates))
    return(formatted_dates)

def extract_subdata_by_date(nc_file, lag, dtype, nodata, available_dates, target_dates):
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
        data = nc[VAR_NAME][date_ix,:,:]
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
            'dtype':dtype,
            'crs':'EPSG:4326',
            'transform':transform,
            'compress':'lzw',
            'nodata':nodata
        }
        # Set filename
        sub_tif = DATA_DIR + '{}.tif'.format(FILENAME.format(date=date, lag=lag))
        logging.info(sub_tif)

        with rio.open(sub_tif, 'w', **profile) as dst:
            ## Need to flip array, original data comes in upside down
            flipped_array = np.flipud(data.astype(dtype))
            dst.write(flipped_array, indexes=1)
        sub_tifs.append(sub_tif)

    del nc
    return sub_tifs


def processNewData(existing_dates, lag):
    '''fetch, process, upload, and clean new data'''
    # 1. Determine which years to read from the netCDF file
    target_dates = getNewTargetDates(existing_dates)

    # 2. Fetch datafile
    logging.info('Fetching files')
    nc_file = fetch(DATA_DIR + 'nc_file.nc', lag)
    available_dates = retrieve_formatted_dates(nc_file)
    dtype, nodata = extract_metadata(nc_file)
    logging.info('type: ' + dtype)
    logging.info('nodata val: ' + str(nodata))

    if target_dates:
        # 3. Convert new files
        logging.info('Converting files')
        sub_tifs = extract_subdata_by_date(nc_file, lag, dtype, nodata, available_dates, target_dates)
        logging.info(sub_tifs)

        # 4. Upload new files
        logging.info('Uploading files')
        dates = [getDate(tif) for tif in sub_tifs]
        datestamps = [datetime.datetime.strptime(date, DATE_FORMAT)
                      for date in dates]
        assets = [getAssetName(date, lag) for date in dates]
        eeUtil.uploadAssets(sub_tifs, assets, GS_FOLDER, datestamps)

        # 5. Delete local files
        logging.info('Cleaning local files')
        os.remove(nc_file)
        for tif in sub_tifs:
            logging.debug('deleting: ' + tif)
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
    eeUtil.initJson()

    # 1. Check if collection exists and create
    if CLEAR_COLLECTION_FIRST:
        if eeUtil.exists(EE_COLLECTION):
            eeUtil.removeAsset(EE_COLLECTION, recursive=True)

    existing_assets = checkCreateCollection(EE_COLLECTION)
    existing_dates = [getDate(a) for a in existing_assets]

    # 2. Fetch, process, stage, ingest, clean
    new_assets =  []
    for lag in TIMELAGS:
        new_assets.extend(processNewData(existing_dates, lag))
    new_dates = [getDate(a) for a in new_assets]

    # 3. Delete old assets
    existing_dates = existing_dates + new_dates
    logging.info('Existing assets: {}, new: {}, max: {}'.format(
        len(existing_dates), len(new_dates), MAX_ASSETS))
    deleteExcessAssets(existing_dates, MAX_ASSETS)

    ###

    logging.info('SUCCESS')
