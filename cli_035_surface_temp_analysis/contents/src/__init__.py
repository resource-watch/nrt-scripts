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

# constants for bleaching alerts
SOURCE_URL = 'https://data.giss.nasa.gov/pub/gistemp/{target_file}'
SOURCE_FILENAME = 'gistemp250.nc.gz'
FILENAME = 'cli_035_{date}'
NODATA_VALUE = None
DATA_TYPE = 'Byte' # Byte/Int16/UInt16/UInt32/Int32/Float32/Float64/CInt16/CInt32/CFloat32/CFloat64
VAR_NAME = 'tempanomaly'
TIME_NAME = 'time'
# For NetCDF
SDS_NAME = 'NETCDF:"{nc_name}":{var_name}'

DATA_DIR = 'data'
GS_PREFIX = 'cli_035_surface_temp_analysis_{date}'
EE_COLLECTION = 'cli_035_surface_temp_analysis'

MAX_ASSETS = 31
DATE_FORMAT = '%Y%m%d'
TIMESTEP = {'days': 1}

# environmental variables
GEE_SERVICE_ACCOUNT = os.environ.get("GEE_SERVICE_ACCOUNT")
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS")
GEE_STAGING_BUCKET = os.environ.get("GEE_STAGING_BUCKET")
GCS_PROJECT = os.environ.get("CLOUDSDK_CORE_PROJECT")

def getAttributes(nc):
    '''Setting attribute values'''
    NODATA_VALUE = nc[data_var_name].getncattr("_FillValue")
    DATA_TYPE = ''

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
    for i in range(MAX_ASSETS):
        date -= datetime.timedelta(**TIMESTEP)
        datestr = date.strftime(DATE_FORMAT)
        if datestr not in exclude_dates:
            new_dates.append(datestr)
    return new_dates

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
    logging.info(time_units)
    ref_time = parser.parse(time_units, fuzzy=True)
    logging.info(ref_time)

    # Format times to DATE_FORMAT
    formatted_dates = [(ref_time + datetime.timedelta(days=int(time_disp))).strftime(date_pattern) for time_disp in time_displacements]
    logging.debug('Dates available: {}'.format(formatted_dates))
    return(formatted_dates)

def convert(nc_file, subdataset_name, var_name, data_type, nodata_value):
    '''convert surface temperature nc to tif'''
    vrt_name = 'data.vrt'
    tif_name = 'data.tif'
    # extract subdataset by name
    sds_path = subdataset_name.format(nc_name=nc_file, var_name=var_name)
    logging.info("Converting {} to {}".format(sds_path, tif_name))
    # set nodata  is out of range of Byte type
    logging.debug('Assert reprojection of {} to {}, EPSG:4326'.format(nc_file, vrt_name))
    reproject = ['gdalwarp', '-o', '-a_nodata', nodata_value, '-t_srs', 'EPSG:4326',
                 '-te', '-180 -90 180 90', '-ot', data_type, '-of', 'VRT', '-r', 'bilinear',
                 sds_path, vrt_name]
    subprocess.call(reproject)

    logging.debug('Assert compression of {} to {}, EPSG:4326'.format(vrt_name, tif_name))
    compress = ['gdal_translate', '-co', 'COMPRESS=LZW', '-of', 'GTIFF',
                vrt_name, tif_name]
    subprocess.call(reproject)

    return vrt_name, tif_name

def extract_subdata_by_date(tif, available_dates, target_dates):
    '''
    new_dates should be a list of tuples of form (date, index_in_netcdf)
    '''
    sub_tifs = []
    with rio.open(tif, 'r') as src:
        profile = src.profile()
        for date in target_dates:
            # Find index in available dates, if not available, skip this date
            try:
                date_ix = available_dates.index(date)
                logging.info("Date {} found! Processing...".format(date))
            except:
                logging.error("Date {} not found in availabel dates".format(date))
                pass
            # Extract data
            data = src.read(indexes=date_ix)
            # Create profile/tif metadata, update to be 1 count only
            new_profile = profile.copy()
            new_profile.update(count=1)
            # Set filename
            sub_tif = FILENAME.format(date)

            with rio.open(open(sub_tif, 'w'), **new_profile) as dst:
                dst.write(data, indexes=1)
            sub_tifs.append(sub_tif)

    return sub_tifs

def fetch(url, target_file, filename):
    '''Fetch files by datestamp'''
    # New data may not yet be posted
    _file = url.format(target_file=target_file)
    try:
        # with closing(urllib.request.urlopen(_file)) as r:
        #     with gzip.open(r, "rb") as unzipped:
        #         with open(filename, 'wb') as f:
        #             shutil.copyfileobj(unzipped, f)
        urllib.request.urlretrieve(_file, filename)
        cmd = ['gdalinfo', filename]
        subprocess.call(cmd)
    except Exception as e:
        logging.warning('Could not fetch {}'.format(_file))
        logging.error(e)
    return filename


def processNewData(existing_dates):
    '''fetch, process, upload, and clean new data'''
    # 1. Determine which years to read from the netCDF file
    target_dates = getNewTargetDates(existing_dates)

    # 2. Fetch datafile
    logging.info('Fetching files')
    nc_file = fetch(SOURCE_URL, SOURCE_FILENAME, 'nc_file.nc')
    available_dates = retrieve_formatted_dates(nc_file, TIME_NAME)

    if target_dates:
        # 3. Convert new files
        logging.info('Converting files')
        vrt_file, tif_name = convert(nc_file, SDS_NAME, VAR_NAME, DATA_TYPE, NODATA_VALUE)
        sub_tifs = extract_subdata_by_date(tif_name, available_dates, target_dates)

        # 4. Upload new files
        logging.info('Uploading files')
        dates = [getDate(tif) for tif in sub_tifs]
        assets = [getAssetName(date) for date in dates]
        eeUtil.uploadAssets(sub_tifs, assets, '', GS_PREFIX, dates)

        # 5. Delete local files
        logging.info('Cleaning local files')
        os.remove(nc_file)
        os.remove(vrt_file)
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
        eeUtil.createFolder(collection, '', True, public=True)
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
