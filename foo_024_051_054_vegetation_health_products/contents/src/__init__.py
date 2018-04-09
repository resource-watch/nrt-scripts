from __future__ import unicode_literals

import os
import sys
import urllib.request
import shutil
from contextlib import closing
import datetime
import logging
import subprocess
import eeUtil
from functools import reduce
from netCDF4 import Dataset
import rasterio as rio
from rasterio.crs import CRS
import numpy as np
from collections import defaultdict

LOG_LEVEL = logging.INFO
CLEAR_COLLECTION_FIRST = False
DELETE_LOCAL = True

# Sources for nrt data
SOURCE_URL = 'ftp://ftp.star.nesdis.noaa.gov/pub/corp/scsb/wguo/data/VHP_4km/VH/{target_file}'
SOURCE_FILENAME = 'VHP.G04.C07.NP.P{date}.VH.nc'
#SDS_NAME = 'NETCDF:"{fname}":{varname}'

VARIABLES = {
    'foo_024':'VHI',
    'foo_051':'VCI'
}

ASSET_NAMES = {
    'foo_024':'vegetation_health_index',
    'foo_051':'vegetation_condition_index',
}

EE_COLLECTION = '{rw_id}_{varname}'
ASSET_NAME = '{rw_id}_{varname}_{date}'

# For naming and storing assets
DATA_DIR = 'data'
GS_PREFIX = '{rw_id}_{varname}'

MAX_DATES = 36
# http://php.net/manual/en/function.strftime.php
DATE_FORMAT = '%Y0%V'
DATE_FORMAT_ISO = '%G0%V-%w'

TIMESTEP = {'days': 7}
#S_SRS = 'EPSG:32662'

EXTENT = '-180 -55.152 180 75.024002'
DTYPE = rio.float32
NODATA = -999
SCALE_FACTOR = .01

# https://gist.github.com/tomkralidis/baabcad8c108e91ee7ab
#os.environ['GDAL_NETCDF_BOTTOMUP']='NO'

###
## Handling RASTERS
###

def getAssetName(tif, rw_id, varname):
    '''get asset name from tif name, extract datetime and location'''
    date = getRasterDate(tif)
    return os.path.join(EE_COLLECTION.format(rw_id=rw_id,
                                                varname=varname),
                        ASSET_NAME.format(rw_id=rw_id,
                                          varname=varname,
                                          date=date))

def getRasterDate(filename):
    '''get last 7 chrs of filename, 4 for year and 3 for week'''
    return os.path.splitext(os.path.basename(filename))[0][-7:]

def getNewTargetDates(exclude_dates):
    '''Get new dates excluding existing'''
    new_dates = []
    date = datetime.date.today()
    for i in range(MAX_DATES):
        datestr = date.strftime(DATE_FORMAT)
        if datestr not in exclude_dates:
            new_dates.append(datestr)
        date -= datetime.timedelta(**TIMESTEP)
    return new_dates

def fetch(datestr):
    '''Fetch files by datestamp'''
    target_file = SOURCE_FILENAME.format(date=datestr)
    _file = SOURCE_URL.format(target_file=target_file)
    urllib.request.urlretrieve(_file, os.path.join(DATA_DIR,target_file))
    return os.path.join(DATA_DIR,target_file)

def extract_subdata(nc_file, rw_id):
    '''
    new_dates should be a list of tuples of form (date, index_in_netcdf)
    '''
    # Set filename
    nc = Dataset(nc_file)
    var_code = VARIABLES[rw_id]

    var_tif = '{}_{}.tif'.format(os.path.splitext(nc_file)[0], var_code)
    logging.info('New tif: {}'.format(var_tif))

    # Extract data
    data = nc[var_code][:, :]
    logging.debug('Type of data: {}'.format(type(data)))
    logging.debug('Shape: {}'.format(data.shape))
    logging.debug('Min,Max: {},{}'.format(data.data.min(), data.data.max()))
    # not sure why this works, but it gets us to the right numbers
    # except for no-data values
    outdata = data.data.copy()
    outdata[outdata>=0] = outdata[outdata>=0] * SCALE_FACTOR

    # There's some strange deferred execution going on here
    # if I set the mask like this, it scales down unmasked data by 10k
    # outdata[data.mask] = NODATA
    logging.debug('Out min, max: {},{}'.format(outdata.min(), outdata.max()))


    # Transformation function
    transform = rio.transform.from_bounds(*[float(pos) for pos in EXTENT.split(' ')], data.shape[1], data.shape[0])

    # Profile
    profile = {
        'driver': 'GTiff',
        'height': data.shape[0],
        'width': data.shape[1],
        'count': 1,
        'dtype': DTYPE,
        'crs':'EPSG:4326',
        'transform': transform,
        'nodata': NODATA
    }

    with rio.open(var_tif, 'w', **profile) as dst:
        dst.write(outdata.astype(DTYPE), 1)

    del nc
    return var_tif

def reproject(ncfile, rw_id, date):
    # Output filename
    new_file = os.path.join(DATA_DIR, '{}.tif'.format(ASSET_NAME.format(rw_id = rw_id, varname = ASSET_NAMES[rw_id], date = date)))

    logging.info('Extracting subdata')
    # METHOD 1
    extracted_var_tif = extract_subdata(ncfile, rw_id)

    # METHOD 2
    ### Using this, get error:
    # ERROR 1: Unable to compute a transformation between pixel/line
    # and georeferenced coordinates for data/VHP.G04.C07.NP.P2018008.VH.tif.
    # There is no affine transformation and no GCPs.
    # varname = VARIABLES[rw_id]
    # sds_path = SDS_NAME.format(fname=ncfile, varname=varname)
    # extracted_var_tif = '{}.tif'.format(os.path.splitext(ncfile)[0])
    # # nodata value -5 equals 251 for Byte type?
    # cmd = ['gdal_translate', '-a_srs', S_SRS, sds_path, extracted_var_tif]
    # logging.debug('Extracting var {} from {} to {}'.format(varname, ncfile, extracted_var_tif))
    # subprocess.call(cmd)

    logging.info('Compressing')
    cmd = ['gdal_translate','-co','COMPRESS=LZW','-of','GTiff',
           extracted_var_tif,
           new_file]
    subprocess.call(cmd)

    if DELETE_LOCAL:
        os.remove(extracted_var_tif)

    logging.info('Reprojected {} to {}'.format(ncfile, new_file))
    return new_file

def _processAssets(tifs, rw_id, varname):
    assets = [getAssetName(tif, rw_id, varname) for tif in tifs]
    dates = [getRasterDate(tif) for tif in tifs]
    # Set date to the end of the reported week,
    # -0 corresponding to Sunday at end of week
    datestamps = [datetime.datetime.strptime(date + '-0', DATE_FORMAT_ISO)
                  for date in dates]
    eeUtil.uploadAssets(tifs, assets, GS_PREFIX.format(rw_id=rw_id, varname=varname), datestamps, timeout=3000)
    return assets

def processAssets(agg, rw_id, tifs):
    agg[rw_id] = _processAssets(tifs[rw_id], rw_id, ASSET_NAMES[rw_id])
    return agg



def deleteLocalFiles(tifs):
    return list(map(os.remove, tifs))

def processNewRasterData(existing_dates_by_id):
    '''fetch, process, upload, and clean new data'''

    # 0. Prep dates
    existing_dates = []
    for rw_id, e_dates in existing_dates_by_id.items():
        existing_dates.extend(e_dates)

    # 1. Determine which years to read from the ftp file
    target_dates = getNewTargetDates(existing_dates) or []
    logging.debug(target_dates)

    # 2. Fetch datafile
    logging.info('Fetching files')
    tifs = defaultdict(list)
    for date in target_dates:
        if date not in existing_dates:
            try:
                nc = fetch(date)
            except:
                logging.error('Could not fetch data for date: {}'.format(date))
                continue

            for rw_id in VARIABLES:
                reproj_file = reproject(nc, rw_id, date)
                tifs[rw_id].append(reproj_file)
            if DELETE_LOCAL:
                os.remove(nc)


    # 3. Upload new files
    logging.info('Uploading files')
    new_assets = reduce(lambda agg, rw_id: processAssets(agg, rw_id, tifs), tifs, {})
    logging.debug('New Assets object: {}'.format(new_assets))

    # 4. Delete local files
    if DELETE_LOCAL:
         deleted_files = list(map(lambda rw_id: deleteLocalFiles(tifs[rw_id]), tifs))

    return new_assets




def checkCreateCollection(collection):
    '''List assests in collection else create new collection'''
    if eeUtil.exists(collection):
        return eeUtil.ls(collection)
    else:
        logging.info('{} does not exist, creating'.format(collection))
        eeUtil.createFolder(collection, imageCollection=True, public=True)
        return []

def deleteExcessAssets(dates, rw_id, varname, max_assets):
    '''Delete assets if too many'''
    # oldest first
    dates.sort()
    logging.debug('ordered dates: {}'.format(dates))
    if len(dates) > max_assets:
        for date in set(dates[:-max_assets]):
            logging.debug('deleting asset from date: {}'.format(date))
            asset_name = os.path.join(EE_COLLECTION.format(rw_id=rw_id,
                                                        varname=varname),
                                        ASSET_NAME.format(rw_id=rw_id,
                                                          varname=varname,
                                                          date=date))
            eeUtil.removeAsset(asset_name)

###
## Application code
###

def main():
    '''Ingest new data into EE and delete old data'''
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    ### 0. Initialize GEE
    eeUtil.initJson()

    ### 1. Create collection names, clear if desired
    collections = {}
    for rw_id, varname in ASSET_NAMES.items():
        collections[rw_id] = EE_COLLECTION.format(rw_id=rw_id,varname=varname)

    if CLEAR_COLLECTION_FIRST:
        for collection in collections.values():
            if eeUtil.exists(collection):
                eeUtil.removeAsset(collection, recursive=True)

    ### 2. Grab existing assets and their dates
    existing_assets = {}
    for rw_id, coll in collections.items():
        existing_assets[rw_id] = checkCreateCollection(coll)

    existing_dates = {}
    for rw_id, ex_assets in existing_assets.items():
        existing_dates[rw_id] = list(map(getRasterDate, ex_assets))

    # This will be a list of objects
    new_assets = processNewRasterData(existing_dates)

    new_dates = {}
    for rw_id, nw_assets in new_assets.items():
        new_dates[rw_id] = list(map(getRasterDate, nw_assets))

    ### 5. Delete old assets
    for rw_id, collection in collections.items():
        e = existing_dates[rw_id]
        n = new_dates[rw_id] if rw_id in new_dates else []
        total = e + n
        logging.info('Existing assets in {}: {}, new: {}, max: {}'.format(
            rw_id, len(e), len(n), MAX_DATES))
        deleteExcessAssets(total,rw_id,ASSET_NAMES[rw_id],MAX_DATES)

    ###

    logging.info('SUCCESS')
