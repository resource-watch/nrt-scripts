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
import numpy as np

LOG_LEVEL = logging.DEBUG
CLEAR_COLLECTION_FIRST = True
VERSION = '3.0'

# Sources for nrt data
SOURCE_URL = 'ftp://ftp.star.nesdis.noaa.gov/pub/corp/scsb/wguo/data/VHP_4km/VH/{target_file}'
SOURCE_FILENAME = 'VHP.G04.C07.NP.P{date}.VH.nc'
# SDS_NAME = 'NETCDF:"{fname}":{varname}'

VARIABLES = {
    'foo_025':'VHI',
    'foo_051':'VCI'
}

ASSETS = {
    'foo_025':'vegetation_health_index',
    'foo_051':'vegetation_condition_index',
}

EE_COLLECTION = '{rw_id}_{varname}'
ASSET_NAME = '{rw_id}_{varname}_{date}'

# For naming and storing assets
DATA_DIR = 'data'
GS_PREFIX = '{rw_id}_{varname}'

MAX_DATES = 36
DATE_FORMAT = '%Y0%V'
TIMESTEP = {'days':7}
S_SRS = 'EPSG:32662'
EXTENT = '-180 -89.75 180 89.75'
DTYPE = rio.float32
NODATA = -999

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
    var = VARIABLES[rw_id]

    var_tif = '{}_{}.tif'.format(os.path.splitext(nc_file)[0], var)
    logging.info('New tif: {}'.format(var_tif))

    # Extract data
    data = nc[var][:,:]
    logging.info('Type of data: {}'.format(type(data)))
    logging.debug('Shape: {}'.format(data.shape))

    # Transformation function
    transform = rio.transform.from_bounds(*[float(pos) for pos in EXTENT.split(' ')], data.shape[1], data.shape[0])

    # Not a clear view of data
    logging.info('Data: {}'.format(data))

    # Max value of each row or column
    # By row
    rowsums = np.sum(data, axis=1)
    # By column
    colsums = np.sum(data, axis=0)

    logging.info('Sorted rowsums: {}'.format(sorted(rowsums)))
    logging.info('Sorted colsums: {}'.format(sorted(colsums)))

    # Profile
    profile = {
        'driver':'GTiff',
        'height':data.shape[0],
        'width':data.shape[1],
        'count':1,
        'dtype':DTYPE,
        'crs':'EPSG:4326',
        'transform':transform,
        'compress':'lzw',
        'nodata':NODATA
    }

    with rio.open(var_tif, 'w', **profile) as dst:
        dst.write(data.astype(DTYPE), indexes=1)

    del nc
    return var_tif

def reproject(ncfile, var):

    new_file = '{}_reprojected_compressed.tif'.format(os.path.splitext(ncfile)[0])
    extracted_var_tif = extract_subdata(ncfile, var)

    logging.info('Reprojecting')
    reprojected_tif = 'reprojected.tif'
    cmd = ' '.join(['gdalwarp','-overwrite','-s_srs',S_SRS,'-t_srs','EPSG:4326',
                    '-te',EXTENT,'-multi','-wo','NUM_THREADS=val/ALL_CPUS',
                    extracted_var_tif,
                    reprojected_tif])
    subprocess.check_output(cmd, shell=True)

    logging.info('Compressing')
    cmd = ' '.join(['gdal_translate','-co','COMPRESS=LZW',
                    reprojected_tif,
                    new_file])
    subprocess.check_output(cmd, shell=True)

    os.remove(extracted_var_tif)
    os.remove(reprojected_tif)
    os.remove(reprojected_tif+'.aux.xml')

    logging.debug('Reprojected {} to {}'.format(ncfile, new_file))
    return new_file


def makeTifListsObj(agg, elem):
    agg[elem] = []
    return agg

def _processAssets(tifs, varname):
    assets = [getAssetName(tif, var, varname) for tif in tifs]
    dates = [getRasterDate(tif) for tif in tifs]
    eeUtil.uploadAssets(tifs, assets, GS_PREFIX.format(rw_id=var,varname=varname), dates, dateformat=DATE_FORMAT, public=True, timeout=3000)

def processAssets(agg, var, tifs):
    agg[elem] = _processAssets(tifs[var], ASSETS[var])
    return agg

def deleteLocalFiles(tifs):
    return list(map(os.remove, tifs))

def processNewRasterData(existing_dates):
    '''fetch, process, upload, and clean new data'''
    # 1. Determine which years to read from the ftp file
    target_dates = getNewTargetDates(existing_dates) or []
    logging.debug(target_dates)

    # 2. Fetch datafile
    logging.info('Fetching files')
    tifs = reduce(makeTifListsObj, VARIABLES.keys(), {})
    for date in target_dates:
        if date not in existing_dates:
            try:
                nc = fetch(date)
            except:
                logging.error('Could not fetch data for date: {}'.format(date))
                continue

            for var in VARIABLES:
                reproj_file = reproject(nc, var)
                tifs[var].append(reproj_file)


    # 3. Upload new files
    logging.info('Uploading files')
    assets = reduce(lambda agg, elem: processAssets(agg, elem, tifs), tifs, {})

    # 4. Delete local files
    deleted_files = list(map(lambda var: deleteLocalFiles(tifs[var]), tifs))

    return assets

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
    for rw_id, varname in ASSETS.items():
        collections[rw_id] = EE_COLLECTION.format(rw_id=rw_id,varname=varname)

    if CLEAR_COLLECTION_FIRST:
        for collection in collections.values():
            if eeUtil.exists(collection):
                eeUtil.removeAsset(collection, recursive=True)

    ### 2. Grab existing assets and their dates
    existing_assets = {}
    for rw_id, coll in collections.items():
        existing_assets[rw_id] = checkCreateCollection(coll)

    existing_dates = []
    for rw_id, ex_assets in existing_assets.items():
        existing_dates.extend(list(map(getRasterDate, ex_assets)))

    # This will be a list of objects
    new_assets = processNewRasterData(existing_dates)

    new_dates = {}
    for rw_id, nw_assets in new_assets.items():
        new_dates[rw_id] = list(map(getRasterDate, nw_assets))

    ### 5. Delete old assets
    for rw_id, collection in collections.items():
        e = existing_dates[rw_id]
        n = new_dates[rw_id]
        total = e + n
        logging.info('Existing assets in {}: {}, new: {}, max: {}'.format(
            rw_id, len(e), len(n), MAX_DATES))
        deleteExcessAssets(total,MAX_DATES)

    ###

    logging.info('SUCCESS')
