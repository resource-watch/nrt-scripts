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
import requests

LOG_LEVEL = logging.INFO
CLEAR_COLLECTION_FIRST = False
VERSION = '3.0'

# Sources for nrt data
SOURCE_URL_MEASUREMENT = 'ftp://sidads.colorado.edu/DATASETS/NOAA/G02135/{north_or_south}/monthly/geotiff/{month}/{target_file}'
SOURCE_FILENAME_MEASUREMENT = '{N_or_S}_{date}_extent_v{version}.tif'
LOCAL_FILE = 'cli_005_{arctic_or_antarctic}_sea_ice_{date}.tif'

EE_COLLECTION = 'cli_005_{arctic_or_antarctic}_sea_ice_extent_{orig_or_reproj}'
ASSET_NAME = 'cli_005_{arctic_or_antarctic}_sea_ice_{date}'

# For naming and storing assets
DATA_DIR = 'data'
GS_PREFIX = 'cli_005_polar_sea_ice_extent'

# Times two because of North / South parallels
MAX_DATES = 12
DATE_FORMAT = '%Y%m'
TIMESTEP = {'days': 30}

# environmental variables
GEE_SERVICE_ACCOUNT = os.environ.get("GEE_SERVICE_ACCOUNT")
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS")
GEE_STAGING_BUCKET = os.environ.get("GEE_STAGING_BUCKET")
GCS_PROJECT = os.environ.get("CLOUDSDK_CORE_PROJECT")
DATASET_ID = {
    'cli_005_antarctic_sea_ice_extent_reproj':'e740efec-c673-431a-be2c-b214613f641a',
    'cli_005_arctic_sea_ice_extent_reproj': '484fbba1-ac34-402f-8623-7b1cc9c34f17',
}

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
###
## Handling RASTERS
###

def getAssetName(tif, orig_or_reproj, arctic_or_antarctic=''):
    '''get asset name from tif name, extract datetime and location'''
    if len(arctic_or_antarctic):
        location = arctic_or_antarctic
    else:
        if orig_or_reproj=='orig':
            location = tif.split('_')[2]
        else:
            location = tif.split('_')[4]

    date = getRasterDate(tif)
    return os.path.join(EE_COLLECTION.format(arctic_or_antarctic=location,
                                                orig_or_reproj=orig_or_reproj),
                        ASSET_NAME.format(arctic_or_antarctic=location,
                                            date=date))

def getRasterDate(filename):
    '''get last 8 chrs of filename'''
    return os.path.splitext(os.path.basename(filename))[0][-6:]

def getNewTargetDates(exclude_dates):
    '''Get new dates excluding existing'''
    new_dates = []
    date = datetime.date.today()
    date.replace(day=15)
    for i in range(MAX_DATES):
        date -= datetime.timedelta(**TIMESTEP)
        date.replace(day=15)
        datestr = date.strftime(DATE_FORMAT)
        if datestr not in exclude_dates:
            new_dates.append(datestr)
    return new_dates

def format_month(datestring):
    month = datestring[-2:]
    names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    name = names[int(month)-1]
    return('_'.join([month, name]))

def fetch(url, arctic_or_antarctic, datestring):
    '''Fetch files by datestamp'''
    # New data may not yet be posted
    month = format_month(datestring)
    north_or_south = 'north' if (arctic_or_antarctic=='arctic') else 'south'

    target_file = SOURCE_FILENAME_MEASUREMENT.format(N_or_S=north_or_south[0].upper(), date=datestring, version=VERSION)
    _file = url.format(north_or_south=north_or_south,month=month,target_file=target_file)
    filename = LOCAL_FILE.format(arctic_or_antarctic=arctic_or_antarctic,date=datestring)
    try:
        with closing(urllib.request.urlopen(_file)) as r:
            with open(os.path.join(DATA_DIR, filename), 'wb') as f:
                shutil.copyfileobj(r, f)
                logging.debug('Copied: {}'.format(_file))
    except Exception as e:
        logging.warning('Could not fetch {}'.format(_file))
        logging.error(e)
    return filename

def reproject(filename, s_srs='EPSG:4326', extent='-180 -89.75 180 89.75'):
    tmp_filename = ''.join(['reprojected_',filename])
    cmd = ' '.join(['gdalwarp','-overwrite','-s_srs',s_srs,'-t_srs','EPSG:4326',
                    '-te',extent,'-multi','-wo','NUM_THREADS=val/ALL_CPUS',
                    os.path.join(DATA_DIR, filename),
                    os.path.join(DATA_DIR, tmp_filename)])
    subprocess.check_output(cmd, shell=True)

    new_filename = ''.join(['compressed_reprojected_',filename])
    cmd = ' '.join(['gdal_translate','-co','COMPRESS=LZW','-stats',
                    os.path.join(DATA_DIR, tmp_filename),
                    os.path.join(DATA_DIR, new_filename)])
    subprocess.check_output(cmd, shell=True)
    os.remove(os.path.join(DATA_DIR, tmp_filename))
    os.remove(os.path.join(DATA_DIR, tmp_filename+'.aux.xml'))

    logging.debug('Reprojected {} to {}'.format(filename, new_filename))
    return new_filename

def processNewRasterData(existing_dates, arctic_or_antarctic):
    '''fetch, process, upload, and clean new data'''
    # 1. Determine which years to read from the ftp file
    target_dates = getNewTargetDates(existing_dates) or []
    logging.debug(target_dates)

    # 2. Fetch datafile
    logging.info('Fetching {} files'.format(arctic_or_antarctic))
    orig_tifs = []
    reproj_tifs = []

    if arctic_or_antarctic == 'arctic':
        s_srs = 'EPSG:3411'
        extent = '-180 50 180 89.75'
    else:
        s_srs = 'EPSG:3412'
        extent = '-180 -89.75 180 -50'

    for date in target_dates:
        if date not in existing_dates:
            orig_file = fetch(SOURCE_URL_MEASUREMENT, arctic_or_antarctic, date)
            reproj_file = reproject(orig_file, s_srs=s_srs, extent=extent)
            orig_tifs.append(os.path.join(DATA_DIR, orig_file))
            reproj_tifs.append(os.path.join(DATA_DIR, reproj_file))
            logging.debug('New files: orig {}, reproj {}'.format(orig_file, reproj_file))

    # 3. Upload new files
    logging.info('Uploading {} files'.format(arctic_or_antarctic))

    orig_assets = [getAssetName(tif, 'orig') for tif in orig_tifs]
    reproj_assets = [getAssetName(tif, 'reproj') for tif in reproj_tifs]

    dates = [getRasterDate(tif) for tif in reproj_tifs]
    datestamps = [datetime.datetime.strptime(date, DATE_FORMAT)  # list comprehension/for loop
                  for date in dates]  # returns list of datetime object
    eeUtil.uploadAssets(orig_tifs, orig_assets, GS_PREFIX, datestamps, timeout=3000)
    eeUtil.uploadAssets(reproj_tifs, reproj_assets, GS_PREFIX, datestamps, timeout=3000)

    # 4. Delete local files
    for tif in orig_tifs:
        logging.debug('Deleting: {}'.format(tif))
        os.remove(tif)
    for tif in reproj_tifs:
        logging.debug('Deleting: {}'.format(tif))
        os.remove(tif)

    return orig_assets, reproj_assets

def checkCreateCollection(collection):
    '''List assests in collection else create new collection'''
    if eeUtil.exists(collection):
        return eeUtil.ls(collection)
    else:
        logging.info('{} does not exist, creating'.format(collection))
        eeUtil.createFolder(collection, imageCollection=True, public=True)
        return []

def deleteExcessAssets(dates, orig_or_reproj, arctic_or_antarctic, max_assets):
    '''Delete assets if too many'''
    # oldest first
    dates.sort()
    logging.debug('ordered dates: {}'.format(dates))
    if len(dates) > max_assets:
        for date in dates[:-max_assets]:
            eeUtil.removeAsset(getAssetName(date, orig_or_reproj, arctic_or_antarctic=arctic_or_antarctic))

###
## Application code
###

def get_most_recent_date(collection):
    existing_assets = checkCreateCollection(collection)  # make image collection if doesn't have one
    existing_dates = [getRasterDate(a) for a in existing_assets]
    existing_dates.sort()
    most_recent_date = datetime.datetime.strptime(existing_dates[-1], DATE_FORMAT)
    return most_recent_date


def main():
    '''Ingest new data into EE and delete old data'''
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    ### 1. Initialize eeUtil
    eeUtil.initJson()

    ### 2. Create collection names, clear if desired
    arctic_collection_orig = EE_COLLECTION.format(arctic_or_antarctic='arctic', orig_or_reproj='orig')
    arctic_collection_reproj = EE_COLLECTION.format(arctic_or_antarctic='arctic', orig_or_reproj='reproj')
    antarctic_collection_orig = EE_COLLECTION.format(arctic_or_antarctic='antarctic', orig_or_reproj='orig')
    antarctic_collection_reproj = EE_COLLECTION.format(arctic_or_antarctic='antarctic', orig_or_reproj='reproj')

    collections = [arctic_collection_orig,arctic_collection_reproj,
                    antarctic_collection_orig,antarctic_collection_reproj]

    if CLEAR_COLLECTION_FIRST:
        for collection in collections:
            if eeUtil.exists(collection):
                eeUtil.removeAsset(collection, recursive=True)

    ### 3. Process arctic data
    arctic_data = collections[0:2]
    arctic_assets_orig = checkCreateCollection(arctic_data[0])
    arctic_assets_reproj = checkCreateCollection(arctic_data[1])
    arctic_dates_orig = [getRasterDate(a) for a in arctic_assets_orig]
    arctic_dates_reproj = [getRasterDate(a) for a in arctic_assets_reproj]

    new_arctic_assets_orig, new_arctic_assets_reproj = processNewRasterData(arctic_dates_orig, 'arctic')
    new_arctic_dates_orig = [getRasterDate(a) for a in new_arctic_assets_orig]
    new_arctic_dates_reproj = [getRasterDate(a) for a in new_arctic_assets_reproj]

    ### 4. Process antarctic data
    antarctic_data = collections[2:]
    antarctic_assets_orig = checkCreateCollection(antarctic_data[0])
    antarctic_assets_reproj = checkCreateCollection(antarctic_data[1])
    antarctic_dates_orig = [getRasterDate(a) for a in antarctic_assets_orig]
    antarctic_dates_reproj = [getRasterDate(a) for a in antarctic_assets_reproj]

    new_antarctic_assets_orig, new_antarctic_assets_reproj  = processNewRasterData(antarctic_dates_orig, 'antarctic')
    new_antarctic_dates_orig = [getRasterDate(a) for a in new_antarctic_assets_orig]
    new_antarctic_dates_reproj = [getRasterDate(a) for a in new_antarctic_assets_reproj]

    ### 5. Delete old assets
    e_dates = [arctic_dates_orig, arctic_dates_reproj,
                     antarctic_dates_orig, antarctic_dates_reproj]
    n_dates = [new_arctic_dates_orig, new_arctic_dates_reproj,
                new_antarctic_dates_orig, new_antarctic_dates_reproj]

    for i in range(4):
        orig_or_reproj = 'orig' if i%2==0 else 'reproj'
        arctic_or_antarctic = 'arctic' if i < 2 else 'antarctic'
        e = e_dates[i]
        n = n_dates[i]
        total = e + n

        logging.info('Existing {} {} assets: {}, new: {}, max: {}'.format(
            orig_or_reproj, arctic_or_antarctic, len(e), len(n), MAX_DATES))
        deleteExcessAssets(total,orig_or_reproj,arctic_or_antarctic,MAX_DATES)

    ###
    for dataset, id in DATASET_ID.items():
        # Get most recent update date
        most_recent_date = get_most_recent_date(dataset)
        lastUpdateDate(id, most_recent_date)

    logging.info('SUCCESS')
