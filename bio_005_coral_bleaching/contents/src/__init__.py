from __future__ import unicode_literals

import os
import sys
import urllib
import datetime
import logging
import subprocess
import eeUtil

# constants for bleaching alerts
SOURCE_URL = 'ftp://ftp.star.nesdis.noaa.gov/pub/sod/mecb/crw/data/5km/v3/nc/v1/composite/daily/7day/baa-max/{year}/b5km_baa-max-7d_{date}.nc'
SDS_NAME = 'NETCDF:"{fname}":CRW_BAA_max7d'
FILENAME = 'bio_005_{date}'
NODATA_VALUE = None

DATA_DIR = 'data'
GS_FOLDER = 'bio_005_bleaching_alerts'
EE_COLLECTION = 'bio_005_bleaching_alerts'

MAX_ASSETS = 61
DATE_FORMAT = '%Y%m%d'
TIMESTEP = {'days': 1}


def getUrl(date):
    '''get source url from datestamp'''
    return SOURCE_URL.format(year=date[:4], date=date)


def getAssetName(date):
    '''get asset name from datestamp'''
    return os.path.join(EE_COLLECTION, FILENAME.format(date=date))


def getFilename(date):
    '''get filename from datestamp'''
    return os.path.join(DATA_DIR, '{}.nc'.format(
        FILENAME.format(date=date)))


def getDate(filename):
    '''get last 8 chrs of filename'''
    return os.path.splitext(os.path.basename(filename))[0][-8:]


def getNewDates(exclude_dates):
    '''Get new dates excluding existing'''
    new_dates = []
    date = datetime.date.today()
    for i in range(MAX_ASSETS):
        date -= datetime.timedelta(**TIMESTEP)
        datestr = date.strftime(DATE_FORMAT)
        if datestr not in exclude_dates:
            new_dates.append(datestr)
    return new_dates


def convert(files):
    '''convert bleaching alert ncs to tifs'''
    tifs = []
    for f in files:
        # extract subdataset by name
        sds_path = SDS_NAME.format(fname=f)
        tif = '{}.tif'.format(os.path.splitext(f)[0])
        # nodata value -5 equals 251 for Byte type?
        cmd = ['gdal_translate', '-q', '-a_nodata', '251', sds_path, tif]
        logging.debug('Converting {} to {}'.format(f, tif))
        subprocess.call(cmd)
        tifs.append(tif)
    return tifs


def fetch(dates):
    '''Fetch files by datestamp'''
    files = []
    for date in dates:
        url = getUrl(date)
        f = getFilename(date)
        logging.debug('Fetching {}'.format(url))
        # New data may not yet be posted
        try:
            urllib.request.urlretrieve(url, f)
            files.append(f)
        except Exception as e:
            logging.warning('Could not fetch {}'.format(url))
            logging.debug(e)
    return files


def processNewData(existing_dates):
    '''fetch, process, upload, and clean new data'''
    # 1. Determine which files to fetch
    new_dates = getNewDates(existing_dates)

    # 2. Fetch new files
    logging.info('Fetching files')
    files = fetch(new_dates)

    if files:
        # 3. Convert new files
        logging.info('Converting files')
        tifs = convert(files)

        # 4. Upload new files
        logging.info('Uploading files')
        dates = [getDate(tif) for tif in tifs]
        datestamps = [datetime.datetime.strptime(date, DATE_FORMAT)
                      for date in dates]
        assets = [getAssetName(date) for date in dates]
        eeUtil.uploadAssets(tifs, assets, GS_FOLDER, datestamps)

        # 5. Delete local files
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
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Initialize eeUtil
    eeUtil.initJson()

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
