from __future__ import unicode_literals

import os
import sys
import urllib.request
import shutil
from contextlib import closing
import zipfile
import datetime
import logging
import subprocess
import fiona
from collections import OrderedDict
import cartosql
from . import eeUtil


LOG_LEVEL = logging.DEBUG
CLEAR_COLLECTION_FIRST = False
CLEAR_TABLE_FIRST = False
VERSION = '3.0'

RUN_RASTERS = True
RUN_VECTORS = False

# Sources for nrt data
SOURCE_URL_MEASUREMENT = 'ftp://sidads.colorado.edu/DATASETS/NOAA/G02135/{north_or_south}/monthly/geotiff/{month}/{target_file}'
SOURCE_FILENAME_MEASUREMENT = '{N_or_S}_{date}_extent_v{version}.tif'
LOCAL_FILE = 'cli_005_{arctic_or_antarctic}_sea_ice_{date}.tif'
ASSET_NAME = 'cli_005_{arctic_or_antarctic}_sea_ice_{date}'

# Sources for average polylines
SOURCE_URL_MONTHLY_MEDIAN = 'ftp://sidads.colorado.edu/DATASETS/NOAA/G02135/{north_or_south}/monthly/shapefiles/shp_median/{target_file}'
SOURCE_FILENAME_MONTHLY_MEDIAN = 'median_extent_{N_or_S}_{month}_1981-2010_polyline_v{version}'
CARTO_TABLE = 'cli_005_polar_monthly_sea_ice_extent_polylines'
CARTO_SCHEMA = OrderedDict([
        ('the_geom', 'geometry'),
        ('date', 'text'),
        ('_uid', 'text')
    ])
UID_FIELD = '_uid'
TIME_FIELD = 'date'

# For naming and storing assets
DATA_DIR = 'data'
GS_PREFIX = 'cli_005_polar_sea_ice_extent'
EE_COLLECTION = 'cli_005_{arctic_or_antarctic}_sea_ice_extent'

# Times two because of North / South parallels
MAX_DATES = 36
MAX_ASSETS = MAX_DATES*2
DATE_FORMAT = '%Y%m'
TIMESTEP = {'days': 30}

# environmental variables
GEE_SERVICE_ACCOUNT = os.environ.get("GEE_SERVICE_ACCOUNT")
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS")
GEE_STAGING_BUCKET = os.environ.get("GEE_STAGING_BUCKET")
GCS_PROJECT = os.environ.get("CLOUDSDK_CORE_PROJECT")

###
## Handling RASTERS
###

def getAssetName(tif):
    '''get asset name from tif name, extract datetime and location'''
    location = tif.split('_')[4]
    date = getDate(tif)
    return os.path.join(EE_COLLECTION.format(arctic_or_antarctic=location), ASSET_NAME.format(arctic_or_antarctic=location, date=date))

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

def fetch(url, north_or_south, datestring):
    '''Fetch files by datestamp'''
    # New data may not yet be posted
    month = format_month(datestring)
    target_file = SOURCE_FILENAME_MEASUREMENT.format(N_or_S=north_or_south[0].upper(), date=datestring, version=VERSION)
    arctic_or_antarctic = 'arctic' if (north_or_south=='north') else 'antarctic'

    _file = url.format(north_or_south=north_or_south,month=month,target_file=target_file)
    filename = LOCAL_FILE.format(arctic_or_antarctic=arctic_or_antarctic, date=datestring)
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

    logging.debug('Reprojected {} to {}'.format(filename, new_filename))
    return new_filename

def processNewRasterData(existing_dates):
    '''fetch, process, upload, and clean new data'''
    # 1. Determine which years to read from the ftp file
    target_dates = getNewTargetDates(existing_dates) or []
    logging.debug(target_dates)

    # 2. Fetch datafile
    logging.info('Fetching files')
    tifs = []
    for date in target_dates:
        arctic_file = fetch(SOURCE_URL_MEASUREMENT, 'north', date)
        reprojected_arctic = reproject(arctic_file, s_srs='EPSG:3411', extent='-180 50 180 89.75')
        os.remove(os.path.join(DATA_DIR,arctic_file))

        antarctic_file = fetch(SOURCE_URL_MEASUREMENT, 'south', date)
        reprojected_antarctic = reproject(antarctic_file, s_srs='EPSG:3412', extent='-180 -89.75 180 -50')
        os.remove(os.path.join(DATA_DIR,antarctic_file))

        logging.debug('Arctic file: {}, Antarctic file: {}'.format(reprojected_arctic, reprojected_antarctic))
        tifs.append(os.path.join(DATA_DIR,reprojected_arctic))
        tifs.append(os.path.join(DATA_DIR,reprojected_antarctic))

    # 3. Upload new files
    logging.info('Uploading files')
    dates = [getRasterDate(tif) for tif in tifs]
    assets = [getAssetName(tif) for tif in tifs]
    eeUtil.uploadAssets(tifs, assets, GS_PREFIX, dates, dateformat=DATE_FORMAT, public=True, timeout=3000)

    # 4. Delete local files
    for tif in tifs:
        logging.debug(tif)
        os.remove(tif)
        os.remove(tif+'.aux.xml')

    return assets

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

###
## Handling VECTORS
###

def extractShp(zfile, dest):
    with zipfile.ZipFile(zfile) as z:
        shp_name = ''
        for f in z.namelist():
            if os.path.splitext(f)[1] == '.shp':
                shp_name = f
        z.extractall(dest)
    return shp_name

def genUID(arctic_or_antarctic, month, fid):
    return '_'.join([arctic_or_antarctic, month, str(fid)])

### Not needed because of ogr2ogr -wrapdateline option
# def breakGeomAt180(geom):
#     line_coords = geom['coordinates']
#     logging.debug('Number of line segments before: {}'.format(len(line_coords)))
#     new_lines = []
#     for line in line_coords:
#         lons, _ = zip(*line)
#         last_break = 0
#         for i in range(len(lons)-1):
#             lon1 = lons[i]
#             lon2 = lons[i+1]
#             if abs(lon1-lon2) > 350:
#                 new_lines.append(line[last_break:i+1])
#                 last_break=i+1
#         new_lines.append(line[last_break:])
#     geom['coordinates'] = new_lines
#     logging.debug('Number of line segments after: {}'.format(len(new_lines)))
#     return geom

def processNewVectorData(existing_ids):
    months = [str(mon) if len(str(mon))==2 else '0'+str(mon) for mon in range(1,13)]
    total_new_count = 0
    for month in months:
        for a in ['arctic', 'antarctic']:
            north_or_south = 'north' if a=='arctic' else 'south'
            filename = SOURCE_FILENAME_MONTHLY_MEDIAN.format(N_or_S=north_or_south[0].upper(), month=month, version=VERSION)
            tmpfile = '{}.zip'.format(os.path.join(DATA_DIR,filename))

            url = SOURCE_URL_MONTHLY_MEDIAN.format(north_or_south=north_or_south, target_file='{}.zip'.format(filename))

            logging.info('Fetching {} median ice extent for {}'.format(a, month))
            logging.debug('url: {}, filename: {}'.format(url, tmpfile))
            try:
                urllib.request.urlretrieve(url, tmpfile)
                unzipped_folder = os.path.join(DATA_DIR,'unzipped_'+filename)
                shpfile = extractShp(tmpfile, unzipped_folder)

                logging.debug('shapefile name: {}'.format(shpfile))

                if a == 'arctic':
                    s_srs = 'EPSG:3411'
                else:
                    s_srs = 'EPSG:3412'

                original_shapefile = os.path.join(unzipped_folder,shpfile)
                logging.debug('Original shapefile: {}'.format(original_shapefile))
                reprojected_shapefile = os.path.join(DATA_DIR,'reprojected_'+shpfile)
                cmd = ' '.join(['ogr2ogr','-overwrite', '-f', '"ESRI Shapefile"',
                                '-wrapdateline',
                                '-s_srs',s_srs,'-t_srs','EPSG:4326',
                                reprojected_shapefile,original_shapefile,])
                subprocess.check_output(cmd, shell=True)

            except Exception as e:
                logging.warning('Could not retrieve and reproject {}'.format(url))
                logging.error(e)
                continue

            logging.info('Parsing data')

            rows = []
            with fiona.open(reprojected_shapefile, 'r') as shp:

                logging.debug(shp.schema)
                for obs in shp:

                    uid = genUID(a, month, obs['properties']['FID'])
                    if uid not in existing_ids:
                        row = []
                        for field in CARTO_SCHEMA.keys():
                            if field == 'the_geom':
                                #better_geom = breakGeomAt180(obs['geometry'])
                                row.append(obs['geometry'])
                            elif field == UID_FIELD:
                                row.append(uid)
                            elif field == TIME_FIELD:
                                row.append(month)

                        rows.append(row)

            # 3. Delete local files
            os.remove(tmpfile)

            # 4. Insert new observations
            new_count = len(rows)
            total_new_count += new_count
            if new_count:
                logging.info('Pushing new rows')
                cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                                    CARTO_SCHEMA.values(), rows)
    return total_new_count



###
## Carto code
###

def createTableWithIndex(table, schema, id_field, time_field=''):
    '''Get existing ids or create table'''
    cartosql.createTable(table, schema)
    cartosql.createIndex(table, id_field, unique=True)
    if time_field:
        cartosql.createIndex(table, time_field)


def getIds(table, id_field):
    '''get ids from table'''
    r = cartosql.getFields(id_field, table, f='csv')
    return r.text.split('\r\n')[1:-1]


###
## Application code
###

def main():
    '''Ingest new data into EE and delete old data'''
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    # 1. Check collection, fetch, process, stage, ingest, clean
    ### RASTERS
    # Initialize eeUtil
    eeUtil.init(GEE_SERVICE_ACCOUNT, GOOGLE_APPLICATION_CREDENTIALS,
                GCS_PROJECT, GEE_STAGING_BUCKET)

    if RUN_RASTERS:
        if CLEAR_COLLECTION_FIRST:
            eeUtil.removeAsset(EE_COLLECTION.format(arctic_or_antarctic='arctic'), recursive=True)
            eeUtil.removeAsset(EE_COLLECTION.format(arctic_or_antarctic='antarctic'), recursive=True)

        ### RASTERS
        existing_assets = checkCreateCollection(EE_COLLECTION)
        existing_dates = [getRasterDate(a) for a in existing_assets]

        new_assets = processNewRasterData(existing_dates)
        new_dates = [getDate(a) for a in new_assets]

        # 3. Delete old assets
        existing_dates = existing_dates + new_dates
        logging.info('Existing assets: {}, new: {}, max: {}'.format(
            len(existing_dates), len(new_dates), MAX_ASSETS))
        deleteExcessAssets(existing_dates, MAX_ASSETS)

        logging.info('SUCCESS')

    ### VECTORS
    if RUN_VECTORS:
        if CLEAR_TABLE_FIRST:
            cartosql.dropTable(CARTO_TABLE)

        existing_ids = []
        if cartosql.tableExists(CARTO_TABLE):
            logging.info('Fetching existing ids')
            existing_ids = getIds(CARTO_TABLE, UID_FIELD)
        else:
            logging.info('Table {} does not exist, creating'.format(CARTO_TABLE))
            createTableWithIndex(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD)

        num_new_vectors = processNewVectorData(existing_ids)

        existing_count = num_new_vectors + len(existing_ids)
        logging.info('Total rows: {}, New: {}, Max: {}'.format(
            existing_count, num_new_vectors, 'none'))
