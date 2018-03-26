from __future__ import unicode_literals

import os
import sys
import urllib
from datetime import datetime, timedelta
import logging
import subprocess
import eeUtil
import rasterio
from rasterio import features
from rasterio.crs import CRS
from affine import Affine
import geopandas as gpd
import numpy as np

LOG_LEVEL = logging.INFO
CLEAR_COLLECTION_FIRST = False

SOURCE_URL = 'https://floodmap.modaps.eosdis.nasa.gov/Products/{tile}/{year}/{file}'
FILE_TEMPLATE = '{short_product}_{date}_{tile}_3D3OT_V.zip'
FILENAME = 'wat_038_{product}_{date}'
GS_FOLDER = 'wat_038_{product}'
EE_COLLECTION = 'wat_038_{product}'

PRODUCTS = {
    'MSW':'modis_surface_water',
    'MFW':'modis_flood_water'
}
DATA_DIR = 'data'
MOSAICS_DIR = 'mosaics'

# Year, day since Jan 1
DATE_FORMAT = '%Y%j'
MAX_ASSETS = 30
TIMESTEP = {'days': 1}

# Tile structure
TILE = '{lon}{card_lon}{lat}{card_lat}'
XSIZE = 4552
YSIZE = 4552
XBOUND = 18
YBOUND = 7

### Tile management ###

def cast_as_days_since_jan(num):
    if num > 0:
        return datetime.strptime(str(num), '%j').strftime('%j')
    else:
        return '000'

def generate_tiles():
    tiles = []
    # 50S - 70N, 180W - 170E

    for i in range(XBOUND):
        # Hack - use days since jan to format tiles
        east = cast_as_days_since_jan(i*10)
        west = cast_as_days_since_jan((i+1)*10)
        for y in range(YBOUND):
            north = cast_as_days_since_jan((y+1)*10)
            south = cast_as_days_since_jan(y*10)

            tiles.append(TILE.format(lon=east,card_lon='E',lat=north,card_lat='N'))
            tiles.append(TILE.format(lon=east,card_lon='E',lat=south,card_lat='S'))
            tiles.append(TILE.format(lon=west,card_lon='W',lat=north,card_lat='N'))
            tiles.append(TILE.format(lon=west,card_lon='W',lat=south,card_lat='S'))

    return tiles

def calc_bounds(zipname):
    tile = zipname.split('_')[2]
    lon = int(tile[:3])
    lat = int(tile[4:7])
    if 'E' in tile:
        if 'N' in tile:
            return lat, lon
        elif 'S' in tile:
            return -lat, lon

    elif 'W' in tile:
        if 'N' in tile:
            return lat, -lon
        elif 'S' in tile:
            return -lat, -lon

### URL locations and file / asset names ###

def getUrl(product, date, tile):
    '''get source url from datestamp'''
    year = datetime.strptime(date, DATE_FORMAT).strftime('%Y')
    f = FILE_TEMPLATE.format(short_product=product, date=date, tile=tile)
    return SOURCE_URL.format(year=year, tile=tile, file=f)

def getFileName(product, date, tile):
    '''get asset name from datestamp'''
    return os.path.join(DATA_DIR, FILE_TEMPLATE.format(short_product=product, date=date, tile=tile))

def getAssetName(product, date):
    '''get asset name from datestamp'''
    return os.path.join(EE_COLLECTION.format(product=PRODUCTS[product]),
                FILENAME.format(product=product,date=date))

def getDate(filename):
    '''get last 7 chrs of filename'''
    return os.path.splitext(os.path.basename(filename))[0][-7:]

### Fetching and processing data ###

def fetch(product, date, tile):
    '''Fetch files by datestamp'''

    url = getUrl(product, date, tile)
    filename = getFileName(product, date, tile)
    logging.debug('Fetching {}'.format(url))
    # New data may not yet be posted
    try:
        urllib.request.urlretrieve(url, filename)
        return filename
    except Exception as e:
        logging.warning('Could not fetch {}'.format(url))
        logging.debug(e)
        return None

def rasterize(zipname):
    pwd = os.getcwd()
    try:
        shp = gpd.read_file('zip:///{}'.format(os.path.join(pwd,zipname)))
    except:
        logging.error('No valid shapefile in {}'.format(zipname))
        return None

    logging.debug('GDF: {}'.format(shp.head(5)))
    tifname = '{}.tif'.format(os.path.splitext(zipname)[0])

    # Transformation function
    top_left_lat, top_left_lon = calc_bounds(zipname)

    row_width = 10./YSIZE
    column_height = -10./XSIZE
    row_rotation = 0
    column_rotation = 0

    transform = Affine(row_width,row_rotation,top_left_lon,
                        column_rotation, column_height, top_left_lat)
    # Profile
    # Rasterio uses numpy's data types anyway, so this is OK
    dtype = np.uint8
    profile = {
        'driver':'GTiff',
        'height':YSIZE,
        'width':XSIZE,
        'count':1,
        'dtype':dtype,
        'crs':CRS({'init':'epsg:4326'}),
        'transform':transform,
        'compress':'lzw',
        'nodata':0
    }

    ###
    ## DRAWBACK of this approach - no ability to set NODATA value
    ## for cloud - obstructed regions where no valid surface water
    ## predictions can be made
    ###

    with rasterio.open(tifname, 'w', **profile) as dst:
        # this is where we create a generator of geom, value pairs to use in rasterizing
        shapes = ((geom,1) for geom in shp.geometry)

        ### all_touched = True was resulting in a freeze, only for some polygons!
        # See shapes[5] for 'MFW_2018082_000E010N_3D3OT_V.zip'
        # The shapes turn out to be equivalent w/ or w/out all_touched=True
        burned = features.rasterize(shapes=shapes,
                                    out_shape=(XSIZE, YSIZE),
                                    transform=transform,
                                    all_touched=False,
                                    dtype = dtype,
                                    fill = 0)
        dst.write(burned.astype(dtype), indexes=1)

    return tifname

def merge(product, date):
    '''convert bleaching alert ncs to tifs'''
    mosaic_name = os.path.join(MOSAICS_DIR, '{}.tif'.format(FILENAME.format(product=product, date=date)))

    # 1. Build mosaic vrt
    vrtname = os.path.join(DATA_DIR,'mosaic_{}.vrt'.format(date))
    cmd = ' '.join(['gdalbuildvrt', vrtname, '{}/*.tif'.format(DATA_DIR)])
    logging.debug('Building vrt mosaic for date {}'.format(date))
    subprocess.call(cmd, shell=True)

    # 2. Complete and compress mosaic
    cmd = ' '.join(['gdal_translate', '-of', 'GTiff',
           '-co', 'COMPRESS=LZW', vrtname, mosaic_name])
    logging.debug('Merging and compressing {}'.format(mosaic_name))
    subprocess.call(cmd, shell=True)

    # 3. Remove intermediate tifs to make room for more :)
    os.chdir(DATA_DIR)
    cmd = ' '.join(['rm', '-r', '$(ls)'])
    subprocess.call(cmd, shell=True)
    os.chdir('..')
    logging.debug('DATA_DIR contents: {}'.format(os.listdir(DATA_DIR)))

    return mosaic_name

### Orchestrating the process ###

def getNewDates(exclude_dates):
    '''Get new dates excluding existing'''
    new_dates = []
    date = datetime.today()
    for i in range(MAX_ASSETS):
        date -= timedelta(**TIMESTEP)
        datestr = date.strftime(DATE_FORMAT)
        if datestr not in exclude_dates:
            new_dates.append(datestr)
    return new_dates

def processNewData(product, existing_dates):
    '''fetch, process, upload, and clean new data'''
    # 1. Tiles to fetch
    tiles = generate_tiles()

    # 2. Determine which dates to fetch
    new_dates = getNewDates(existing_dates)

    # 3. Fetch and format new mosaics
    logging.info('Fetching files')
    mosaic_names = list(map(lambda d: processDate(product, d, tiles), new_dates))

    if mosaic_names:
        # 4. Upload new files
        logging.info('Uploading files')
        dates = [getDate(m) for m in mosaic_names]
        datestamps = [datetime.strptime(date, DATE_FORMAT)
                      for date in dates]
        assets = [getAssetName(product, date) for date in dates]

        ## Need to set a long timeout - default of 300 seconds fails
        eeUtil.uploadAssets(mosaic_names, assets,
                            GS_FOLDER.format(product=PRODUCTS[product]),
                            datestamps,
                            timeout = 30000)

        return assets

    return []

def exists(item):
    return True if item else False

def processDate(product, date, tiles):
    zips = list(map(lambda t: fetch(product, date, t), tiles))
    zips = list(filter(exists, zips))
    logging.debug('ZIPS: {}'.format(zips))
    tifs = list(map(rasterize, zips))
    tifs = list(filter(exists, tifs))
    return merge(product, date)

### Creating and cleaning up ImageCollections ###

def checkCreateCollection(collection):
    '''List assests in collection else create new collection'''
    if eeUtil.exists(collection):
        return eeUtil.ls(collection)
    else:
        logging.info('{} does not exist, creating'.format(collection))
        eeUtil.createFolder(collection, True, public=True)
        return []


def deleteExcessAssets(product, dates, max_assets):
    '''Delete assets if too many'''
    # oldest first
    dates.sort()
    if len(dates) > max_assets:
        for date in dates[:-max_assets]:
            eeUtil.removeAsset(getAssetName(product, date))

###
## All together now!
###

def main():
    '''Ingest new data into EE and delete old data'''
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    # Initialize eeUtil
    eeUtil.initJson()

    # 1. Check if collection exists and create
    for product in PRODUCTS:
        collection = EE_COLLECTION.format(product=PRODUCTS[product])
        if CLEAR_COLLECTION_FIRST:
            if eeUtil.exists(collection):
                eeUtil.removeAsset(collection, recursive=True)

        existing_assets = checkCreateCollection(collection)
        existing_dates = [getDate(a) for a in existing_assets]

        # 2. Fetch, process, stage, ingest, clean
        new_assets = processNewData(product, existing_dates)
        new_dates = [getDate(a) for a in new_assets]

        # 3. Delete old assets
        existing_dates = existing_dates + new_dates
        logging.info('Existing assets: {}, new: {}, max: {}'.format(
            len(existing_dates), len(new_dates), MAX_ASSETS))
        deleteExcessAssets(product, existing_dates, MAX_ASSETS)

    logging.info('SUCCESS')
