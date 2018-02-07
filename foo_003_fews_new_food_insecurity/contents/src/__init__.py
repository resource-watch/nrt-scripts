from __future__ import unicode_literals

import os
import logging
import sys
import urllib
import zipfile
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import requests as req
import fiona
from collections import OrderedDict
from shapely.geometry import mapping, Polygon, MultiPolygon
import cartosql

from src.update_layers import update_layers

# Constants
DATA_DIR = 'data'
SOURCE_URL = 'http://shapefiles.fews.net.s3.amazonaws.com/HFIC/{region}/{target_file}'
REGIONS = {'WA':'west-africa{date}.zip',
            'CA':'central-asia{date}.zip',
            'EA':'east-africa{date}.zip',
            'LAC':'caribbean-central-america{date}.zip',
            'SA':'southern-africa{date}.zip'}

TIMESTEP = {'days': 30}
DATE_FORMAT = '%Y%m'
DATETIME_FORMAT = '%Y%m%dT00:00:00Z'
CLEAR_TABLE_FIRST = False
SIMPLIFICATION_TOLERANCE = .04
PRESERVE_TOPOLOGY = True

# asserting table structure rather than reading from input
CARTO_TABLE = 'foo_003_fews_net_food_insecurity'
CARTO_SCHEMA = OrderedDict([
    ('the_geom', 'geometry'),
    ('_uid', 'text'),
    ('start_date', 'timestamp'),
    ('end_date', 'timestamp'),
    ('ifc_type', 'text'),
    ('ifc', 'numeric')
])
TIME_FIELD = 'start_date'
UID_FIELD = '_uid'

LOG_LEVEL = logging.INFO
MAXROWS = 10000
MAXAGE = datetime.today() - timedelta(days=365*3)

# Generate UID
def genUID(date, region, ifc_type, pos_in_shp):
    '''ifc_type can be:
    CS = "current status",
    ML1 = "most likely status in next four months"
    ML2 = "most likely status in following four months"
    '''
    return str('{}_{}_{}_{}'.format(date,region,ifc_type,pos_in_shp))

def getDate(uid):
    '''first component of ID'''
    return uid.split('_')[0]

def formatStartAndEndDates(date, plus=0):
    dt = datetime.strptime(date, DATE_FORMAT) + relativedelta(months=plus)
    return(dt.strftime(DATETIME_FORMAT))

def findShps(zfile):
    files = {}
    with zipfile.ZipFile(zfile) as z:
        for f in z.namelist():
            if os.path.splitext(f)[1] == '.shp':
                if 'CS' in f:
                    files['CS'] = f
                elif 'ML1' in f:
                    files['ML1'] = f
                elif 'ML2' in f:
                    files['ML2'] = f
    if len(files)!=3:
        logging.error('There should be 3 shapefiles: CS, ML1, ML2')
    return files

def potentialNewDates(exclude_dates):
    '''Get new dates excluding existing'''
    new_dates = []
    date = datetime.today()
    while date > MAXAGE:
        datestr = date.strftime(DATE_FORMAT)
        if datestr not in exclude_dates:
            logging.debug('Will fetch data for {}'.format(datestr))
            new_dates.append(datestr)
        else:
            logging.debug('Data for {} already in table'.format(datestr))
        date -= timedelta(**TIMESTEP)
    return new_dates

def simpleGeom(geom):
    # Simplify complex polygons
    # https://gis.stackexchange.com/questions/83084/shapely-multipolygon-construction-will-not-accept-the-entire-set-of-polygons
    if geom['type'] == 'MultiPolygon':
        multi = []
        for polycoords in geom['coordinates']:
            multi.append(Polygon(polycoords[0]))
        geo = MultiPolygon(multi)
    else:
        geo = Polygon(geom['coordinates'][0])

    logging.debug('Length orig WKT: {}'.format(len(geo.wkt)))
    simple_geo = geo.simplify(SIMPLIFICATION_TOLERANCE, PRESERVE_TOPOLOGY)
    logging.debug('Length simple WKT: {}'.format(len(simple_geo.wkt)))
    geojson = mapping(simple_geo)

    return geojson

def processNewData(exclude_dates):
    new_ids = []
    # get non-existing dates
    new_dates = potentialNewDates(exclude_dates)
    for date in new_dates:
        # 1. Fetch data from source
        for region, filename in REGIONS.items():
            _file = filename.format(date=date)
            url = SOURCE_URL.format(region=region,target_file=_file)
            tmpfile = os.path.join(DATA_DIR,_file)
            logging.info('Fetching data for {} in {}'.format(region,date))
            try:
                urllib.request.urlretrieve(url, tmpfile)
            except Exception as e:
                logging.warning('Could not retrieve {}'.format(url))
                logging.error(e)
                continue

            # 2. Parse fetched data and generate unique ids
            logging.info('Parsing data')
            shpfiles = findShps(tmpfile)
            for ifc_type, shpfile in shpfiles.items():
                shpfile = '/{}'.format(shpfile)
                zfile = 'zip://{}'.format(tmpfile)
                rows = []

                if ifc_type == 'CS':
                    start_date = formatStartAndEndDates(date)
                    end_date = formatStartAndEndDates(date)
                elif ifc_type == 'ML1':
                    start_date = formatStartAndEndDates(date)
                    end_date = formatStartAndEndDates(date,plus=4)
                elif ifc_type == 'ML2':
                    start_date = formatStartAndEndDates(date,plus=4)
                    end_date = formatStartAndEndDates(date,plus=8)

                with fiona.open(shpfile, 'r', vfs=zfile) as shp:
                    logging.debug('Schema: {}'.format(shp.schema))
                    pos_in_shp = 0
                    for obs in shp:
                        uid = genUID(date, region, ifc_type, pos_in_shp)
                        ### Received an error due to attempting to load same UID twice.
                        # If happens again, to reproduce, set CLEAR_TABLE_FIRST=True and run again.
                        new_ids.append(uid)
                        row = []
                        for field in CARTO_SCHEMA.keys():
                            if field == 'the_geom':
                                row.append(simpleGeom(obs['geometry']))
                            elif field == UID_FIELD:
                                row.append(uid)
                            elif field == 'ifc_type':
                                row.append(ifc_type)
                            elif field == 'ifc':
                                row.append(obs['properties'][ifc_type])
                            elif field == 'start_date':
                                row.append(start_date)
                            elif field == 'end_date':
                                row.append(end_date)
                        rows.append(row)
                        pos_in_shp += 1

                # 3. Insert new observations
                new_count = len(rows)
                if new_count:
                    logging.info('Pushing {} new rows: {} for {} in {}'.format(new_count,ifc_type,region,date))
                    cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                                        CARTO_SCHEMA.values(), rows)

            # 4. Delete local files
            os.remove(tmpfile)

    num_new = len(new_ids)
    return num_new


##############################################################
# General logic for Carto
# should be the same for most tabular datasets
##############################################################

def createTableWithIndices(table, schema, idField, timeField):
    '''Get existing ids or create table'''
    cartosql.createTable(table, schema)
    cartosql.createIndex(table, idField, unique=True)
    if timeField != idField:
        cartosql.createIndex(table, timeField, unique=False)

def getFieldAsList(table, field, orderBy=''):
    assert isinstance(field, str), 'Field must be a single string'
    r = cartosql.getFields(field, table, order='{}'.format(orderBy),
                           f='csv')
    return(r.text.split('\r\n')[1:-1])

def deleteExcessRows(table, max_rows, time_field, max_age=''):
    '''Delete excess rows by age or count'''
    num_dropped = 0
    if isinstance(max_age, datetime):
        max_age = max_age.isoformat()

    # 1. delete by age
    if max_age:
        r = cartosql.deleteRows(table, "{} < '{}'".format(time_field, max_age))
        num_dropped = r.json()['total_rows']

    # 2. get sorted ids (old->new)
    ids = getFieldAsList(CARTO_TABLE, 'cartodb_id', orderBy=''.format(TIME_FIELD))

    # 3. delete excess
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[:-max_rows])
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))


def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    if CLEAR_TABLE_FIRST:
        if cartosql.tableExists(CARTO_TABLE):
            logging.info("Clearing table")
            cartosql.dropTable(CARTO_TABLE)

    # 1. Check if table exists and create table
    existing_ids = []
    if cartosql.tableExists(CARTO_TABLE):
        existing_ids = getFieldAsList(CARTO_TABLE, UID_FIELD)
    else:
        createTableWithIndices(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)

    # 2. Iterively fetch, parse and post new data
    existing_dates = [getDate(_id) for _id in existing_ids]
    num_new = processNewData(existing_dates)

    existing_count = num_new + len(existing_dates)
    logging.info('Total rows: {}, New: {}, Max: {}'.format(
        existing_count, num_new, MAXROWS))

    # 3. Remove old observations
    deleteExcessRows(CARTO_TABLE, MAXROWS, TIME_FIELD, MAXAGE)

    # 4. Update layer definitions
    update_layers()

    ###

    logging.info('SUCCESS')
