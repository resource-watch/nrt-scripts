import os
import logging
import sys
import urllib
import zipfile
import datetime
from dateutil.relativedelta import relativedelta

import fiona
from collections import OrderedDict
from shapely import geometry
import cartosql
import requests

# Constants
DATA_DIR = './data'
SOURCE_URL = 'http://shapefiles.fews.net.s3.amazonaws.com/HFIC/{region}/{target_file}'
REGIONS = {'WA':'west-africa{date}.zip',
            'CA':'central-asia{date}.zip',
            'EA':'east-africa{date}.zip',
            'LAC':'caribbean-central-america{date}.zip',
            'SA':'southern-africa{date}.zip'}

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
MAXROWS = 1000000
MINDATES = 6
MAXAGE = datetime.datetime.today() - datetime.timedelta(days=365*5)
DATASET_ID = 'ac6dcdb3-2beb-4c66-9f83-565c16c2c914'
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


# Generate UID
def genUID(date, region, ifc_type, pos_in_shp):
    '''ifc_type can be:
    CS = "current status",
    ML1 = "most likely status in next four months"
    ML2 = "most likely status in following four months"
    '''
    return str('{}_{}_{}_{}'.format(date, region, ifc_type, pos_in_shp))


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
    if len(files) != 3:
        logging.info('There should be 3 shapefiles: CS, ML1, ML2')
    return files


def simplifyGeom(geom):
    # Simplify complex polygons
    shp = geometry.shape(geom)
    simp = shp.simplify(SIMPLIFICATION_TOLERANCE, PRESERVE_TOPOLOGY)
    return geometry.mapping(simp)


def processNewData(exclude_ids):
    new_ids = []

    # Truncate date to monthly resolution
    date = datetime.datetime.strptime(
        datetime.datetime.today().strftime(DATE_FORMAT), DATE_FORMAT)

    # 1. Fetch data from source
    # iterate backwards 1 month at a time
    while date > MAXAGE:
        date -= relativedelta(months=1)
        datestr = date.strftime(DATE_FORMAT)
        rows = []

        logging.info('Fetching data for {}'.format(datestr))
        for region, fileTemplate in REGIONS.items():
            filename = fileTemplate.format(date=datestr)
            url = SOURCE_URL.format(region=region, target_file=filename)
            tmpfile = os.path.join(DATA_DIR, filename)

            try:
                urllib.request.urlretrieve(url, tmpfile)
            except Exception as e:
                logging.info('Could not retrieve {}'.format(url))
                # skip dates that don't work
                continue

            # 2. Parse fetched data and generate unique ids
            logging.info('Parsing data for {}'.format(region))
            shpfiles = findShps(tmpfile)
            for ifc_type, shpfile in shpfiles.items():
                shpfile = '/{}'.format(shpfile)
                zfile = 'zip://{}'.format(tmpfile)

                start_date = date
                end_date = date
                if ifc_type == 'ML1':
                    end_date = date + relativedelta(months=4)
                elif ifc_type == 'ML2':
                    start_date = date + relativedelta(months=4)
                    end_date = date + relativedelta(months=8)

                with fiona.open(shpfile, 'r', vfs=zfile) as shp:
                    logging.debug('Schema: {}'.format(shp.schema))
                    pos_in_shp = 0
                    for obs in shp:
                        uid = genUID(datestr, region, ifc_type, pos_in_shp)
                        if uid not in exclude_ids and uid not in new_ids:
                            new_ids.append(uid)
                            row = []
                            for field in CARTO_SCHEMA.keys():
                                if field == 'the_geom':
                                    row.append(simplifyGeom(obs['geometry']))
                                elif field == UID_FIELD:
                                    row.append(uid)
                                elif field == 'ifc_type':
                                    row.append(ifc_type)
                                elif field == 'ifc':
                                    row.append(obs['properties'][ifc_type])
                                elif field == 'start_date':
                                    row.append(start_date.strftime(DATETIME_FORMAT))
                                elif field == 'end_date':
                                    row.append(end_date.strftime(DATETIME_FORMAT))
                            rows.append(row)
                            pos_in_shp += 1

            # 4. Delete local files
            os.remove(tmpfile)

        # 3. Insert new observations
        new_count = len(rows)
        if new_count:
            logging.info('Pushing {} new rows: {} for {}'.format(
                new_count, ifc_type, date))
            cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                                CARTO_SCHEMA.values(), rows)
        elif date < datetime.datetime.today() - relativedelta(months=MINDATES):
            break

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
    if isinstance(max_age, datetime.datetime):
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

def get_most_recent_date(table):
    #get only check times for current state (CS) because dates associated with projections are
    #in the future and don't make sense to list as our most recent update date
    r = cartosql.getFields(TIME_FIELD, table, where="ifc_type LIKE 'CS'", f='csv', post=True)
    dates = r.text.split('\r\n')[1:-1]
    dates.sort()
    most_recent_date = datetime.datetime.strptime(dates[-1], '%Y-%m-%d %H:%M:%S')
    return most_recent_date

def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    if CLEAR_TABLE_FIRST:
        if cartosql.tableExists(CARTO_TABLE):
            logging.info("Clearing table")
            cartosql.deleteRows(CARTO_TABLE, 'cartodb_id IS NOT NULL', user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))

    # 1. Check if table exists and create table
    existing_ids = []
    if cartosql.tableExists(CARTO_TABLE):
        existing_ids = getFieldAsList(CARTO_TABLE, UID_FIELD)
    else:
        createTableWithIndices(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)

    # 2. Iterively fetch, parse and post new data
    num_new = processNewData(existing_ids)

    existing_count = num_new + len(existing_ids)
    logging.info('Total rows: {}, New: {}, Max: {}'.format(existing_count, num_new, MAXROWS))

    # 3. Remove old observations
    deleteExcessRows(CARTO_TABLE, MAXROWS, TIME_FIELD, MAXAGE)

    # Get most recent update date
    most_recent_date = get_most_recent_date(CARTO_TABLE)
    lastUpdateDate(DATASET_ID, most_recent_date)

    ###
    logging.info('SUCCESS')
