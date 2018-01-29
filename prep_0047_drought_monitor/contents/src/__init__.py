from __future__ import unicode_literals

import fiona
import os
import logging
import sys
import urllib
import datetime
from collections import OrderedDict
import cartosql
import zipfile

# Constants
DATA_DIR = 'data'
SOURCE_URL = 'http://droughtmonitor.unl.edu/data/shapefiles_m/USDM_{date}_M.zip'
FILENAME = 'USDM_{date}'
TIMESTEP = {'days': 1}
DATE_FORMAT = '%Y%m%d'
# Tuesday = 1
WEEKDAY = 1

# asserting table structure rather than reading from input
CARTO_TABLE = 'prep_0047_drought_monitor'
CARTO_SCHEMA = OrderedDict([
    ('the_geom', 'geometry'),
    ('_UID', 'text'),
    ('date', 'timestamp'),
    ('OBJECTID', 'int'),
    ('DM', 'int')
])
UID_FIELD = '_UID'
TIME_FIELD = 'date'

LOG_LEVEL = logging.INFO
MAXROWS = 10000
MAXAGE = datetime.datetime.today() - datetime.timedelta(days=365*10)


# Generate UID
def genUID(obs, date):
    return str('{}_{}'.format(date, obs['properties']['OBJECTID']))


def getDate(uid):
    '''first 8 chr of ID'''
    return uid[:8]


def findShp(zfile):
    with zipfile.ZipFile(zfile) as z:
        for f in z.namelist():
            if os.path.splitext(f)[1] == '.shp':
                return f
    return False


def getNewDates(exclude_dates):
    '''Get new dates excluding existing'''
    new_dates = []
    date = datetime.datetime.today()
    while date > MAXAGE:
        date -= datetime.timedelta(**TIMESTEP)
        if date.weekday() == WEEKDAY:
            datestr = date.strftime(DATE_FORMAT)
            if datestr not in exclude_dates:
                new_dates.append(datestr)
    return new_dates


def processNewData(exclude_ids):
    new_ids = []

    # get non-existing dates
    dates = set([getDate(uid) for uid in exclude_ids])
    new_dates = getNewDates(dates)
    for date in new_dates:
        # 1. Fetch data from source

        url = SOURCE_URL.format(date=date)
        tmpfile = '{}.zip'.format(os.path.join(DATA_DIR,
                                               FILENAME.format(date=date)))
        logging.info('Fetching {}'.format(date))
        try:
            urllib.request.urlretrieve(url, tmpfile)
        except Exception as e:
            logging.warning('Could not retrieve {}'.format(url))
            logging.error(e)
            continue

        # 2. Parse fetched data and generate unique ids
        logging.info('Parsing data')
        shpfile = '/{}'.format(findShp(tmpfile))
        zfile = 'zip://{}'.format(tmpfile)
        rows = []
        with fiona.open(shpfile, 'r', vfs=zfile) as shp:
            logging.debug(shp.schema)
            for obs in shp:
                uid = genUID(obs, date)
                new_ids.append(uid)
                row = []
                for field in CARTO_SCHEMA.keys():
                    if field == 'the_geom':
                        row.append(obs['geometry'])
                    elif field == UID_FIELD:
                        row.append(uid)
                    elif field == TIME_FIELD:
                        row.append(date)
                    else:
                        row.append(obs['properties'][field])
                rows.append(row)
        # 3. Delete local files
        os.remove(tmpfile)

        # 4. Insert new observations
        new_count = len(rows)
        if new_count:
            logging.info('Pushing new rows')
            cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                                CARTO_SCHEMA.values(), rows)

    return new_ids


##############################################################
# General logic for Carto
# should be the same for most tabular datasets
##############################################################

def checkCreateTable(table, schema, id_field, time_field):
    '''Get existing ids or create table'''
    if cartosql.tableExists(table):
        logging.info('Fetching existing IDs')
        r = cartosql.getFields(id_field, table, f='csv')
        return r.text.split('\r\n')[1:-1]
    else:
        logging.info('Table {} does not exist, creating'.format(table))
        cartosql.createTable(table, schema)
        cartosql.createIndex(table, id_field, unique=True)
        cartosql.createIndex(table, time_field)
    return []


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
    r = cartosql.getFields('cartodb_id', table, order='{}'.format(time_field),
                           f='csv')
    ids = r.text.split('\r\n')[1:-1]

    # 3. delete excess
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[:-max_rows])
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))


def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    # 1. Check if table exists and create table
    existing_ids = checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD,
                                    TIME_FIELD)

    # 2. Iterively fetch, parse and post new data
    new_ids = processNewData(existing_ids)

    new_count = len(new_ids)
    existing_count = new_count + len(existing_ids)
    logging.info('Total rows: {}, New: {}, Max: {}'.format(
        existing_count, new_count, MAXROWS))

    # 3. Remove old observations
    deleteExcessRows(CARTO_TABLE, MAXROWS, TIME_FIELD, MAXAGE)

    logging.info('SUCCESS')
