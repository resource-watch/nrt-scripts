import os
import logging
import sys
import requests
from collections import OrderedDict
import src.carto
import datetime

### Constants
DATA_DIR='data'
LATEST_URL='https://api.acleddata.com/acled/read?page={}'

### asserting table structure rather than reading from input
CARTO_TABLE = 'soc_016_conflict_protest_events'
CARTO_SCHEMA = OrderedDict([
    ("the_geom","geometry"),
    ("data_id","int"),
    ("event_date", "timestamp"),
    ("year", "int"),
    ("time_precision", "int"),
    ("event_type", "text"),
    ("actor1", "text"),
    ("ally_actor_1", "text"),
    ("inter1", "int"),
    ("actor2", "text"),
    ("ally_actor_2", "text"),
    ("inter2", "int"),
    ("interaction", "int"),
    ("country", "text"),
    ("admin1", "text"),
    ("admin2", "text"),
    ("admin3", "text"),
    ("location", "text"),
    ("geo_precision", "int"),
    ("source", "text"),
    ("notes", "text"),
    ("fatalities", "int"),
])
UID_FIELD = 'data_id'
TIME_FIELD = 'event_date'

CARTO_USER = os.environ.get('CARTO_USER')
CARTO_KEY = os.environ.get('CARTO_KEY')

# Limit 1M rows, drop older than 10yrs
MAXROWS = 1000000
MAXAGE = datetime.datetime.now() - datetime.timedelta(days=3650)

### Generate UID
def genUID(obs):
    return str(obs[UID_FIELD])

def getACLEDRowBlocks(exclude_ids):
    page = 1
    newcount = 1
    # get and parse each page; stop when no new results
    while newcount:
        logging.info("Fetching page {}".format(page))
        r = requests.get(LATEST_URL.format(page))
        data = r.json()
        page += 1

        # parse data excluding existing observations
        rows = []
        for obs in data['data']:
            uid = genUID(obs)
            if uid not in exclude_ids:
                row = []
                for field in CARTO_SCHEMA.keys():
                    if field == 'the_geom':
                        # construct geojson geometry
                        geom = {
                            "type": "Point",
                            "coordinates": [
                                obs['longitude'],
                                obs['latitude']
                            ]
                        }
                        row.append(geom)
                    elif field == UID_FIELD:
                        row.append(uid)
                    else:
                        row.append(obs[field])
                rows.append(row)
        # yield results in blocks
        newcount = len(rows)
        yield rows

# get, parse, insert
def process(exclude_ids):
    newcount = 0
    # iterively fetch parse and post new data
    for row_block in getACLEDRowBlocks(exclude_ids):
        if len(row_block):
            logging.info('Pushing new rows')
            carto.insertRows(CARTO_TABLE, CARTO_SCHEMA, row_block)
            newcount += len(row_block)
    return newcount

##############################################################
# General logic for Carto
# should be the same for most tabular datasets
##############################################################
def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)

    ### 1. Check if table exists and create table
    existing_ids = []
    if not carto.tableExists(CARTO_TABLE):
        logging.info('Table {} does not exist, creating'.format(
            CARTO_TABLE))
        carto.createTable(CARTO_TABLE, CARTO_SCHEMA)
        carto.createIndex(CARTO_TABLE, TIME_FIELD)
    ### 2. Fetch existing IDs from table
    else:
        r = carto.getFields(UID_FIELD, CARTO_TABLE, order='{} desc'.format(TIME_FIELD), f='csv')
        # quick read 1-column csv to list
        existing_ids = r.text.split('\r\n')[1:-1]

    ### 3. Iterively fetch, parse and post new data
    newcount = process(existing_ids)

    ### 4. Remove old observations
    oldcount = len(existing_ids)
    logging.info('Previous rows: {}, New: {}, Max: {}'.format(
        oldcount, newcount, MAXROWS))

    # by max age
    delete_where = "{} < '{}'".format(
        TIME_FIELD, MAXAGE.isoformat())
    # by excess rows
    if oldcount + newcount > MAXROWS:
        drop_ids = existing_ids[min(MAXROWS, MAXROWS - newcount):]
        delete_where = '{} OR {} in ({})'.format(
            delete_where, UID_FIELD, ','.join(drop_ids))

    r = carto.deleteRows(CARTO_TABLE, delete_where)
    numdropped = r.json()['total_rows']
    if numdropped > 0:
        logging.info('Dropped {} old rows'.format(numdropped))

    logging.info('SUCCESS')
