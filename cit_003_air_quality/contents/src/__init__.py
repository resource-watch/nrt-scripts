import os
import logging
import sys
import requests
from collections import OrderedDict
import src.carto

### Constants
DATA_DIR = 'data'
LATEST_URL='https://api.openaq.org/v1/latest?limit=10000&has_geo=true'
HISTORY_URL='https://openaq-data.s3.amazonaws.com/'

### asserting table structure rather than reading from input
CARTO_TABLE = 'cit_003_air_quality'
CARTO_SCHEMA = OrderedDict([
    ("the_geom","geometry"),
    ("_UID","text"),
    ("lastUpdated","timestamp"),
    ("value","float"),
    ("parameter","text"),
    ("location","text"),
    ("city","text"),
    ("country","text"),
    ("unit","text"),
    ("sourceName","text")
])
UID_FIELD = '_UID'
TIME_FIELD = 'lastUpdated'

CARTO_USER = os.environ.get('CARTO_USER')
CARTO_KEY = os.environ.get('CARTO_KEY')

MAXROWS = 9000000 #Carto limit at 10M?

### Generate UID
def genUID(loc, measure):
    # location should be unique, plus measurement timestamp
    return '{}_{}_{}'.format(loc['location'], measure['parameter'], measure['lastUpdated'])

### Read openAQ and returnse list of insertable rows
def parseOpenAQ(data, fields, exclude_ids):
    rows=[]
    new_ids=[]
    # iterate locations
    for loc in data['results']:
        # construct geojson geometry
        geom = {
            "type": "Point",
            "coordinates": [
                loc['coordinates']['longitude'],
                loc['coordinates']['latitude']
            ]
        }
        # iterate measurements within locations
        for measure in loc['measurements']:
            uid = genUID(loc, measure)
            if uid not in exclude_ids and uid not in new_ids:
                new_ids.append(uid)
                row = []
                for field in fields:
                    if field == 'the_geom':
                        row.append(geom)
                    elif field == UID_FIELD:
                        row.append(uid)
                    elif field in ('location', 'city', 'country'):
                        row.append(loc[field])
                    else:
                        row.append(measure[field])
                rows.append(row)
    return rows

### Fetch and parse openAQ
def fetchData(existing_ids):
    r = requests.get(LATEST_URL)
    data = r.json()
    # Parse fetched data and generate unique ids
    logging.info('Parsing data')
    return parseOpenAQ(data, CARTO_SCHEMA.keys(), existing_ids)

### Main
def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)

    ### 1. Check if table exists and create table
    dest_ids = []
    if not carto.tableExists(CARTO_TABLE):
        logging.info('Table {} does not exist, creating'.format(CARTO_TABLE))
        carto.createTable(CARTO_TABLE, CARTO_SCHEMA)

    ### 2. Fetch existing IDs from table
    else:
        r = carto.getFields(UID_FIELD, CARTO_TABLE, order=TIME_FIELD, f='csv')
        # quick read 1-column csv to list
        dest_ids = r.split('\r\n')[1:-1]

    ### 3. Fetch data and parse
    logging.info('Fetching latest data')
    rows = fetchData(dest_ids)

    ### 5. Insert new observations
    if len(rows):
        carto.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA, rows)

    ### 6. Remove old observations
    logging.info('Previous count: {}, New: {}, Max: {}'.format(len(dest_ids), len(rows), MAXROWS))
    if len(dest_ids) + len(rows) > MAXROWS and MAXROWS > len(rows):
        drop_ids = dest_ids[(MAXROWS - len(rows)):]
        carto.deleteRowsByIDs(CARTO_TABLE, "_UID", drop_ids)

    logging.info('SUCCESS')
