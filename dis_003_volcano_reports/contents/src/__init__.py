import logging
import sys
import os
import time
import requests as req
from collections import OrderedDict
from datetime import datetime, timedelta
import cartosql
import lxml
from xmljson import parker as xml2json
from dateutil import parser

### Constants
SOURCE_URL = "http://volcano.si.edu/news/WeeklyVolcanoRSS.xml"

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
SIGNIFICANT_THRESHOLD = 600

LOG_LEVEL = logging.INFO
CLEAR_TABLE_FIRST = True

### Table name and structure
CARTO_TABLE = 'dis_003_volcano_reports'
CARTO_SCHEMA = OrderedDict([
    ('uid', 'text'),
    ('the_geom', 'geometry'),
    ('pubdate', 'timestamp'),
    ('volcano_name', 'text'),
    ('country_name', 'text'),
    ('description', 'text'),
    ('sources', 'text')
])
UID_FIELD = 'uid'
TIME_FIELD = 'pubdate'

# Table limits
MAX_ROWS = 1000000
MAX_AGE = datetime.today() - timedelta(days=365*5)

###
## Carto code
###

def checkCreateTable(table, schema, id_field, time_field):
    '''
    Get existing ids or create table
    Return a list of existing ids in time order
    '''
    if cartosql.tableExists(table):
        logging.info('Table {} already exists'.format(table))
    else:
        logging.info('Creating Table {}'.format(table))
        cartosql.createTable(table, schema)
        cartosql.createIndex(table, id_field, unique=True)
        if id_field != time_field:
            cartosql.createIndex(table, time_field)

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
    r = cartosql.getFields('cartodb_id', table, order='{}'.format(time_field),
                           f='csv')
    ids = r.text.split('\r\n')[1:-1]

    # 3. delete excess
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[:-max_rows])
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))
    return num_dropped

###
## Accessing remote data
###

def genUID(lat,lon,dt):
    return '{}_{}_{}'.format(lat,lon,dt)

def processData(existing_ids):
    """
    Inputs: FTP SOURCE_URL and filename where data is stored, existing_ids not to duplicate
    Actions: Retrives data, dedupes and formats it, and adds to Carto table
    Output: Number of new rows added
    """
    new_data = []
    new_ids = []

    res = req.get(SOURCE_URL)
    xml = lxml.etree.fromstring(res.content)
    json = xml2json.data(xml)
    items = json['channel']['item']

    for item in items:
        title = item['title'].split(')')[0].split('(')
        place_info = [place.strip() for place in title]
        volcano_name = place_info[0]
        country_name = place_info[1]

        coords = item['{http://www.georss.org/georss}point'].split(' ')
        dt = parser.parse(item['pubDate'], fuzzy=True).strftime(DATETIME_FORMAT)

        lat = coords[0]
        lon = coords[1]
        geom = {
            'type':'Point',
            'coordinates':[lon,lat]
        }

        info = item['description'].split('Source:')
        if len(info) < 2:
            info = item['description'].split('Sources:')

        description_text = [text.replace('<p>','').replace('</p>','') for text in info]
        description = description_text[0]
        sources = description_text[1]

        _uid = genUID(lat,lon,dt)
        if _uid not in existing_ids + new_ids:
            new_ids.append(_uid)
            row = []
            for field in CARTO_SCHEMA:
                if field == 'uid':
                    row.append(_uid)
                elif field == 'the_geom':
                    row.append(geom)
                elif field == 'pubdate':
                    row.append(dt)
                elif field == 'description':
                    row.append(description)
                elif field == 'sources':
                    row.append(sources)
                elif field == 'volcano_name':
                    row.append(volcano_name)
                elif field == 'country_name':
                    row.append(country_name)

            new_data.append(row)

    num_new = len(new_ids)
    if num_new:
        logging.info('Adding {} new records'.format(num_new))
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), new_data)

    return(num_new)

###
## Application code
###

def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)

    if CLEAR_TABLE_FIRST:
        if cartosql.tableExists(CARTO_TABLE):
            cartosql.dropTable(CARTO_TABLE)

    ### 1. Check if table exists, if not, create it
    checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)

    ### 2. Retrieve existing data
    r = cartosql.getFields(UID_FIELD, CARTO_TABLE, order='{} desc'.format(TIME_FIELD), f='csv')
    existing_ids = r.text.split('\r\n')[1:-1]
    num_existing = len(existing_ids)

    ### 3. Fetch data from FTP, dedupe, process
    num_new = processData(existing_ids)

    ### 4. Delete data to get back to MAX_ROWS
    num_dropped = deleteExcessRows(CARTO_TABLE, MAX_ROWS, TIME_FIELD, MAX_AGE)

    ### 5. Notify results
    total = num_existing + num_new - num_dropped
    logging.info('Existing rows: {},  New rows: {}, Max: {}'.format(total, num_new, MAX_ROWS))
    logging.info("SUCCESS")
