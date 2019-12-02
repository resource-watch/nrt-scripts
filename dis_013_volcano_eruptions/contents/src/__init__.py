import logging
import sys
import os
from collections import OrderedDict
import cartosql
import lxml
from xmljson import parker as xml2json
import requests
import datetime

### Constants
SOURCE_URL = "http://webservices.volcano.si.edu/geoserver/GVP-VOTW/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=GVP-VOTW:Smithsonian_VOTW_Holocene_Eruptions"

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


LOG_LEVEL = logging.INFO
CLEAR_TABLE_FIRST = False

### Table name and structure
CARTO_TABLE = 'dis_013_volcano_eruptions'
CARTO_SCHEMA_UPPER = OrderedDict([
    ("the_geom", "geometry"),
    ("Volcano_Number", "numeric"),
    ("Volcano_Name", "text"),
    ("Eruption_Number", "numeric"),
    ("Activity_Type", "text"),
    ("ActivityArea", "text"),
    ("ActivityUnit", "text"),
    ("ExplosivityIndexMax", "numeric"),
    ("ExplosivityIndexModifier", "text"),
    ("StartEvidenceMethod", "text"),
    ("StartDateYearModifier", "text"),
    ("StartDateYear", "numeric"),
    ("StartDateYearUncertainty", "numeric"),
    ("StartDateMonth", "numeric"),
    ("StartDateDayModifier", "text"),
    ("StartDateDay", "numeric"),
    ("StartDateDayUncertainty", "numeric"),
    ("EndDateYearModifier", "text"),
    ("EndDateYear", "numeric"),
    ("EndDateYearUncertainty", "numeric"),
    ("EndDateMonth", "numeric"),
    ("EndDateDayModifier", "text"),
    ("EndDateDay", "numeric"),
    ("EndDateDayUncertainty", "numeric")
])
CARTO_SCHEMA = OrderedDict([(key.lower(), value) for key,value in CARTO_SCHEMA_UPPER.items()])

UID_FIELD = 'Eruption_Number'
AGE_FIELD = 'StartDateYear'
# Table limits
MAX_ROWS = 1000000

DATASET_ID = 'f2016c79-82f7-466e-b4db-2c734dd5706d'
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
## Carto code
###

def checkCreateTable(table, schema, id_field, time_field=''):
    '''Get existing ids or create table'''
    if cartosql.tableExists(table):
        logging.info('Fetching existing IDs')
        r = cartosql.getFields(id_field, table, f='csv', post=True)
        return r.text.split('\r\n')[1:-1]
    else:
        logging.info('Table {} does not exist, creating'.format(table))
        cartosql.createTable(table, schema)
        cartosql.createIndex(table, id_field, unique=True)
        if time_field:
            cartosql.createIndex(table, time_field)
    return []


def deleteExcessRows(table, max_rows, age_field):
    '''Delete excess rows by age or count'''
    num_dropped = 0

    # get sorted ids (old->new)
    r = cartosql.getFields('cartodb_id', table, order='{}'.format(age_field.lower()),
                           f='csv')
    ids = r.text.split('\r\n')[1:-1]

    #  delete excess
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[:-max_rows])
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))
    return num_dropped

def processData(existing_ids):
    new_data = []
    new_ids = []

    r = requests.get(SOURCE_URL)
    xml = lxml.etree.fromstring(r.content)
    json = xml2json.data(xml)
    data_dict = list(json.values())[1]

    for entry in data_dict:
        data = entry['{volcano.si.edu}Smithsonian_VOTW_Holocene_Eruptions']
        uid = data['{volcano.si.edu}'+UID_FIELD]
        if str(uid) not in existing_ids + new_ids:
            new_ids.append(uid)
            row = []
            for key in CARTO_SCHEMA_UPPER.keys():
                source_key = '{volcano.si.edu}'+key
                try:
                    if key == 'the_geom':
                        source_key = '{volcano.si.edu}GeoLocation'
                        coords=data[source_key]['{http://www.opengis.net/gml}Point']['{http://www.opengis.net/gml}coordinates'].split(',')
                        #coords are provided as lon, lat
                        lon = coords[0]
                        lat = coords[1]
                        item = {
                            'type': 'Point',
                            'coordinates': [lon, lat]
                        }
                    else:
                        item = data[source_key]
                except KeyError:
                    item=None
                row.append(item)
            new_data.append(row)
    num_new = len(new_ids)
    if num_new:
        logging.info('Adding {} new records'.format(num_new))
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), new_data)
    return(num_new)

def get_most_recent_date(table):
    r = cartosql.getFields('pubdate', table, f='csv', post=True)
    dates = r.text.split('\r\n')[1:-1]
    dates.sort()
    most_recent_date = datetime.datetime.strptime(dates[-1], '%Y-%m-%d %H:%M:%S')
    return most_recent_date


def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    if CLEAR_TABLE_FIRST:
        logging.info('Clearing Table')
        if cartosql.tableExists(CARTO_TABLE):
            cartosql.deleteRows(CARTO_TABLE, 'cartodb_id IS NOT NULL', user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))

    ### 1. Check if table exists, if not, create it
    logging.info('Checking if table exists and getting existing IDs.')
    existing_ids = checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD.lower())
    num_existing = len(existing_ids)

    ### 2. Fetch data from FTP, dedupe, process
    logging.info('Fetching new data')
    num_new = processData(existing_ids)

    ### 3. Delete data to get back to MAX_ROWS
    logging.info('Deleting excess rows')
    num_dropped = deleteExcessRows(CARTO_TABLE, MAX_ROWS, AGE_FIELD)

    ### 4. Notify results
    total = num_existing + num_new - num_dropped

    # If updates, change update date on RW
    if num_new>0:
        lastUpdateDate(DATASET_ID, datetime.datetime.utcnow())

    logging.info('Existing rows: {},  New rows: {}, Max: {}'.format(total, num_new, MAX_ROWS))
    logging.info("SUCCESS")
