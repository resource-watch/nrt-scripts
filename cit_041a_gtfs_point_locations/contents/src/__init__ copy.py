from __future__ import unicode_literals
import logging
import sys
import os
from collections import OrderedDict
import datetime
#import cartosql
import pandas as pd
import urllib.request
import requests
#from shapely.geometry import Point
import datetime

'''
Utility library for interacting with CARTO via the SQL API

Example:
```
import cartosql

# CARTO_USER and CARTO_KEY read from environment if not specified
r = cartosql.get('select * from mytable', user=CARTO_USER, key=CARTO_KEY)

data = r.json()
```

Read more at:
http://carto.com/docs/carto-engine/sql-api/making-calls/
'''

from builtins import str
import requests
import os
import logging
import json

CARTO_URL = 'https://{}.carto.com/api/v2/sql'
CARTO_USER = os.environ.get('CARTO_USER')
CARTO_KEY = os.environ.get('CARTO_KEY')
STRICT = True

def sendSql(sql, user=CARTO_USER, key=CARTO_KEY, f='', post=True):
    '''Send arbitrary sql and return response object or False'''
    url = CARTO_URL.format(user)
    
    sql = sql.replace("''","'")
    print('SQL query = {}'.format(sql))
    payload = {
        'api_key': key,
        'q': sql,
    }
    if len(f):
        payload['format'] = f
    logging.debug((url, payload))
    print(url, payload)
    if post:
        r = requests.post(url, json=payload)
    else:
        r = requests.get(url, params=payload)
    if not r.ok:
        print(r.status_code)
        logging.error(r.text)
        if STRICT:
            raise Exception(r.text)
        return False
    return r


def get(sql, user=CARTO_USER, key=CARTO_KEY, f=''):
    '''Send arbitrary sql and return response object or False'''
    return sendSql(sql, user, key, f, False)


def post(sql, user=CARTO_USER, key=CARTO_KEY, f=''):
    '''Send arbitrary sql and return response object or False'''
    return sendSql(sql, user, key, f)


def getFields(fields, table, where='', order='', user=CARTO_USER,
              key=CARTO_KEY, f='', post=False):
    '''Select fields from table'''
    fields = (fields,) if isinstance(fields, str) else fields
    where = ' WHERE {}'.format(where) if where else ''
    order = ' ORDER BY {}'.format(order) if order else ''
    sql = 'SELECT {} FROM "{}" {} {}'.format(
        ','.join(fields), table, where, order)
    return sendSql(sql, user, key, f, post)


def getTables(user=CARTO_USER, key=CARTO_KEY, f='csv'):
    '''Get the list of tables'''
    r = get('SELECT * FROM CDB_UserTables()',user, key, f)
    if f == 'csv':
        return r.text.split("\r\n")[1:-1]
    return r


def tableExists(table, user=CARTO_USER, key=CARTO_KEY):
    '''Check if table exists'''
    return table in getTables(user, key)


def createTable(table, schema, user=CARTO_USER, key=CARTO_KEY):
    '''
    Create table with schema and CartoDBfy table

    `schema` should be a dict or list of tuple pairs with
     - keys as field names and
     - values as field types
    '''
    items = schema.items() if isinstance(schema, dict) else schema
    defslist = ['{} {}'.format(k, v) for k, v in items]
    sql = 'CREATE TABLE "{}" ({})'.format(table, ','.join(defslist))
    if post(sql, user, key):
        return _cdbfyTable(table, user, key)
    return False


def _cdbfyTable(table, user=CARTO_USER, key=CARTO_KEY):
    '''CartoDBfy table so that it appears in Carto UI'''
    sql = "SELECT cdb_cartodbfytable('{}','\"{}\"')".format(user, table)
    return post(sql, user, key)


def createIndex(table, fields, unique='', using='', user=CARTO_USER,
                key=CARTO_KEY):
    '''Create index on table on field(s)'''
    fields = (fields,) if isinstance(fields, str) else fields
    f_underscore = '_'.join(fields)
    f_comma = ','.join(fields)
    unique = 'UNIQUE' if unique else ''
    using = 'USING {}'.format(using) if using else ''
    sql = 'CREATE {} INDEX idx_{}_{} ON {} {} ({})'.format(
        unique, table, f_underscore, table, using, f_comma)
    return post(sql, user, key)


def _escapeValue(value, dtype):
    '''
    Escape value for SQL based on field type

    TYPE         Escaped
    None      -> NULL
    geometry  -> string as is; obj dumped as GeoJSON
    text      -> single quote escaped
    timestamp -> single quote escaped
    varchar   -> single quote escaped
    else      -> as is
    '''
    if value is None:
        return "NULL"
    if dtype == 'geometry':
        # if not string assume GeoJSON and assert WKID
        if isinstance(value, str):
            return value
        else:
            value = json.dumps(value)
            return "ST_SetSRID(ST_GeomFromGeoJSON('{}'),4326)".format(value)
    elif dtype in ('text', 'timestamp', 'varchar'):
        # quote strings, escape quotes, and drop nbsp
        return "'{}'".format(
            str(value).replace("'", "''"))
    else:
        return str(value)


def _dumpRows(rows, dtypes):
    '''Escapes rows of data to SQL strings'''
    dumpedRows = []
    for row in rows:
        escaped = [
            _escapeValue(row[i], dtypes[i])
            for i in range(len(dtypes))
        ]
        dumpedRows.append('({})'.format(','.join(escaped)))
    return ','.join(dumpedRows)


def _insertRows(table, fields, dtypes, rows, user=CARTO_USER, key=CARTO_KEY):
    values = _dumpRows(rows, tuple(dtypes))
    sql = 'INSERT INTO "{}" ({}) VALUES {}'.format(
        table, ', '.join(fields), values)
    return post(sql, user, key)


def insertRows(table, fields, dtypes, rows, user=CARTO_USER,
               key=CARTO_KEY, blocksize=1000):
    '''
    Insert rows into table

    `rows` must be a list of lists containing the data to be inserted
    `fields` field names for the columns in `rows`
    `dtypes` field types for the columns in `rows`

    Automatically breaks into multiple requests at `blocksize` rows
    '''
    # iterate in blocks
    while len(rows):
        print(table, fields, dtypes, rows[:blocksize], user, key)
        if not _insertRows(table, fields, dtypes, rows[:blocksize], user, key):
            return False
        rows = rows[blocksize:]
    return True

# Alias insertRows
blockInsertRows = insertRows


def deleteRows(table, where, user=CARTO_USER, key=CARTO_KEY):
    '''Delete rows from table'''
    sql = 'DELETE FROM "{}" WHERE {}'.format(table, where)
    return post(sql,user, key)


def deleteRowsByIDs(table, ids, id_field='cartodb_id', dtype='',
                    user=CARTO_USER, key=CARTO_KEY):
    '''Delete rows from table by IDs'''
    if dtype:
        ids = [_escapeValue(i, dtype) for i in ids]
    where = '{} in ({})'.format(id_field, ','.join(ids))
    return deleteRows(table, where, user, key)


def dropTable(table, user=CARTO_USER, key=CARTO_KEY):
    '''Delete table'''
    sql = 'DROP TABLE "{}"'.format(table)
    return post(sql, user, key)

def truncateTable(table, user=CARTO_USER, key=CARTO_KEY):
    '''Delete table'''
    sql = 'TRUNCATE TABLE "{}"'.format(table)
    return post(sql,user, key)

if __name__ == '__main__':
    from . import cli
    cli.main()


#import boto3
#import gzip
print('Helloooo')
### Constants
LOG_LEVEL = logging.INFO
DATA_DIR = 'data'
DATA_LOCATION_URL = 'https://api.transitfeeds.com/v1/getLocations?key=258e3d67-9c2e-46db-9484-001ce6ff3cc7'
DATA_URL = 'https://api.transitfeeds.com/v1/getFeeds?key=258e3d67-9c2e-46db-9484-001ce6ff3cc7&location={}'

#Useful links:
#http://transitfeeds.com/api/
#https://developers.google.com/transit/gtfs/reference/#pathwaystxt


#Filename for local files
FILENAME = 'gtfs_points'

# asserting table structure rather than reading from input
CARTO_TABLE= 'cit_041_gtfs'
CARTO_SCHEMA = OrderedDict([
    ('the_geom','geometry'),
    ('id', 'numeric'),
    ('feed_type','text'),
    ('feed_title','text'),
    ('id2','numeric'),
    ('pid','numeric'),
    ('loc_title_t','text'),
    ('loc_title_n','text'),
    ('latitude','numeric'),
    ('longitude','numeric'),
    ('timestamp_epoch','numeric'),
    ('timestamp','timestamp'),
    ('info_url','text'),
    ('download_url','text')
])
'id','feed_type','feed_title','id2','pid','loc_title_t','loc_title_n','latitude','longitude','timestamp','info_url','download_url'
CLEAR_TABLE_FIRST = True
INPUT_DATE_FORMAT = '%Y%m%d'
DATE_FORMAT = '%Y-%m-%d'
TIME_FIELD = 'UTC_Scan_time'
MAX_TRIES = 8


###
## Accessing remote data
###

DATASET_ID = '41b08616-8039-4069-aaa9-f6dafcc8adf6'
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
       print('[lastUpdated]: SUCCESS, '+ date.isoformat() +' status code '+str(r.status_code))
       return 0
   except Exception as e:
       print('[lastUpdated]: '+str(e))

def get_most_recent_date(table):
    #r = cartosql.getFields(TIME_FIELD, table, f='csv', post=True)
    r = getFields(TIME_FIELD, table, f='csv', post=True)
    dates = r.text.split('\r\n')[1:-1]
    dates.sort()
    most_recent_date = datetime.datetime.strptime(dates[-1], '%Y-%m-%d %H:%M:%S')
    return most_recent_date


def formatDate(date):
    """ Parse input date string and write in output date format """
    return datetime.datetime.strptime(date, INPUT_DATE_FORMAT)\
                            .strftime(DATE_FORMAT)
def getFilename(date):
    '''get filename from datestamp CHECK FILE TYPE'''
    return os.path.join(DATA_DIR, '{}.csv'.format(
        FILENAME.format(date=date.strftime('%Y%m%d'))))
        
def getGeom(lon,lat):
    '''Define point geometry from latitude and longitude'''
    geometry = {
        'type': 'Point',
        'coordinates': [float(lon), float(lat)]
    }
    return geometry

    
#Function to grab the unique location id (uids) from the locations api.
def location():
    print('Fetching location ids')
    r=requests.get(DATA_LOCATION_URL)
    json_obj=r.json()
    json_obj_list=json_obj['results']
    json_obj_list_get=json_obj_list.get('locations')
    location_id=[]
    # for dict in json_obj_list_get:
    #     x=dict.get('id')
    #     location_id.append(x)
    #     print('Location Ids Collected')
    for i,dict in enumerate(json_obj_list_get):
        if i<2:
            x=dict.get('id')
            location_id.append(x)
            print('Location Ids Collected')
    return location_id

def convert_time_since_epoch(timestamp):
    value = datetime.datetime.fromtimestamp(timestamp)
    return value.strftime('%Y-%m-%d')
  
#Function to use the uids to obtain the feed information and put them into a pandas dataframe with all the dictionaries unpacked
def feeds():
    feed_list = []
    print('Fetching Feed info')
    for id in location():
        r = requests.get(DATA_URL.format(id))
        json_obj = r.json()
        #print(json_obj)
        feed_results = json_obj['results']
        feed_feeds = feed_results['feeds']
        try:
            feed_list.append(feed_feeds[0])
        except:
            #print('hello')
            continue
    df = pd.DataFrame(feed_list)
    df_3 = pd.DataFrame(feed_list)
    df_2 = pd.concat([df_3.drop(['l'], axis=1), df_3['l'].apply(pd.Series)], axis=1)
    df_1 = pd.concat([df_2.drop(['latest'], axis=1), df_2['latest'].apply(pd.Series)], axis=1)
    df = pd.concat([df_1.drop(['u'], axis=1), df_1['u'].apply(pd.Series)], axis=1)
    #Original columns = 'id', 'ty', 't', 'id', 'pid', 't', 'n', 'lat', 'lng', 'ts', 'i', 'd', described in API documentation http://transitfeeds.com/api/swagger/#!/default/getFeeds
    new_columns = ['id','feed_type','feed_title','id2','pid','loc_title_t','loc_title_n','latitude','longitude','timestamp_epoch','info_url','download_url']
    #new_columns = ['feed_id','feed_title','feed_type','loc_id','ploc_id','loc_title_l','loc_title_s','latitude','longitude',
    #                'NaN','ts_latest','gtfs_zip','gtfs_txt']
    df.columns = new_columns
    
    # df['latitude'] = pd.to_numeric(df['latitude'], errors = 'coerce')
    # df['longitude'] = pd.to_numeric(df['longitude'], errors = 'coerce')
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        print(df)
    df = df.astype({'id': str, 'feed_type': str,'feed_title':str,'id2':int,'pid':int,'loc_title_t':str,'loc_title_n':str,'latitude':float,'longitude':float,
                    'timestamp_epoch':int,'info_url':str,'download_url':str})
    #df['coordinates']=list(zip(df.longitude, df.latitude))
    df['the_geom'] = df.apply(lambda row: getGeom(row['longitude'],row['latitude']),axis=1)
    df['timestamp'] = [convert_time_since_epoch(x) for x in df['timestamp_epoch'].values]
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        print(df.dtypes)
    #print(df)
    print('Dataframe Created')
    return df


def processData():
    '''
    Function to download data and upload it to Carto
    Will first try to get the data for today three times
    Then decrease a day up until 8 tries until it finds one
    '''
    success = False
    tries = 0
    df = None
    while tries < MAX_TRIES and success==False:
        print('Try running feeds, try number = {}'.format(tries))
        try:
            df = feeds()
            success = True
        except Exception as inst:
            print(inst)
            print("Error fetching data trying again")
            tries = tries + 1
            if tries == MAX_TRIES:
                logging.error("Error fetching data, and max tries reached. See source for last data update.")
            success = False
    if success == True:
        #print(cartosql.tableExists(CARTO_TABLE))
        print(tableExists(CARTO_TABLE))
        #if not cartosql.tableExists(CARTO_TABLE):
        if not tableExists(CARTO_TABLE):
            print('Table {} does not exist'.format(CARTO_TABLE))
            #cartosql.createTable(CARTO_TABLE, CARTO_SCHEMA)
            createTable(CARTO_TABLE, CARTO_SCHEMA)
        else:
            print('Trying to drop table')
            # cartosql.dropTable(CARTO_TABLE)
#             cartosql.createTable(CARTO_TABLE, CARTO_SCHEMA)
            dropTable(CARTO_TABLE)
            createTable(CARTO_TABLE, CARTO_SCHEMA)
            rows = df.values.tolist()
            # print('Success!')
            # print('The following includes the first ten rows added to Carto:')
            #print(rows[:10])
            if len(rows):
                blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),CARTO_SCHEMA.values(), rows)
                #cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),CARTO_SCHEMA.values(), rows)


def main():
    print('STARTING')
    print('Starting')
    processData()
    # Push update date
    lastUpdateDate(DATASET_ID, datetime.datetime.now())
    print('SUCCESS')