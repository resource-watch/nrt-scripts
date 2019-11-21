import pandas as pd
from shapely.geometry import Point
import requests
import logging
import json
import os
import cartoframes
import datetime
import cartosql
from collections import OrderedDict
import numpy as np

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
    ('feed_id', 'numeric'),
    ('feed_type','text'),
    ('feed_title','text'),
    ('loc_id','numeric'),
    ('ploc_id','numeric'),
    ('loc_title_l','text'),
    ('loc_title_s','text'),
    ('latitude','numeric'),
    ('longitude','numeric'),
    ('timestamp_epoch','numeric'),
    ('ts_latest','timestamp'),
    ('gtfs_zip','text'),
    ('gtfs_txt','text')
])
CLEAR_TABLE_FIRST = True
INPUT_DATE_FORMAT = '%Y%m%d'
DATE_FORMAT = '%Y-%m-%d'
TIME_FIELD = 'ts_latest'
MAX_TRIES = 8
CARTO_URL = 'https://{}.carto.com/api/v2/sql'
CARTO_USER = os.environ.get('CARTO_USER')
CARTO_KEY = os.environ.get('CARTO_KEY')
STRICT = True
USR_BASE_URL = "https://{user}.carto.com/".format(user=CARTO_USER)

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
       logging.info('[lastUpdated]: SUCCESS, '+ date.isoformat() +' status code '+str(r.status_code))
       return 0
   except Exception as e:
       logging.info('[lastUpdated]: '+str(e))

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

def convert_time_since_epoch(timestamp):
    '''Function to convert seconds since the epoch to human readable time'''
    value = datetime.datetime.fromtimestamp(timestamp)
    return value.strftime('%Y-%m-%d')
    
def location():
    '''Function to grab the unique location id (uids) from the locations api.'''
    logging.info('Fetching location ids')
    r=requests.get(DATA_LOCATION_URL)
    json_obj=r.json()
    json_obj_list=json_obj['results']
    json_obj_list_get=json_obj_list.get('locations')
    location_id=[]
    for dict in json_obj_list_get:
        x=dict.get('id')
        location_id.append(x)
        logging.info('Location Ids Collected')
    return location_id
  
def feeds():
    '''Function to use the uids to obtain the feed information and put them into a pandas dataframe with all the dictionaries unpacked'''
    feed_list = []
    logging.info('Fetching Feed info')
    for id in location():
        r = requests.get(DATA_URL.format(id))
        json_obj = r.json()
        feed_results = json_obj['results']
        feed_feeds = feed_results['feeds']
        try:
            feed_list.append(feed_feeds[0])
        except:
            continue
    df = pd.DataFrame(feed_list)
    df_3 = pd.DataFrame(feed_list)
    df_2 = pd.concat([df_3.drop(['l'], axis=1), df_3['l'].apply(pd.Series)], axis=1)
    df_1 = pd.concat([df_2.drop(['latest'], axis=1), df_2['latest'].apply(pd.Series)], axis=1)
    df = pd.concat([df_1.drop(['u'], axis=1), df_1['u'].apply(pd.Series)], axis=1)
    
    #Original columns = 'id', 'ty', 't', 'id', 'pid', 't', 'n', 'lat', 'lng', 0, 'ts', 'd', 'i', described in API documentation http://transitfeeds.com/api/swagger/#!/default/getFeeds
    df = df.dropna(axis=1, how='all')
    new_columns = ['feed_id','feed_type','feed_title','loc_id','ploc_id','loc_title_l','loc_title_s','latitude','longitude','timestamp_epoch','gtfs_zip','gtfs_txt']
    df.columns = new_columns        
    df['the_geom'] = df.apply(lambda row: getGeom(row['longitude'],row['latitude']),axis=1)
    df['timestamp_epoch'] = np.nan_to_num(df['timestamp_epoch'].values)
    df['ts_latest'] = [convert_time_since_epoch(x) for x in df['timestamp_epoch'].values]
    return df


def processData():
    '''
    Function to download data and upload it to Carto
    Will first try to get the data for MAX_TRIES then quits
    '''
    success = False
    tries = 0
    df = None
    while tries < MAX_TRIES and success==False:
        logging.info('Try running feeds, try number = {}'.format(tries))
        try:
            df = feeds()
            success = True
        except Exception as inst:
            logging.info(inst)
            logging.info("Error fetching data trying again")
            tries = tries + 1
            if tries == MAX_TRIES:
                logging.error("Error fetching data, and max tries reached. See source for last data update.")
            success = False
    if success == True:
        if not cartosql.tableExists(CARTO_TABLE):
            logging.info('Table {} does not exist'.format(CARTO_TABLE))
            cartosql.createTable(CARTO_TABLE, CARTO_SCHEMA)
        else:
            cartosql.dropTable(CARTO_TABLE)
            cartosql.createTable(CARTO_TABLE, CARTO_SCHEMA)
            #Send dataframe to Carto
            logging.info('Writing to Carto')
            cc = cartoframes.CartoContext(base_url=USR_BASE_URL,
                                          api_key=CARTO_KEY)
            cc.write(df, CARTO_TABLE, overwrite=True)


def main():
    logging.info('STARTING')
    processData()
    # Push update date
    lastUpdateDate(DATASET_ID, datetime.datetime.now())
    logging.info('SUCCESS')