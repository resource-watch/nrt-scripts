import logging
import sys
import os
from collections import OrderedDict
import datetime
import cartosql
import pandas as pd
import urllib.request
import requests
#import boto3
#import gzip

### Constants
LOG_LEVEL = logging.INFO
DATA_DIR = 'data'
SOURCE_URL = 'https://eogdata.mines.edu/wwwdata/viirs_products/vbd/v23/global-saa/nrt/VBD_npp_d{date}_global-saa_noaa_ops_v23.csv'
#Example URL: https://eogdata.mines.edu/wwwdata/viirs_products/vbd/v23/global-saa/daily/VBD_npp_d20170101_global-saa_noaa_ops_v23.csv
#Example file name: VBD_npp_d20160701_global-saa_noaa_ops_v23.csv.gz

#Filename for local files
FILENAME = 'boats_{date}'

# asserting table structure rather than reading from input
CARTO_TABLE = 'com_031_boat_detections'
CARTO_SCHEMA = OrderedDict([
    ('the_geom', 'geometry'),
    ('QF', 'numeric'),
    ('UTC_Scan_time', 'timestamp'),
    ('Local_Scan_time', 'timestamp'),
    ('Land_mask', 'numeric'),
    ('long', 'numeric'),
    ('lat', 'numeric'),
    ('EEZ', 'text')
])

CLEAR_TABLE_FIRST = True
INPUT_DATE_FORMAT = '%Y%m%d'
DATE_FORMAT = '%Y-%m-%d'
TIME_FIELD = 'UTC_Scan_time'
MAX_TRIES = 8


#URL to view column definitions: https://eogdata.mines.edu/vbd/#csv_column
#Values of QF flag, denotes what type of detection it was
# 1    Strong detection. Detection surpassed all VBD threshold tests
# 2    Weak detection. Detection did not pass SHI threshold test.
# 3    Blurry detection. Detection did not pass SI threshold test.
# 4    Gas flare. Detection has a concurrent Nightfire detection, or is in the location of a known gas flare.
# 5    False detection: Detection is from high energy particles impacting the DNB sensor, usually due to the South Atlantic anomaly.
# 6    False detection: Detection is from lunar glint.
# 7    False detection: Detection is from atmospheric glow around bright sources.
# 8    Recurring detection. Detection is in location where boats are known to recur.
# 9    False detection: Detection is from sensor crosstalk around extremely bright sources, usually flares.
# 10    Weak and blurry detection. Detection did not pass either the SHI or SI threshold tests.
# 11    Offshore platform. Detection is in location of a known stable light.


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
    r = cartosql.getFields(TIME_FIELD, table, f='csv', post=True)
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

def processData():
    '''
    Function to download data and upload it to Carto
    Will first try to get the data for today three times
    Then decrease a day up until 8 tries until it finds one
    '''
    date = datetime.date.today()- datetime.timedelta(days=1)
    success = False
    tries = 0
    while tries < MAX_TRIES and success==False:
        logging.info("Fetching data for {}".format(str(date)))
        f = getFilename(date)
        url = SOURCE_URL.format(date=date.strftime('%Y%m%d'))
        try:
            urllib.request.urlretrieve(url, f)
            
        except Exception as inst:
            logging.info("Error fetching data for {}".format(str(date)))
            if tries>=2:
                date = date - datetime.timedelta(days=1)
            tries = tries + 1
            if tries == MAX_TRIES:
                logging.error("Error fetching data for {}, and max tries reached. See source for last data update.".format(str(datetime.date.today())))
            success = False
        else:
            df = pd.read_csv(f, header=0, usecols=['Lat_DNB','Lon_DNB','Date_Mscan', 'Date_LTZ','QF_Detect','EEZ','Land_Mask'])
            df = df.drop(df[df.QF_Detect == 999999].index)
            df['the_geom'] = df.apply(lambda row: getGeom(row['Lon_DNB'],row['Lat_DNB']),axis=1)

            df = df[['the_geom', 'QF_Detect', 'Date_Mscan', 'Date_LTZ', 'Land_Mask', 'Lon_DNB', 'Lat_DNB','EEZ']]
            if not cartosql.tableExists(CARTO_TABLE):
                logging.info('Table {} does not exist'.format(CARTO_TABLE))
                cartosql.createTable(CARTO_TABLE, CARTO_SCHEMA)
            else:
                cartosql.dropTable(CARTO_TABLE)
                cartosql.createTable(CARTO_TABLE, CARTO_SCHEMA)
            
                rows = df.values.tolist()
                logging.info('Success!')
                #logging.info('The following includes the first ten rows added to Carto:')
                #logging.info(rows[:10])
                if len(rows):
                    cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),CARTO_SCHEMA.values(), rows)
            tries = tries + 1
            success = True
    


def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')
    processData()
    # Get most recent update date
    most_recent_date = get_most_recent_date(CARTO_TABLE)
    print(most_recent_date)
    lastUpdateDate(DATASET_ID, most_recent_date)
    
    #logging.info('SUCCESS')
