import logging
import sys
import os
from collections import OrderedDict
import cartosql
import requests
import datetime
import copy
import time
import numpy as np
import urllib
import zipfile
import pandas as pd
import shutil

# WDPA API Reference document: https://api.protectedplanet.net/documentation

### Constants
#API documentation: https://api.protectedplanet.net/documentation#get-v3protectedareas

LOG_LEVEL = logging.INFO
CLEAR_TABLE_FIRST = False
REPLACE_ALL = True
### Table name and structure
CARTO_TABLE = 'bio_007_world_database_on_protected_areas'
UID_FIELD='wdpa_id'
CARTO_SCHEMA = OrderedDict([
    ("the_geom", "geometry"),
    ("name", "text"),
    ("orig_name", "text"),
    ("wdpa_id", "numeric"),
    ("marine", "text"),
    ("rep_m_area", "numeric"),
    ("rep_area", "numeric"),
    ("mang_plan", "text"),
    ("is_green_list", "text"),
    ("own_type", "text"),
    ("country_name", "text"),
    ("iso3", "text"),
    ("iucn_cat", "text"),
    ("desig", "text"),
    ("desig_type", "text"),
    ("no_take", "text"),
    ("no_tk_area", "numeric"),
    ("status", "text"),
    ("mang_auth", "text"),
    ("gov_type", "text"),
    ("link", "text"),
    ("legal_status_updated_at", "timestamp"),
    ("status_yr", "numeric"),
])

JSON_LOC = {
    "the_geom": ["geojson", "geometry"],
    "name": ["name"],
    "orig_name": ["original_name"],
    "wdpa_id": ["id"],
    "marine": ["marine"],
    "rep_m_area": ["reported_marine_area"],
    "rep_area": ["reported_area"],
    "mang_plan": ["management_plan"],
    "is_green_list": ["is_green_list"],
    "own_type": ["owner_type"],
    "country_name": ["countries", 0, "name"],
    "iso3": ["countries", 0, "iso_3"],
    "iucn_cat": ["iucn_category", "name"],
    "desig": ["designation", "name"],
    "desig_type": ["designation", "jurisdiction", "name"],
    "no_take": ["no_take_status", "name"],
    "no_tk_area": ["no_take_status", "area"],
    "status": ["legal_status", "name"],
    "mang_auth": ["management_authority", "name"],
    "gov_type": ["governance", "governance_type"],
    "link": ["links", "protected_planet"],
    "legal_status_updated_at": ["legal_status_updated_at"],
    "status_yr": ["legal_status_updated_at"],
     }
# Table limits
MAX_ROWS = 1000000
DATA_DIR = 'data'
DELETE_LOCAL=True
DATASET_ID = '2442891a-157a-40e6-9092-ee596e6d30ba'
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
    if cartosql.tableExists(table, user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY')):
        logging.info('Fetching existing IDs')
        r = cartosql.getFields(id_field, table, f='csv', post=True, user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))
        return r.text.split('\r\n')[1:-1]
    else:
        logging.info('Table {} does not exist, creating'.format(table))
        cartosql.createTable(table, schema, user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))
        if id_field:
            cartosql.createIndex(table, id_field, unique=True, user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))
        if time_field:
            cartosql.createIndex(table, time_field, user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))
    return []

def fetch_ids_old(existing_ids):
    new_ids = []

    #get new ids that aren't in the table
    page = 1
    all_ids=[]

    #pull the first page of ids and continue to do so as long as we are still receiving data
    url = "https://api.protectedplanet.net/v3/protected_areas?token={}".format(os.getenv('WDPA_key'))
    while page ==1 or r.json()['protected_areas']:
        #don't download geometries for all areas because it takes a very long time
        logging.info('Fetching page {}'.format(page))
        params = {'with_geometry': 'False',
                  'page': str(page),
                  'per_page': '50'}
        r = requests.get(url, params=params)
        for response in r.json()['protected_areas']:
            id = response['wdpa_id']
            all_ids.append(id)
            if str(id) not in existing_ids:
                new_ids.append(id)
        page+=1
    new_ids = np.unique(new_ids)
    logging.info('{} new records found'.format(len(new_ids)))
    return new_ids, all_ids

def fetch_ids(existing_ids_int):
    filename_csv = 'WDPA_{mo}{yr}-csv'.format(mo=datetime.datetime.today().strftime("%b"), yr=datetime.datetime.today().year)
    url_csv = 'http://d1gam3xoknrgr2.cloudfront.net/current/{}.zip'.format(filename_csv)

    urllib.request.urlretrieve(url_csv, DATA_DIR + '/' + filename_csv + '.zip')
    zip_ref = zipfile.ZipFile(DATA_DIR + '/' + filename_csv + '.zip', 'r')
    zip_ref.extractall(DATA_DIR + '/' + filename_csv)
    zip_ref.close()

    # read in climate change vulnerability data to pandas dataframe
    filename = DATA_DIR + '/' + filename_csv + '/' + filename_csv + '.csv'
    wdpa_df = pd.read_csv(filename, low_memory=False)

    all_ids = np.unique(wdpa_df.WDPAID.to_list()).tolist()
    logging.info('found {} ids'.format(len(all_ids)))
    new_ids = np.unique(np.setdiff1d(all_ids, existing_ids_int)).tolist()
    logging.info('{} new ids'.format(len(new_ids)))

    return new_ids, all_ids

def delete_carto_entries(id_list, column):
    where = None
    for delete_id in id_list:
        if where:
            where += f' OR {column} = {delete_id}'
        else:
            where = f'{column} = {delete_id}'
        # if where statement is long or we are on the last id, delete rows
        if len(where) > 15000 or delete_id == id_list[-1]:
            cartosql.deleteRows(CARTO_TABLE, where=where, user=os.getenv('CARTO_USER'),
                                key=os.getenv('CARTO_KEY'))
            where = None

def processData(existing_ids):
    existing_ids_int = [int(i) for i in existing_ids]
    # Fetching list of new WDPA IDs
    new_ids, all_ids = fetch_ids(existing_ids_int)
    if REPLACE_ALL==True:
        id_list = all_ids
    else:
        id_list = new_ids
    #go through and fetch information for new ids
    new_data = []
    send_list=[]
    for id in [2898, 365152, 555557932, 555638687, 555643543]:
        try_num=0
        url = "https://api.protectedplanet.net/v3/protected_areas/{}?token={}".format(id, os.getenv('WDPA_key'))
        if try_num <3:
            try:
                r = requests.get(url)
            except:
                time.sleep(60)
                try_num+=1
        else:
            logging.info(f'Could not fetch {id}')
        try:
            data = r.json()['protected_area']
            row = []
            for key in CARTO_SCHEMA.keys():
                location = JSON_LOC[key]
                key_data = copy.copy(data)
                if key == 'country_name' and len(key_data['countries']) > 1:
                    countries = key_data["countries"]
                    c_list=[]
                    for country in countries:
                        c_list.append(country["name"])
                    key_data = '; '.join(c_list)
                elif key == 'iso3' and len(key_data['countries']) > 1:
                    countries= key_data["countries"]
                    c_list=[]
                    for country in countries:
                        c_list.append(country["iso_3"])
                    key_data = '; '.join(c_list)
                else:
                    for sub in location:
                        try:
                            key_data = key_data[sub]
                            if type(key_data)==str:
                                key_data = key_data.rstrip()
                        except (TypeError, IndexError):
                            key_data=None
                            break
                if key_data:
                    if key == 'status_yr':
                        key_data=int(key_data[-4:])
                    if key == 'metadataid':
                        key_data=int(key_data)
                    if key == 'wdpa_id':
                        if key_data:
                            key_data = int(key_data)
                        else:
                            key_data=int(id)
                        send_list.append(key_data)
                    if key == 'no_tk_area' or key == 'rep_area' or key == 'rep_m_area':
                        key_data=float(key_data)
                    if key == 'legal_status_updated_at':
                        key_data=datetime.datetime.strptime(key_data, '%m/%d/%Y')
                else:
                    key_data=None
                row.append(key_data)
            if len(row):
                new_data.append(row)
        except Exception as e:
            logging.error('error pulling {}'.format(id))
        if (id_list.index(id) % 1000)==0 and id_list.index(id)>1:
            logging.info('{} records processed.'.format(id_list.index(id)))
            num_new = len(new_data)
            if num_new:
                logging.info('Deleting old records in this batch')
                delete_carto_entries(send_list, 'wdpa_id')

                # push new data
                logging.info('Adding {} new records.'.format(num_new))
                cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), new_data, user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))
                new_data = []
                send_list = []
    logging.info('Deleting records that are no longer in the database.')


    '''
    delete rows that no longer exist
    '''
    deleted_ids = np.setdiff1d(existing_ids_int, id_list)
    delete_carto_entries(deleted_ids, 'wdpa_id')
    logging.info('{} ids deleted'.format(len(deleted_ids)))
    return(num_new)


def main():
    start_time=time.time()
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    if CLEAR_TABLE_FIRST:
        logging.info('Clearing Table')
        if cartosql.tableExists(CARTO_TABLE, user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY')):
            cartosql.deleteRows(CARTO_TABLE, 'cartodb_id IS NOT NULL', user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))

    ### 1. Check if table exists, if not, create it
    logging.info('Checking if table exists and getting existing IDs.')
    existing_ids = checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD)
    # for now, we will just replace the whole table because API does not have indication of which areas have been updated
    #existing_ids=[]
    num_existing = len(existing_ids)

    ### 2. Fetch data from FTP, dedupe, process
    logging.info('Fetching new data')
    num_new = processData(existing_ids)

    ### 3. Notify results
    total = num_existing + num_new

    # If updates, change update date on RW
    if num_new>0:
        lastUpdateDate(DATASET_ID, datetime.datetime.utcnow())
    else:
        logging.error('No new data.')

    logging.info('Existing rows: {},  New rows: {}'.format(total, num_new))
    end_time=time.time()
    run_time=end_time-start_time
    logging.info("SUCCESS, run time: {}".format(datetime.timedelta(seconds=run_time)))
    # Delete local files
    if DELETE_LOCAL:
        try:
            for f in os.listdir(DATA_DIR):
                try:
                    logging.info('Removing {}'.format(f))
                    os.remove(DATA_DIR+'/'+f)
                except:
                    shutil.rmtree(f)
        except NameError:
            logging.info('No local files to clean.')
