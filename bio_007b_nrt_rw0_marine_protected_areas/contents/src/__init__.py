import logging
import sys
import os
from collections import OrderedDict
import cartosql
from carto.datasets import DatasetManager
from carto.auth import APIKeyAuthClient
import requests
import datetime
import copy
import time
import geopandas as gpd
import pandas as pd
import Zipfile
import glob 
import numpy as np
import urllib
import zipfile
import pandas as pd
import shutil

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = True

# name of data directory in Docker container
DATA_DIR = 'data'

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# do you want to update all the entries in the table when you run this script?
# True - update entire table
# False - just check for new areas added or areas deleted
# for now, we will replace everything in the table because there is no way to see if an area has been updated
REPLACE_ALL = True

DATA_DICT = OrderedDict()
DATA_DICT['polygon'] = {'CARTO_TABLE': 'bio_007b_nrt_rw0_marine_protected_areas_polygon'}
DATA_DICT['point'] = {'CARTO_TABLE': 'bio_007b_nrt_rw0_marine_protected_areas_point'}

# column of table that can be used as a unique ID (UID)
UID_FIELD='wdpa_id'

SOURCE_URL = 'https://d1gam3xoknrgr2.cloudfront.net/current/WDPA_WDOECM_marine_shp.zip' #check

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = ''

'''
FUNCTIONS FOR ALL DATASETS

The functions below must go in every near real-time script.
Their format should not need to be changed.
'''

def lastUpdateDate(dataset, date):
    '''
    Given a Resource Watch dataset's API ID and a datetime,
    this function will update the dataset's 'last update date' on the API with the given datetime
    INPUT   dataset: Resource Watch API dataset ID (string)
            date: date to set as the 'last update date' for the input dataset (datetime)
    '''
    # generate the API url for this dataset
    apiUrl = f'http://api.resourcewatch.org/v1/dataset/{dataset}'
    # create headers to send with the request to update the 'last update date'
    headers = {
    'Content-Type': 'application/json',
    'Authorization': os.getenv('apiToken')
    }
    # create the json data to send in the request
    body = {
        "dataLastUpdated": date.isoformat() # date should be a string in the format 'YYYY-MM-DDTHH:MM:SS'
    }
    # send the request
    try:
        r = requests.patch(url = apiUrl, json = body, headers = headers)
        logging.info('[lastUpdated]: SUCCESS, '+ date.isoformat() +' status code '+str(r.status_code))
        return 0
    except Exception as e:
        logging.error('[lastUpdated]: '+str(e))

'''
FUNCTIONS FOR CARTO DATASETS

The functions below must go in every near real-time script for a Carto dataset.
Their format should not need to be changed.
'''
def delete_local():
    '''
    Delete all files and folders in Docker container's data directory
    '''
    try:
        # for each object in the data directory
        for f in os.listdir(DATA_DIR):
            # try to remove it as a file
            try:
                logging.info('Removing {}'.format(f))
                os.remove(DATA_DIR+'/'+f)
            # if it is not a file, remove it as a folder
            except:
                shutil.rmtree(f, ignore_errors=True)
    except NameError:
        logging.info('No local files to clean.')

'''
FUNCTIONS FOR THIS DATASET

The functions below have been tailored to this specific dataset.
They should all be checked because their format likely will need to be changed.
'''
def fetch():
    # download the data from the source
    raw_data_file = os.path.join(data_dir, os.path.basename(SOURCE_URL))
    urllib.request.urlretrieve(SOURCE_URL, raw_data_file)

    # unzip source data
    raw_data_file_unzipped = raw_data_file.split('.')[0]
    zip_ref = ZipFile(raw_data_file, 'r')
    zip_ref.extractall(raw_data_file_unzipped)
    zip_ref.close()

    # find all the zipped folders that contain the shapefiles
    zipped_shp = glob.glob(os.path.join(raw_data_file_unzipped, '*shp*.zip' ))

    # unzipped each of them
    for zipped in zipped_shp:
        zip_ref = ZipFile(zipped, 'r')
        zip_ref.extractall(zipped.split('.')[0])
        zip_ref.close()
    
    # store the path to all the point shapefiles in a list 
    DATA_DICT['point']['path'] = [glob.glob(os.path.join(path.split('.')[0], '*points.shp'))[0] for path in zipped_shp]

    # store the path to all the polygon shapefiles in a list
    DATA_DICT['polygon']['path'] = [glob.glob(os.path.join(path.split('.')[0], '*polygons.shp'))[0] for path in zipped_shp]

    for value in DATA_DICT.values():
        value['gdf'] = gpd.GeoDataFrame(pd.concat([gpd.read_file(shp) for shp in value['path']], 
                        ignore_index=True), crs=gpd.read_file(value['path'][0]).crs)

def upload_to_carto(file, privacy, collision_strategy='skip'):
    '''
    Upload tables to Carto
    INPUT   file: location of file on local computer that you want to upload (string)
            privacy: the privacy setting of the dataset to upload to Carto (string)
            collision_strategy: determines what happens if a table with the same name already exists
            set the parameter to 'overwrite' if you want to overwrite the existing table on Carto
    '''
    # set up carto authentication using local variables for username (CARTO_WRI_RW_USER) and API key (CARTO_WRI_RW_KEY)
    auth_client = APIKeyAuthClient(api_key=CARTO_KEY, base_url="https://{user}.carto.com/".format(user=CARTO_USER))
    # set up dataset manager with authentication
    dataset_manager = DatasetManager(auth_client)
    # upload dataset to carto
    dataset = dataset_manager.create(file, collision_strategy = collision_strategy)
    logger.info('Carto table created: {}'.format(os.path.basename(file).split('.')[0]))
    # set dataset privacy
    dataset.privacy = privacy
    dataset.save()

def processData():
    '''
    Fetch, process, upload, and clean new data
    INPUT   existing_ids: list of WDPA IDs that we already have in our Carto table  (list of strings)
    RETURN  num_new: number of rows of data sent to Carto table (integer)
    '''
    fetch()
    for value in DATA_DICT.values():
        processed_data_file = os.path.join(DATA_DIR, value['CARTO_TABLE'] + '.shp')
        value['gdf'].to_file(processed_data_file, encoding = 'UTF-8')
        
        processed_data_dir = os.path.join(DATA_DIR, value['CARTO_TABLE'] + '.zip'
        with ZipFile(processed_data_dir),'w') as zip:
            for file in glob.glob(os.path.join(DATA_DIR, value['CARTO_TABLE'] + '*')):
                zip.write(file, os.path.basename(file))

        upload_to_carto(processed_data_dir, 'LINK', collision_strategy='skip')

def updateResourceWatch(num_new):

    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date' and updating any dates on layers
    '''
    # If there are new entries in the Carto table
    if num_new>0:
        # Update dataset's last update date on Resource Watch
        most_recent_date = datetime.datetime.utcnow()
        lastUpdateDate(DATASET_ID, most_recent_date)

    # Update the dates on layer legends - TO BE ADDED IN FUTURE

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    processData()
    logging.info('Previous rows: {},  New rows: {}'.format(len(existing_ids), num_new))

    # Update Resource Watch
    updateResourceWatch(num_new)

    # Delete local files in Docker container
    delete_local()

    logging.info('SUCCESS')
