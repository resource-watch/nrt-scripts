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
from zipfile import ZipFile
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

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
DATA_DICT['polygon']['CARTO_SCHEMA'] = OrderedDict([
    ('wpdaid', "numeric"),
    ("wdpa_id", "text"),
    ('pa_def', "numeric"),
    ("name", "text"),
    ("orig_name", "text"),
    ("desig", "text"),
    ("desig_eng", "text"),
    ("desig_type", "text"),
    ("iucn_cat", "text"),
    ("int_crit", "text"),
    ("marine", "numeric"),
    ("rep_m_area", "numeric"),
    ("gis_m_area", "numeric"),
    ("rep_area", "numeric"),
    ("gis_area", "numeric"),
    ("no_take", "text"),
    ("no_tk_area", "numeric"),
    ("status", "text"),
    ("status_yr", "numeric"),
    ("gov_type", "text"),
    ("own_type", "text"),
    ("mang_auth", "text"),
    ("mang_plan", "text"),
    ("verif", "text"),
    ("metadataid", "numeric"),
    ("sub_loc", "text"),
    ("parent_iso", "text"),
    ("iso3", "text"),
    ("supp_info", "text"),
    ("cons_obj", "text"),
    ("the_geom", "geometry")])

DATA_DICT['point']['CARTO_SCHEMA'] = OrderedDict([
    ('wpdaid', "numeric"),
    ("wdpa_id", "text"),
    ('pa_def', "numeric"),
    ("name", "text"),
    ("orig_name", "text"),
    ("desig", "text"),
    ("desig_eng", "text"),
    ("desig_type", "text"),
    ("iucn_cat", "text"),
    ("int_crit", "text"),
    ("marine", "numeric"),
    ("rep_m_area", "numeric"),
    ("rep_area", "numeric"),
    ("no_take", "text"),
    ("no_tk_area", "numeric"),
    ("status", "text"),
    ("status_yr", "numeric"),
    ("gov_type", "text"),
    ("own_type", "text"),
    ("mang_auth", "text"),
    ("mang_plan", "text"),
    ("verif", "text"),
    ("metadataid", "numeric"),
    ("sub_loc", "text"),
    ("parent_iso", "text"),
    ("iso3", "text"),
    ("supp_info", "text"),
    ("cons_obj", "text"),
    ("the_geom", "geometry")])

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
def checkCreateTable(table, schema, id_field, time_field=''):
    '''
    Create the table if it does not exist, and pull list of IDs already in the table if it does
    INPUT   table: Carto table to check or create (string)
            schema: dictionary of column names and types, used if we are creating the table for the first time (dictionary)
            id_field: name of column that we want to use as a unique ID for this table; this will be used to compare the
                    source data to the our table each time we run the script so that we only have to pull data we
                    haven't previously uploaded (string)
            time_field:  optional, name of column that will store datetime information (string)
    RETURN  list of existing IDs in the table, pulled from the id_field column (list of strings)
    '''
    # check it the table already exists in Carto
    if cartosql.tableExists(table, user=CARTO_USER, key=CARTO_KEY):
        # if the table does exist, get a list of all the values in the id_field column
        logging.info('Fetching existing IDs')
        r = cartosql.getFields(id_field, table, f='csv', post=True, user=CARTO_USER, key=CARTO_KEY)
        # turn the response into a list of strings, removing the first and last entries (header and an empty space at end)
        return r.text.split('\r\n')[1:-1]
    else:
        # if the table does not exist, create it with columns based on the schema input
        logging.info('Table {} does not exist, creating'.format(table))
        cartosql.createTable(table, schema, user=CARTO_USER, key=CARTO_KEY)
        # if a unique ID field is specified, set it as a unique index in the Carto table; when you upload data, Carto
        # will ensure no two rows have the same entry in this column and return an error if you try to upload a row with
        # a duplicate unique ID
        if id_field:
            cartosql.createIndex(table, id_field, unique=True, user=CARTO_USER, key=CARTO_KEY)
        # if a time_field is specified, set it as an index in the Carto table; this is not a unique index
        if time_field:
            cartosql.createIndex(table, time_field, user=CARTO_USER, key=CARTO_KEY)
        # return an empty list because there are no IDs in the new table yet
        return []

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
    raw_data_file = os.path.join(DATA_DIR, os.path.basename(SOURCE_URL))
    # urllib.request.urlretrieve(SOURCE_URL, raw_data_file)

    # unzip source data
    raw_data_file_unzipped = raw_data_file.split('.')[0]
    zip_ref = ZipFile(raw_data_file, 'r')
    # zip_ref.extractall(raw_data_file_unzipped)
    zip_ref.close()

    # find all the zipped folders that contain the shapefiles
    zipped_shp = glob.glob(os.path.join(raw_data_file_unzipped, '*shp*.zip' ))

    # unzipped each of them
    for zipped in zipped_shp:
        zip_ref = ZipFile(zipped, 'r')
        # zip_ref.extractall(zipped.split('.')[0])
        zip_ref.close()
    
    # store the path to all the point shapefiles in a list 
    DATA_DICT['point']['path'] = [glob.glob(os.path.join(path.split('.')[0], '*points.shp'))[0] for path in zipped_shp]

    # store the path to all the polygon shapefiles in a list
    DATA_DICT['polygon']['path'] = [glob.glob(os.path.join(path.split('.')[0], '*polygons.shp'))[0] for path in zipped_shp]

    for value in DATA_DICT.values():
        value['gdf'] = gpd.GeoDataFrame(pd.concat([gpd.read_file(shp) for shp in value['path']], 
                        ignore_index=True), crs=gpd.read_file(value['path'][0]).crs)
        logging.info(list(value['gdf']))

    

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
    RETURN  num_new: total number of rows of data sent to Carto table (integer)
    '''
    num_new = 0
    # fetch the shapefiles from the data source and import them as geopandas dataframes
    fetch()
    # loop through the data dictionary
    for value in DATA_DICT.values():
        # create a copy of the geopandas dataframe
        gdf_converted = value['gdf'].copy()
        # convert the geometry of the geodataframe copy to geojsons
        converted_geom = []
        for geom in gdf_converted.geometry:
            converted_geom.append(geom.__geo_interface__)
        gdf_converted['geometry'] = converted_geom
        # convert all the Nan to None 
        gdf_converted = gdf_converted.where(pd.notnull(gdf_converted), None)
        # upload the data to Carto 
        logging.info('Uploading data to {}'.format(value['CARTO_TABLE']))
        cartosql.blockInsertRows(value['CARTO_TABLE'], value['CARTO_SCHEMA'].keys(), value['CARTO_SCHEMA'].values(), gdf_converted.values.tolist(), user=CARTO_USER, key=CARTO_KEY)
        # 
        num_new += len(gdf_converted.index)
        #for index, row in gdf_converted.iterrows():
        #    cartosql.insertRows(value['CARTO_TABLE'], value['CARTO_SCHEMA'].keys(), value['CARTO_SCHEMA'].values(), [row.values.tolist()], user=CARTO_USER, key=CARTO_KEY)
        # change privacy of table on Carto
        # set up carto authentication using local variables for username and API key 
        auth_client = APIKeyAuthClient(api_key=CARTO_KEY, base_url="https://{user}.carto.com/".format(user=CARTO_USER))
        # set up dataset manager with authentication
        dataset_manager = DatasetManager(auth_client)
        # set dataset privacy
        dataset = dataset_manager.get(value['CARTO_TABLE'])
        dataset.privacy = 'LINK'
        dataset.save()

    return num_new

def updateResourceWatch(num_new):

    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date' and updating any dates on layers
    '''
    # If there are new entries in the Carto table
    if num_new > 0:
        # Update dataset's last update date on Resource Watch
        most_recent_date = datetime.datetime.utcnow()
        lastUpdateDate(DATASET_ID, most_recent_date)

    # Update the dates on layer legends - TO BE ADDED IN FUTURE

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    for value in DATA_DICT.values():
        # clear the table before starting, if specified
        if CLEAR_TABLE_FIRST:
            logging.info('Clearing Table')
            # if the table exists
            if cartosql.tableExists(value['CARTO_TABLE'], user=CARTO_USER, key=CARTO_KEY):
                # delete all the rows
                cartosql.deleteRows(value['CARTO_TABLE'], 'cartodb_id IS NOT NULL', user=CARTO_USER, key=CARTO_KEY)
                # note: we do not delete the entire table because this will cause the dataset visualization on Resource Watch
                # to disappear until we log into Carto and open the table again. If we simply delete all the rows, this
                # problem does not occur

        # Check if table exists, create it if it does not
        logging.info('Checking if table exists and getting existing IDs.')
        existing_ids = checkCreateTable(value['CARTO_TABLE'], value['CARTO_SCHEMA'], UID_FIELD)

    processData()
    logging.info('Previous rows: {},  New rows: {}'.format(len(existing_ids), num_new))

    # Update Resource Watch
    updateResourceWatch(num_new)

    # Delete local files in Docker container
    delete_local()

    logging.info('SUCCESS')
