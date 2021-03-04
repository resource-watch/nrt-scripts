import logging
import sys
import os
from collections import OrderedDict
import cartosql
import requests
import datetime
import copy
import multiprocessing
import time
import numpy as np
import urllib
import zipfile
import geopandas as gpd
import pandas as pd
import shutil
import glob

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = False

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

# name of table in Carto where we will upload the data
CARTO_TABLE = 'bio_007_rw2_world_database_on_protected_areas'

# column of table that can be used as a unique ID (UID)
UID_FIELD='wdpa_pid'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA =OrderedDict([
    ('wdpaid', "numeric"),
    ("wdpa_pid", "text"),
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

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = 'dc930848-5a2d-4d76-bff6-99d8ad9a0763'

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

def fetch_data():
    '''
    Get a list of WDPA IDs in the version of the dataset we are pulling
    RETURN  new_ids: list of IDs in the WDPA table that we don't already have in our existing IDs (list of strings)
            all_ids: list of all IDs in the WDPA table (list of strings)
    '''
    # pull current csv containing WDPA IDs
    # note: IDs are pulled from this csv and not the API because querying the API is very slow, so it is much faster
    # to get a list of all the IDS from this csv
    filename_data = 'WDPA_Feb2021_Public'
    """ 
    url_data = f'https://d1gam3xoknrgr2.cloudfront.net/current/{filename_data}.zip'
    urllib.request.urlretrieve(url_data, DATA_DIR + '/' + filename_data + '.zip')

    # unzip file containing csv
    zip_ref = zipfile.ZipFile(DATA_DIR + '/' + filename_data + '.zip', 'r')
    zip_ref.extractall(DATA_DIR + '/' + filename_data)
    zip_ref.close() """

    # load in the table from the geodatabase
    gdb = glob.glob(os.path.join(DATA_DIR + '/' + filename_data, '*.gdb'))[0]
    gdf = gpd.read_file(gdb, driver='FileGDB', layer = 0, encoding='utf-8')
    
    logging.info(list(gdf))
    return gdf

def delete_carto_entries(id_list, column):
    '''
    Delete entries in Carto table based on values in a specified column
    INPUT   id_list: list of column values for which you want to delete entries in table (list of strings)
            column: column name where you should search for these values (string)
    '''
    # generate empty variable to store WHERE clause of SQL query we will send
    where = None
    # go through each ID in the list to be deleted
    for delete_id in id_list:
        # if we already have values in the SQL query, add the new value with an OR before it
        if where:
            where += f' OR {column} = {delete_id}'
        # if the SQL query is empty, create the start of the WHERE clause
        else:
            where = f'{column} = {delete_id}'
        # if where statement is long or we are on the last id, delete rows
        # the length of 15000 was chosen arbitrarily - all the IDs to be deleted could not be sent at once, but no
        # testing was done to optimize this value
        if len(where) > 15000 or delete_id == id_list[-1]:
            cartosql.deleteRows(CARTO_TABLE, where=where, user=CARTO_USER,
                                key=CARTO_KEY)
            # after we have deleted a set of rows, start over with a blank WHERE clause for the SQL query so we don't
            # try to delete rows we have already deleted
            where = None

def convert_geometry(geom):
    if geom.geom_type == 'Polygon':
        return geom.__geo_interface__
    # if it's a multipoint series containing only one point
    elif (geom.geom_type == 'MultiPoint') & (len(geom) == 1):
        return geom[0].__geo_interface__
    else:
        return geom.__geo_interface__

def upload_to_carto(gdf):
    gdf = gdf.where(pd.notnull(gdf))
     # upload the data to Carto 
    logging.info('Uploading data to {}'.format(CARTO_TABLE))
    # maximum attempts to make
    n_tries = 4
    # sleep time between each attempt   
    retry_wait_time = 6
    for index, row in gdf.iterrows():
        # for each row in the geopandas dataframe
        insert_exception = None
        for i in range(n_tries):
            row['geometry'] = convert_geometry(row['geometry'])
            try:
                # upload the row to the carto table
                cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), [row.values.tolist()], user=CARTO_USER, key=CARTO_KEY)
            except Exception as e: # if there's an exception do this
                insert_exception = e
                logging.warning('Attempt #{} to upload row #{} unsuccessful. Trying again after {} seconds'.format(i, index, retry_wait_time))
                logging.debug('Exception encountered during upload attempt: '+ str(e))
                time.sleep(retry_wait_time)
            else: # if no exception do this
                break # break this for loop, because we don't need to try again
        else:
            # this happens if the for loop completes, ie if it attempts to insert row n_tries times
            logging.error('Upload of row #{} has failed after {} attempts'.format(index, n_tries))
            # could skip to next row, or more likely abort operation, for example by raising uncaught exception
            logging.error('Problematic row: '+ str(row))
            logging.error('Raising exception encountered during last upload attempt')
            logging.error(insert_exception)
            raise insert_exception

def processData(existing_ids):
    '''
    Fetch, process, upload, and clean new data
    INPUT   existing_ids: list of WDPA IDs that we already have in our Carto table  (list of strings)
    RETURN  num_new: number of rows of data sent to Carto table (integer)
    '''
    # fetch the data 
    gdf_data = fetch_data()
    # get a list of all IDs in the table
    all_ids = list(gdf_data['WDPA_PID'])
    logging.info('found {} ids'.format(len(all_ids)))
    # get a list of the IDs in the table that we don't already have in our existing IDs
    new_ids = [x for x in all_ids if x not in existing_ids]
    logging.info('{} new ids'.format(len(new_ids)))
    
    # send all new data to Carto 
    # subset the geopandas dataframe to isolate the new data and create a copy of the geopandas dataframe
    gdf_converted = gdf_data[gdf_data['WDPA_PID'].isin(new_ids)].copy()

    NUM_CORES = 5
    # split the dataframe into chunks
    gdf_chunks = np.array_split(gdf_converted ,NUM_CORES)

    # use a pool to spawn multiple proecsses
    with multiprocessing.Pool(NUM_CORES) as pool:
        pool.map(upload_to_carto, gdf_chunks)

    """ # convert all the Nan to None 
    gdf_converted = gdf_converted.where(pd.notnull(gdf_converted), None)
    # upload the data to Carto 
    logging.info('Uploading data to {}'.format(CARTO_TABLE))
    # maximum attempts to make
    n_tries = 4
    # sleep time between each attempt   
    retry_wait_time = 5
    # for each row in the geopandas dataframe
    for index, row in gdf_converted.iterrows():
        geom = row['geometry']
        # if it's a polygon
        if geom.geom_type == 'Polygon':
            row['geometry'] = geom.__geo_interface__
        # if it's a multipoint series containing only one point
        elif (geom.geom_type == 'MultiPoint') & (len(geom) == 1):
            row['geometry'] = geom[0].__geo_interface__
        else:
            row['geometry'] = geom.__geo_interface__
        insert_exception = None
        for i in range(n_tries):
            try:
                # upload the row to the carto table
                cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), [row.values.tolist()], user=CARTO_USER, key=CARTO_KEY)
            except Exception as e: # if there's an exception do this
                insert_exception = e
                logging.warning('Attempt #{} to upload row #{} unsuccessful. Trying again after {} seconds'.format(i, index, retry_wait_time))
                logging.debug('Exception encountered during upload attempt: '+ str(e))
                time.sleep(retry_wait_time)
            else: # if no exception do this
                break # break this for loop, because we don't need to try again
        else:
            # this happens if the for loop completes, ie if it attempts to insert row n_tries times
            logging.error('Upload of row #{} has failed after {} attempts'.format(index, n_tries))
            # could skip to next row, or more likely abort operation, for example by raising uncaught exception
            logging.error('Problematic row: '+ str(row))
            logging.error('Raising exception encountered during last upload attempt')
            logging.error(insert_exception)
            raise insert_exception """

    # add the number of rows uploaded to num_new
    logging.info('{} of rows uploaded to {}'.format(len(gdf_converted.index), CARTO_TABLE))
    num_new = len(gdf_converted.index)
        
    return(num_new)

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

    # clear the table before starting, if specified
    if CLEAR_TABLE_FIRST:
        logging.info('Clearing Table')
        # if the table exists
        if cartosql.tableExists(CARTO_TABLE, user=CARTO_USER, key=CARTO_KEY):
            # delete all the rows
            cartosql.deleteRows(CARTO_TABLE, 'cartodb_id IS NOT NULL', user=CARTO_USER, key=CARTO_KEY)
            # note: we do not delete the entire table because this will cause the dataset visualization on Resource Watch
            # to disappear until we log into Carto and open the table again. If we simply delete all the rows, this
            # problem does not occur

    # Check if table exists, create it if it does not
    logging.info('Checking if table exists and getting existing IDs.')
    existing_ids = checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD)

    # Fetch, process, and upload the new data
    logging.info('Fetching new data')
    num_new = processData(existing_ids)
    logging.info('Previous rows: {},  New rows: {}'.format(len(existing_ids), num_new))

    # Update Resource Watch
    #updateResourceWatch(num_new)

    # Delete local files in Docker container
    delete_local()

    logging.info('SUCCESS')
