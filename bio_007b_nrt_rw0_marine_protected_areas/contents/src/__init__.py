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

# create a dictionary to store the parameters of the two wdpa marine datasets: point and polygon
DATA_DICT = OrderedDict()
# the name of the two carto tables to store the data 
DATA_DICT['polygon'] = {'CARTO_TABLE': 'bio_007b_rw0_marine_protected_area_polygon_edit'}
DATA_DICT['point'] = {'CARTO_TABLE': 'bio_007b_rw0_marine_protected_area_point_edit'}

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
DATA_DICT['polygon']['CARTO_SCHEMA'] = OrderedDict([
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

DATA_DICT['point']['CARTO_SCHEMA'] = OrderedDict([
    ('wdpaid', "numeric"),
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

# url at which the data can be downloaded 
SOURCE_URL = 'https://d1gam3xoknrgr2.cloudfront.net/current/WDPA_WDOECM_{}_Public_marine_shp.zip' #check

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = '483c87c7-8724-4758-b8f0-a536b3a8f8a9'

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
    '''
    download, unzip, and import the data as geopandas dataframes
    '''
    # pull the data from the url 
    n_tries = 5
    date = datetime.datetime.utcnow()  
    fetch_exception = None
    for i in range(0, n_tries):
        try:
            date_str = date.strftime("%b%Y")
            raw_data_file = os.path.join(DATA_DIR, os.path.basename(SOURCE_URL.format(date_str)))
            urllib.request.urlretrieve(SOURCE_URL.format(date_str), raw_data_file)
            # unzip file containing the data 
            logging.info('Unzip the data folder')
    
            # unzip source data
            raw_data_file_unzipped = raw_data_file.split('.')[0]
            zip_ref = ZipFile(raw_data_file, 'r')
            zip_ref.extractall(raw_data_file_unzipped)
            zip_ref.close()
            
            logging.info("Data of {} successfully fetched.".format(date_str))
        except Exception as e: 
            fetch_exception = e
            first = date.replace(day=1)
            date = (first - datetime.timedelta(days=1)).strftime("%b%Y") 
        
        else: 
            break
    
    else: 
        logging.info('Failed to fetch data.')
        raise fetch_exception

    # find all the zipped folders that contain the shapefiles
    zipped_shp = glob.glob(os.path.join(raw_data_file_unzipped, '*shp*.zip' ))
    # unzipped each of them
    for zipped in zipped_shp:
        zip_ref = ZipFile(zipped, 'r')
        zip_ref.extractall(os.path.join(raw_data_file_unzipped, zipped.split('.')[0][-5:]))
        zip_ref.close()
    
    # store the path to all the point shapefiles in a list 
    DATA_DICT['point']['path'] = [glob.glob(os.path.join(raw_data_file_unzipped, zipped.split('.')[0][-5:], '*points.shp'))[0] for path in zipped_shp]

    # store the path to all the polygon shapefiles in a list
    DATA_DICT['polygon']['path'] = [glob.glob(os.path.join(raw_data_file_unzipped, zipped.split('.')[0][-5:], '*polygons.shp'))[0] for path in zipped_shp]

    # for each value in the dictionary, merge the corresponding three shapefiles and read them as one single dataframe
    for value in DATA_DICT.values():
        value['gdf'] = gpd.GeoDataFrame(pd.concat([gpd.read_file(shp) for shp in value['path']], 
                        ignore_index=True), crs=gpd.read_file(value['path'][0]).crs)
        logging.info(list(value['gdf']))

def processData(table, gdf, schema):
    '''
    Upload new data
    INPUT   table: Carto table to upload data to (string)
            gdf: data to be uploaded to the Carto table (geopandas dataframe)
            schema: dictionary of column names and types, used if we are creating the table for the first time (dictionary)

    RETURN  num_new: total number of rows of data sent to Carto table (integer)
    '''

    # create a copy of the geopandas dataframe
    gdf_converted = gdf.copy()
    # convert the geometry of the geodataframe to geojsons
    '''
    gdf_converted['geometry'] = [x.__geo_interface__ if x.geom_type == 'Polygon' else x[0].__geo_interface__ if (x.geom_type == 'MultiPoint') & (len(x) == 1) else x.__geo_interface__ for x in gdf_converted.geometry]
    '''
    # convert all the Nan to None 
    gdf_converted = gdf_converted.where(pd.notnull(gdf_converted), None)
    # upload the data to Carto 
    logging.info('Uploading data to {}'.format(table))
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
                cartosql.insertRows(table, schema.keys(), schema.values(), [row.values.tolist()], user=CARTO_USER, key=CARTO_KEY)
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

    # add the number of rows uploaded to num_new
    logging.info('{} of rows uploaded to {}'.format(len(gdf_converted.index), table))
    num_new = len(gdf_converted.index)
    """ # change privacy of table on Carto
        # set up carto authentication using local variables for username and API key 
        auth_client = APIKeyAuthClient(api_key=CARTO_KEY, base_url="https://{user}.carto.com/".format(user=CARTO_USER))
        # set up dataset manager with authentication
        dataset_manager = DatasetManager(auth_client)
        # set dataset privacy
        dataset = dataset_manager.get(value['CARTO_TABLE'])
        dataset.privacy = 'LINK'
        dataset.save() """

    return num_new

def updateResourceWatch(num_new):

    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date' and updating any dates on layers
    '''
    # If there have been data uploaded to the Carto table
    if num_new > 0:
        # Update dataset's last update date on Resource Watch
        most_recent_date = datetime.datetime.utcnow()
        lastUpdateDate(DATASET_ID, most_recent_date)

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # fetch the shapefiles from the data source and import them as geopandas dataframes
    logging.info('Download the data, unzip the folders, and import the shapefiles as geopandas dataframes.')
    delete_local()
    fetch()

    # number of rows of data uploaded 
    num_new = 0
    for value in DATA_DICT.values():
        # clear the table before starting, if specified
        if CLEAR_TABLE_FIRST:
            logging.info('Clearing Table')
            # if the table exists
            if cartosql.tableExists(value['CARTO_TABLE'], user=CARTO_USER, key=CARTO_KEY):
                # delete all the rows
                # maximum attempts to make
                n_tries = 3
                # sleep time between each attempt   
                retry_wait_time = 5
                clear_exception = None
                for i in range(n_tries):
                    try:
                        cartosql.deleteRows(value['CARTO_TABLE'], 'cartodb_id IS NOT NULL', user=CARTO_USER, key=CARTO_KEY)
                    except Exception as e:
                        clear_exception = e
                        logging.info('Failed to clear table. Try again after 5 seconds.')
                        time.sleep(retry_wait_time)
                    else:
                        logging.info('{} cleared.'.format(value['CARTO_TABLE']))
                        break
                else: 
                    # this happens if the for loop completes, ie if it attempts to clear the table n_tries times
                    logging.info('Failed to clear table.')
                    logging.error('Raising exception encountered during last clear table attempt')
                    logging.error(clear_exception)
                    raise clear_exception
                 
                # note: we do not delete the entire table because this will cause the dataset visualization on Resource Watch
                # to disappear until we log into Carto and open the table again. If we simply delete all the rows, this
                # problem does not occur

        # Check if table exists, create it if it does not
        logging.info('Checking if table exists and getting existing IDs.')
        checkCreateTable(value['CARTO_TABLE'], value['CARTO_SCHEMA'], UID_FIELD)
        
        # process and upload the data to the carto tables 
        num_new += processData(value['CARTO_TABLE'], value['gdf'], value['CARTO_SCHEMA'])

    # Update Resource Watch
    updateResourceWatch(num_new)

    # Delete local files in Docker container
    delete_local()

    logging.info('SUCCESS')
