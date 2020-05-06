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
CARTO_TABLE = 'bio_007_world_database_on_protected_areas'

# column of table that can be used as a unique ID (UID)
UID_FIELD='wdpa_id'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
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

# column names and paths to find them in the json returned by the source
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

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = '2442891a-157a-40e6-9092-ee596e6d30ba'

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
                shutil.rmtree(f)
    except NameError:
        logging.info('No local files to clean.')

'''
FUNCTIONS FOR THIS DATASET

The functions below have been tailored to this specific dataset.
They should all be checked because their format likely will need to be changed.
'''

def fetch_ids(existing_ids_int):
    '''
    Get a list of WDPA IDs in the version of the dataset we are pulling
    INPUT   existing_ids_int: list of WDPA IDs that we already have in our Carto table (list of integers)
    RETURN  new_ids: list of IDs in the WDPA table that we don't already have in our existing IDs (list of strings)
            all_ids: list of all IDs in the WDPA table (list of strings)
    '''
    # pull current csv containing WDPA IDs
    # note: IDs are pulled from this csv and not the API because querying the API is very slow, so it is much faster
    # to get a list of all the IDS from this csv
    filename_csv = 'WDPA_{mo}{yr}-csv'.format(mo=datetime.datetime.today().strftime("%b"), yr=datetime.datetime.today().year)
    url_csv = 'http://d1gam3xoknrgr2.cloudfront.net/current/{}.zip'.format(filename_csv)
    urllib.request.urlretrieve(url_csv, DATA_DIR + '/' + filename_csv + '.zip')

    # unzip file containing csv
    zip_ref = zipfile.ZipFile(DATA_DIR + '/' + filename_csv + '.zip', 'r')
    zip_ref.extractall(DATA_DIR + '/' + filename_csv)
    zip_ref.close()

    # read in WDPA csv as a pandas dataframe
    filename = DATA_DIR + '/' + filename_csv + '/' + filename_csv + '.csv'
    wdpa_df = pd.read_csv(filename, low_memory=False)

    # get a list of all IDs in the table
    all_ids = np.unique(wdpa_df.WDPAID.to_list()).tolist()
    logging.info('found {} ids'.format(len(all_ids)))
    # get a list of the IDs in the table that we don't already have in our existing IDs
    new_ids = np.unique(np.setdiff1d(all_ids, existing_ids_int)).tolist()
    logging.info('{} new ids'.format(len(new_ids)))

    return new_ids, all_ids

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

def processData(existing_ids):
    '''
    Fetch, process, upload, and clean new data
    INPUT   existing_ids: list of WDPA IDs that we already have in our Carto table  (list of strings)
    RETURN  num_new: number of rows of data sent to Carto table (integer)
    '''
    # turn list of existing ids from strings into integers
    existing_ids_int = [int(i) for i in existing_ids]
    # fetch list of WDPA IDs (list of all IDs and list of new ones) so that we can pull info from the API about each area
    new_ids, all_ids = fetch_ids(existing_ids_int)
    # if we have designated that we want to replace all the ids, then the list of IDs we will query (id_list) will
    # include all the IDs available; otherwise, we will just pull the new IDs
    if REPLACE_ALL==True:
        id_list = all_ids
    else:
        id_list = new_ids
    # create empty list to store IDs for rows we want to send to Carto so that we can delete any current entries before
    # sending new data
    send_list=[]
    # create empty lists to store data we will be sending to Carto table
    new_data = []
    # go through and fetch information for each of the ids
    for id in id_list:
        # set try number to 0 for this area's ID because this will be our first try fetching the data
        try_num=0
        # generate the url to pull data for this area from the WDPA API
        # WDPA API Reference document: https://api.protectedplanet.net/documentation#get-v3protectedareas
        url = "https://api.protectedplanet.net/v3/protected_areas/{}?token={}".format(id, os.getenv('WDPA_key'))
        # try at least 3 times to fetch the data for this area from the source
        if try_num <3:
            try:
                r = requests.get(url)
            except:
                # if the API call fails, wait 60 seconds before moving on to the next attempt to fetch the data
                time.sleep(60)
                try_num+=1
        else:
            # after 3 failures to fetch data for this ID, log that the data could not be fetched
            logging.info(f'Could not fetch {id}')

        # process the retrieved data
        try:
            # pull data from request response json
            data = r.json()['protected_area']
            # create an empty list to store the processed data for this row that we will send to Carto
            row = []
            # go through each column in the Carto table
            for key in CARTO_SCHEMA.keys():
                # find the location in the json where you can find this column's data
                location = JSON_LOC[key]
                # make a copy of the data that we can modify
                key_data = copy.copy(data)
                # if we are fetching data for the country_name column and there is more than one country,
                # we will need to process this entry
                if key == 'country_name' and len(key_data['countries']) > 1:
                    # get the list of countries
                    countries = key_data["countries"]
                    # make a list of the country names
                    c_list=[]
                    for country in countries:
                        c_list.append(country["name"])
                    # turn this list into a single string with the countries names listed, separated by a semicolon
                    key_data = '; '.join(c_list)
                # we will also need to process the iso3 data if there is more than one country
                elif key == 'iso3' and len(key_data['countries']) > 1:
                    # get the list of countries
                    countries= key_data["countries"]
                    # make a list of the country iso3 values
                    c_list=[]
                    for country in countries:
                        c_list.append(country["iso_3"])
                    # turn this list into a single string with the countries iso3s listed, separated by a semicolon
                    key_data = '; '.join(c_list)
                # for any other column, no special processing is required at this point, just pull out the data from
                # the correct location in the json
                else:
                    # go through each nested name
                    for sub in location:
                        # try to pull out the data from that name
                        try:
                            key_data = key_data[sub]
                            # if the data is a string, remove and leading or tailing whitespace
                            if type(key_data)==str:
                                key_data = key_data.rstrip()
                        # if we aren't able to find the data for this column, set the data as a None value and move
                        # on to the next column
                        except (TypeError, IndexError):
                            key_data=None
                            break
                # if we were able to successfully find the value for the column, do any additional required processing
                if key_data:
                    # pull the year from the data from the 'legal status updated at' field
                    if key == 'status_yr':
                        key_data=int(key_data[-4:])
                    # turn the wdpa_id into an integer
                    if key == 'wdpa_id':
                        # pull it from the API entry, if possible
                        if key_data:
                            key_data = int(key_data)
                        # otherwise just use the id from the list of ids we are going through (some entries are missing
                        # this field on the API)
                        else:
                            key_data=int(id)
                        # add this ID to the list of IDs we are sending new data for
                        send_list.append(key_data)
                    # turn these columns into float data
                    if key == 'no_tk_area' or key == 'rep_area' or key == 'rep_m_area':
                        key_data=float(key_data)
                    # turn the legal_status_updated_at column into a datetime
                    if key == 'legal_status_updated_at':
                        key_data=datetime.datetime.strptime(key_data, '%m/%d/%Y')
                # if no data was found for this column, make sure the entry is None
                else:
                    key_data=None
                # add this value to the row data
                row.append(key_data)
            # if this ID's row of data was processed, add it to the new data to be sent to Carto
            if len(row):
                new_data.append(row)
        # if we failed to process this data, log an error
        except Exception as e:
            logging.error('error pulling {}'.format(id))

        # send data
        # for every 1000 rows processed, send the data to Carto
        if (id_list.index(id) % 1000)==0 and id_list.index(id)>1:
            logging.info('{} records processed.'.format(id_list.index(id)))
            num_new = len(new_data)
            if num_new:
                # delete the old entries in the Carto table for the IDs we have processed
                logging.info('Deleting old records in this batch')
                delete_carto_entries(send_list, 'wdpa_id')

                # push new data rows to Carto
                logging.info('Adding {} new records.'.format(num_new))
                cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), new_data, user=CARTO_USER, key=CARTO_KEY)

                # start with empty lists again to process the next batch of data
                new_data = []
                send_list = []

    # delete rows for areas that are no longer in the WDPA dataset
    logging.info('Deleting records that are no longer in the database.')
    # get a list of IDs that are in the Carto table but not in the most recent WDPA dataset
    deleted_ids = np.setdiff1d(existing_ids_int, id_list)
    # delete these rows from the Carto table
    delete_carto_entries(deleted_ids, 'wdpa_id')
    logging.info('{} ids deleted'.format(len(deleted_ids)))
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
    updateResourceWatch(num_new)

    # Delete local files in Docker container
    delete_local()

    logging.info('SUCCESS')
