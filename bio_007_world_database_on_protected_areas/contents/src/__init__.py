import logging
import sys
import os
from collections import OrderedDict
import cartosql
import requests
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import rapidjson
import urllib
import zipfile
import geopandas as gpd
import shutil
import glob
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)


# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = True

# name of data directory in Docker container
DATA_DIR = 'data'

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# initiate a request session
session = requests.Session()

# name of table in Carto where we will upload the data
CARTO_TABLE = 'bio_007_world_database_on_protected_areas'

# column of table that can be used as a unique ID (UID)
UID_FIELD='wdpa_pid'

# url from which the data is fetched
URL = 'https://d1gam3xoknrgr2.cloudfront.net/current/WDPA_{}_Public.zip'

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
    ("legal_status_updated_at", "timestamp"),
    ("gov_type", "text"),
    ("own_type", "text"),
    ("mang_auth", "text"),
    ("mang_plan", "text"),
    ("verif", "text"),
    ("metadataid", "numeric"),
    ("sub_loc", "text"),
    ("parent_iso3", "text"),
    ("iso3", "text"),
    ("supp_info", "text"),
    ("cons_obj", "text"),
    ("the_geom", "geometry")])

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
                shutil.rmtree(DATA_DIR+'/'+f, ignore_errors=True)
    except NameError:
        logging.info('No local files to clean.')

'''
FUNCTIONS FOR THIS DATASET

The functions below have been tailored to this specific dataset.
They should all be checked because their format likely will need to be changed.
'''

def fetch_data():
    '''
    Get a file path to where the unzipped geodatabase is stored
    RETURN  gdb: the file path to the location of the unzipped geodatabase
    '''
    # pull the data from the url
    # maximum number of attempts 
    n_tries = 5
    # retrieve the current date 
    date = datetime.datetime.utcnow()  
    fetch_exception = None
    for i in range(0, n_tries):
        try:
            date_str = date.strftime("%b%Y")
            # construct the url based on the current month and year
            urllib.request.urlretrieve(URL.format(date_str), DATA_DIR + '/' + os.path.basename(URL.format(date_str)))
            # unzip file containing the data 
            zip_ref = zipfile.ZipFile(DATA_DIR + '/' + os.path.basename(URL.format(date_str)), 'r')
            zip_ref.extractall(DATA_DIR + '/' + os.path.basename(URL.format(date_str)).split('.')[0])
            zip_ref.close()
            
            # the path to the geodatabase
            gdb = glob.glob(os.path.join(DATA_DIR + '/' + os.path.basename(URL.format(date_str)).split('.')[0], '*.gdb'))[0]
            
            logging.info("Data of {} successfully fetched.".format(date_str))

        except Exception as e: 
            fetch_exception = e
            # use the previous month in the next attempt 
            first = date.replace(day=1)
            date = first - datetime.timedelta(days=1)
        
        else: 
            break
    
    else: 
        logging.info('Failed to fetch data.')
        raise fetch_exception

    
    return gdb

def delete_carto_entries(id_list):
    '''
    Delete entries in Carto table based on values in a specified column
    INPUT   id_list: list of column values for which you want to delete entries in table (list of strings)
    RETURN  number of ids deleted (number)
    '''
    # generate empty variable to store WHERE clause of SQL query we will send
    where = None
    # column: column name where you should search for these values 
    column = UID_FIELD
    # go through each ID in the list to be deleted
    for delete_id in id_list:
        # if we already have values in the SQL query, add the new value with an OR before it
        if where:
            where += f" OR {column} = '{delete_id}'"
        # if the SQL query is empty, create the start of the WHERE clause
        else:
            where = f"{column} = '{delete_id}'"
        # if where statement is long or we are on the last id, delete rows
        # the length of 15000 was chosen arbitrarily - all the IDs to be deleted could not be sent at once, but no
        # testing was done to optimize this value
        if len(where) > 15000 or delete_id == id_list[-1]:
            cartosql.deleteRows(CARTO_TABLE, where=where, user=CARTO_USER,
                                key=CARTO_KEY)
            # after we have deleted a set of rows, start over with a blank WHERE clause for the SQL query so we don't
            # try to delete rows we have already deleted
            where = None
    
    return len(id_list)

def convert_geometry(geom):
    '''
    Function to convert shapely geometries to geojsons
    INPUT   geom: shapely geometry 
    RETURN  output: geojson 
    '''
    # if it's a polygon
    if geom.geom_type == 'Polygon':
        return geom.__geo_interface__
    # if it's a multipoint series containing only one point
    elif (geom.geom_type == 'MultiPoint') & (len(geom.geoms) == 1):
        return geom.geoms[0].__geo_interface__
    else:
        return geom.__geo_interface__

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
            value = rapidjson.dumps(value)
            return "ST_SetSRID(ST_GeomFromGeoJSON('{}'),4326)".format(value)
    elif dtype in ('text', 'timestamp', 'varchar'):
        # quote strings, escape quotes, and drop nbsp
        return "'{}'".format(
            str(value).replace("'", "''"))
    else:
        return str(value)

def _dumpRows(rows, dtypes):
    '''Escapes rows of data to SQL strings'''
    """ escaped = [_escapeValue(rows[i], dtypes[i]) for i in range(len(dtypes))] """

    escaped = ''
    for i in range(len(dtypes)):
        if i == 0:
            escaped += _escapeValue(rows[i], dtypes[i])
        else:
            escaped += ',' + _escapeValue(rows[i], dtypes[i])
    return '({})'.format(escaped)

def upload_to_carto(row):
    '''
    Function to upload data to the Carto table 
    INPUT   row: the geopandas dataframe of data we want to upload (geopandas dataframe)
    RETURN  the wdpa_pid of the row just uploaded
    '''
    # replace all null values with None
    row = row.where(row.notnull(), None)
    # convert the geometry in the geometry column to geojsons
    row['geometry'] = convert_geometry(row['geometry'])
    # construct the sql query to upload the row to the carto table
    fields = CARTO_SCHEMA.keys()

    # maximum attempts to make
    n_tries = 4
    # sleep time between each attempt   
    retry_wait_time = 6
    values = _dumpRows(row.values.tolist(), tuple(CARTO_SCHEMA.values()))
    
    insert_exception = None
    payload = {
        'api_key': CARTO_KEY,
        'q': 'INSERT INTO "{}" ({}) VALUES {}'.format(CARTO_TABLE, ', '.join(fields), values)
        }
    del values
    for i in range(n_tries):
        try:
            # send the sql query to the carto API 
            r = session.post('https://{}.carto.com/api/v2/sql'.format(CARTO_USER), json=payload)
            r.raise_for_status()
        except Exception as e: # if there's an exception do this
            insert_exception = e
            if r.status_code != 429:
                logging.error(r.content)
            logging.warning('Attempt #{} to upload row #{} unsuccessful. Trying again after {} seconds'.format(i, row['WDPA_PID'], retry_wait_time))
            logging.debug('Exception encountered during upload attempt: '+ str(e))
            time.sleep(retry_wait_time)
        else: # if no exception do this
            return row['WDPA_PID']
    else:
        # this happens if the for loop completes, ie if it attempts to insert row n_tries times
        logging.error('Upload of row #{} has failed after {} attempts'.format(row['WDPA_PID'], n_tries))
        logging.error('Problematic row: '+ str(row))
        logging.error('Raising exception encountered during last upload attempt')
        logging.error(insert_exception)
        raise insert_exception

def processData():
    '''
    Fetch, process, upload, and clean new data
    RETURN  all_ids: a list storing all the wdpa_pids in the current dataframe (list of strings)
    '''
    # fetch the path to the unzipped geodatabase folder
    gdb = fetch_data()
    # whether we have reached the last slice 
    last_slice = False
    # the index of the first row we want to import from the geodatabase
    start = -100
    # the number of rows we want to fetch and process each time 
    step = 100
    # the row after the last one we want to fetch and process
    end = None
    # create an empty list to store all the wdpa_pids 
    all_ids = []

    gdf = gpd.read_file(gdb, driver='FileGDB', layer = 0, encoding='utf-8', rows = slice(-75000, -74500))
    # deal with the large geometries first 
    if '555643543' in gdf['WDPA_PID'].to_list():
        # isolate the large polygon
        gdf_large = gdf.loc[gdf['WDPA_PID'] =='555643543']
        # get rid of the \r\n in the wdpa_pid column 
        gdf_large['WDPA_PID'] = [x.split('\r\n')[0] for x in gdf_large['WDPA_PID']]
        # create a new column to store the status_yr column as timestamps
        gdf_large.insert(19, "legal_status_updated_at", [None if x == 0 else datetime.datetime(x, 1, 1) for x in gdf_large['STATUS_YR']])
        gdf_large["legal_status_updated_at"] = gdf_large["legal_status_updated_at"].astype(object)
    
        # first upload the polygon to carto
        upload_to_carto(gdf_large.iloc[0])
        logging.info('Large geometry upload completed!')
        all_ids.append('555643543')
        
    for i in range(0, 100000000):
        # import a slice of the geopandas dataframe 
        gdf = gpd.read_file(gdb, driver='FileGDB', layer = 0, encoding='utf-8', rows = slice(start, end))
        # get rid of the \r\n in the wdpa_pid column 
        gdf['WDPA_PID'] = [x.split('\r\n')[0] for x in gdf['WDPA_PID']]
        # create a new column to store the status_yr column as timestamps
        gdf.insert(19, "legal_status_updated_at", [None if x == 0 else datetime.datetime(x, 1, 1) for x in gdf['STATUS_YR']])
        gdf["legal_status_updated_at"] = gdf["legal_status_updated_at"].astype(object)
        logging.info('Process {} rows starting from the {}th row as a geopandas dataframe.'.format(step, start))

        # create an empty list to store the ids of large polygons
        large_ids = []
        with ThreadPoolExecutor(max_workers = 8) as executor:
            futures = []
            for index, row in gdf.iterrows():
                # for each row in the geopandas dataframe, submit a task to the executor to upload it to carto 
                if row['WDPA_PID'] not in all_ids: 
                    if row['geometry'].length > 300:
                        large_ids.append(row['WDPA_PID'])
                    else: 
                        futures.append(
                            executor.submit(
                                upload_to_carto, row)
                                )

            for future in as_completed(futures):
                all_ids.append(future.result())

        for index, row in gdf.loc[gdf['WDPA_PID'].isin(large_ids)].iterrows():
            logging.info('Processing large polygon of id {}'.format(row['WDPA_PID']))
            upload_to_carto(row)
            logging.info('Large polygon of id {} uploaded'.format(row['WDPA_PID']))
            all_ids.append(row['WDPA_PID'])

        # if the number of rows is equal to the size of the slice 
        if gdf.shape[0] == step:
            # move to the next slice
            end = start 
            start -= step

        elif gdf.shape[0] == 0 and last_slice == False:
            # we may have reached the last slice 
            start = 0
            last_slice = True
        else:
            # we've processed the whole dataframe 
            break

    return(all_ids)

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

    # Check if table exists, create it if it does not
    logging.info('Checking if table exists and getting existing IDs.')
    # fetch the existing ids in the carto table 
    existing_ids = checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD)

    # number of rows deleted
    deleted_ids = 0
    # clear the table before starting, if specified
    if CLEAR_TABLE_FIRST:
        logging.info('Clearing Table')
        # if the table exists
        if cartosql.tableExists(CARTO_TABLE, user=CARTO_USER, key=CARTO_KEY):
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = []
                for i in range(0, len(existing_ids), 500):
                    # loop through the existing ids to remove all rows from the table in chunks of size 500 
                    futures.append(
                        executor.submit(delete_carto_entries, existing_ids[i: i + 500])
                    )
                # sum the numbers of rows deleted
                for future in as_completed(futures):
                    deleted_ids += future.result()

            logging.info('{} rows of old records removed!'.format(deleted_ids))
            # note: we do not delete the entire table because this will cause the dataset visualization on Resource Watch
            # to disappear until we log into Carto and open the table again. If we simply delete all the rows, this
            # problem does not occur

    # Fetch, process, and upload the new data
    logging.info('Fetching and processing new data')
    # The total number of rows in the Carto table
    num_new = len(processData())
    logging.info('Previous rows: {},  Current rows: {}'.format(len(existing_ids), num_new))

    # Update Resource Watch
    updateResourceWatch(num_new)

    # Delete local files in Docker container
    delete_local()

    logging.info('SUCCESS')