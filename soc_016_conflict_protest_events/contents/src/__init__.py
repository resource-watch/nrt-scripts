from io import DEFAULT_BUFFER_SIZE
import os
import logging
import sys
from collections import OrderedDict
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import cartosql
import requests
import json
import time
import geopandas as gpd
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = True

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# API key and username for ACLED 
ACLED_KEY = os.getenv('ACLED_KEY')
ACLED_USER = os.getenv('ACLED_USER')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'soc_016_conflict_protest_events_count'

# initiate a request session
session = requests.Session()

# name of the table in Carto that stores administrative boundaries
CARTO_GEO = 'wpsi_adm2_counties_display'

# column that stores the unique ids
UID_FIELD = 'objectid'

# column that stores time in the point dataset
TIME_FIELD = 'event_date'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA = OrderedDict([('cartodb_id', 'text'), 
('engtype_1', 'text'), 
('engtype_2', 'text'), 
('the_geom', 'geometry'), 
('gid_0', 'text'), 
('gid_1', 'text'), 
('gid_2', 'text'), 
('name_0', 'text'), 
('name_1', 'text'), 
('name_2', 'text'), 
('objectid', 'text'), 
('shape_area', 'numeric'), 
('shape_leng', 'numeric'),
('battles', 'numeric'),
('protests', 'numeric'),
('riots', 'numeric'), 
('strategic_developments', 'numeric'),
('total', 'numeric')])

# url for armed conflict location & event data
SOURCE_URL = 'https://api.acleddata.com/acled/read/?key={key}&email={user}&event_date={date_start}|{date_end}&event_date_where=BETWEEN&page={page}'

# minimum pages to process
MIN_PAGES = 1

# maximum pages to process
MAX_PAGES = 800

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = 'ea208a8b-4559-434b-82ee-95e041596a3a'

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

'''
FUNCTIONS FOR THIS DATASET

The functions below have been tailored to this specific dataset.
They should all be checked because their format likely will need to be changed.
'''

def processNewData(src_url):
    '''
    Fetch, process and upload new data
    INPUT   src_url: unformatted url where you can find the source data (string)
            existing_ids: list of unique IDs that we already have in our Carto table (list of strings)
    RETURN  new_ids: list of unique ids of new data sent to Carto table (list of strings)
    '''
    # get the range of dates we will be fetching data from 
    date_start = get_date_range()[0]
    date_end = get_date_range()[1]
    # specify the page of source url we want to pull
    # initialize at 0 so that we can start pulling from page 1 in the loop
    page = 0
    # length (number of rows) of new_data
    # initialize at 1 so that the while loop works during first step
    new_count = 1
    # create an empty list to store ids
    new_ids = []
    # create an empty dataframe to store data
    data_df = pd.DataFrame()
    # get and parse each page; stop when no new results or max pages
    # process up to MIN_PAGES even if there are no new results from them
    while page <= MIN_PAGES or new_count and page < MAX_PAGES:
        try:
            # increment page number in every loop
            page += 1
            logging.info("Fetching page {}".format(page))
            # create an empty list to store data
            new_rows = []
            # generate the url and pull data for this page 
            r = requests.get(src_url.format(key=ACLED_KEY, user=ACLED_USER, date_start=date_start, date_end=date_end, page=page))
            # columns of the pandas dataframe
            """  cols = ["data_id", "event_date", "year", "time_precision", "event_type", "sub_event_type", "actor1", "assoc_actor_1", "inter1", 
            "actor2", "assoc_actor_2", "inter2", "interaction", "country", "iso3", "region", "admin1", "admin2", "admin3", "location", 
            "geo_precision", "time_precision", "source", "source_scale", "notes", "fatalities", "latitude", "longitude"] """
            cols = ['event_type', 'latitude', 'longitude']
            # pull data from request response json
            for obs in r.json()['data']:
                # if the id doesn't already exist in Carto table or 
                # isn't added to the list for sending to Carto yet 
                if obs['data_id'] not in new_ids:
                    # append the id to the list for sending to Carto 
                    new_ids.append(obs['data_id'])
                    # create an empty list to store data from this row
                    row = []
                    # go through each column in the Carto table
                    for col in cols:
                        try:
                            # add data for remaining fields to the list of data from this row
                            row.append(obs[col])
                        except:
                            logging.debug('{} not available for this row'.format(col))
                            # if the column we are trying to retrieve doesn't exist in the source data, store blank
                            row.append('')

                    # add the list of values from this row to the list of new data
                    new_rows.append(row)
            
            # number of new rows added in this page 
            new_count = len(new_rows)
            # append the new rows to the pandas dataframe
            data_df = data_df.append(pd.DataFrame(new_rows, columns=cols))

        except:
            logging.error('Could not fetch or process page {}'.format(page))

    # convert the pandas dataframe to a geopandas dataframe
    data_gdf = gpd.GeoDataFrame(data_df, geometry=gpd.points_from_xy(data_df.longitude, data_df.latitude))
   
    # get the ids of polygons from the carto table storing administrative areas
    r = cartosql.getFields('objectid', CARTO_GEO, f='csv', post=True, user=CARTO_USER, key=CARTO_KEY)
    
    # turn the response into a list of ids
    geo_id = r.text.split('\r\n')[1:-1]
    
    # create an empty list to store the ids of rows uploaded to Carto
    uploaded_ids = []

    # number of rows in each slice 
    slice = 1000 

    # access the administrative areas in slices 
    for i in range(0, len(geo_id) - slice, slice):
        # create a geopandas dataframe for the adminstrative area geometries
        geo_gdf = get_admin_area(CARTO_GEO, geo_id[i:i+slice])
        # spatial join the two dataframes to get the number of points per polygon
        joined = spatial_join(data_gdf, geo_gdf)
        # convert the geometry column to geojson 
        joined['geometry'] = [convert_geometry(geom) for geom in joined.geometry]
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for index, row in joined.iterrows():
                # for each row in the geopandas dataframe, submit a task to the executor to upload it to carto 
                if row['objectid'] not in uploaded_ids: 
                    futures.append(
                        executor.submit(
                            upload_to_carto, row)
                            )
            for future in as_completed(futures):
                uploaded_ids.append(future.result())
        logging.info('{} of rows uploaded to Carto.'.format(slice))

    return uploaded_ids

def convert_geometry(geom):
    '''
    Function to convert shapely geometries to geojsons
    INPUT   geom: shapely geometry 
    RETURN  output: geojson 
    '''
    return geom.__geo_interface__

def upload_to_carto(row):
    '''
    Function to upload data to the Carto table 
    INPUT   row: the geopandas dataframe of data we want to upload (geopandas dataframe)
    RETURN  the objectid of the row just uploaded
    '''
    # maximum attempts to make
    n_tries = 4
    # sleep time between each attempt   
    retry_wait_time = 6

    insert_exception = None

    # construct the sql query to upload the row to the carto table
    fields = CARTO_SCHEMA.keys()
    values = cartosql._dumpRows([row.values.tolist()], tuple(CARTO_SCHEMA.values()))

    payload = {
        'api_key': CARTO_KEY,
        'q': 'INSERT INTO "{}" ({}) VALUES {}'.format(CARTO_TABLE, ', '.join(fields), values)
        }

    for i in range(n_tries):
        try:
            # send the sql query to the carto API 
            r = session.post('https://{}.carto.com/api/v2/sql'.format(CARTO_USER), json=payload)
            r.raise_for_status()
        except Exception as e: # if there's an exception do this
            insert_exception = e
            try:
                logging.error(r.content)
            except:
                pass
            logging.warning('Attempt #{} to upload row #{} unsuccessful. Trying again after {} seconds'.format(i, row['objectid'], retry_wait_time))
            logging.debug('Exception encountered during upload attempt: '+ str(e))
            time.sleep(retry_wait_time)
        else: # if no exception do this
            return row['objectid']
    else:
        # this happens if the for loop completes, ie if it attempts to insert row n_tries times
        logging.error('Upload of row #{} has failed after {} attempts'.format(row['objectid'], n_tries))
        logging.error('Problematic row: '+ str(row))
        logging.error('Raising exception encountered during last upload attempt')
        logging.error(insert_exception)
        raise insert_exception
    
    return row['objectid']

def get_admin_area(admin_table, id_list):
    '''
    Delete entries in Carto table based on values in a specified column
    INPUT   admin_table: the name of the carto table storing the administrative areas (string)
            id_list: list of ids for rows to fetch from the table (list of strings)
    RETURN  data fetched from the table (geopandas dataframe)
    '''
    # generate empty variable to store WHERE clause of SQL query we will send
    where = None
    # column: column name where you should search for these values 
    column = 'objectid'
    # go through each ID in the list to be deleted
    for id in id_list:
        # if we already have values in the SQL query, add the new value with an OR before it
        if where:
            where += f" OR {column} = '{id}'"
        # if the SQL query is empty, create the start of the WHERE clause
        else:
            where = f"{column} = '{id}'"
    
    sql = 'SELECT * FROM "{}" WHERE {}'.format(admin_table, where)
    r = cartosql.sendSql(sql, user=CARTO_USER, key=CARTO_KEY, f = 'GeoJSON', post=True)
    data = r.json()
    admin_gdf = gpd.GeoDataFrame.from_features(data)

    return admin_gdf

def spatial_join(gdf_pt, gdf_poly):
    # spatial join the two geopandas dataframes
    dfsjoin = gpd.sjoin(gdf_poly, gdf_pt)
    # count the number of points per administrative area 
    pt_count = dfsjoin.groupby(['objectid', 'event_type']).size().reset_index(name='counts')
    # convert the dataframe from long to wide form 
    pt_count = pd.pivot_table(pt_count, index = 'objectid', columns='event_type')
    # clean up the column names to match the naming requirements of Carto 
    pt_count.columns = [x.lower().replace(' ', '_') for x in pt_count.columns.droplevel(0)]
    # merge the counts to the administrative area dataframe
    pt_poly = gdf_poly.merge(pt_count, how='left', on='objectid')
    # replace NaN in the columns with zeros 
    pt_poly[pt_count.columns] = pt_poly[pt_count.columns].fillna(value = 0)
    # make sure there is one column for each event type
    for event_type in ['battles', 'protests', 'riots', 'strategic_developments']:
        if event_type not in pt_poly.columns:
            pt_poly[event_type] = 0
    # add a column to store the sum of number of events 
    pt_poly['total'] = pt_poly['battles'] + pt_poly['protests'] + pt_poly['riots'] + pt_poly['strategic_developments']

    return pt_poly


def get_date_range():
    date_end = datetime.date.today()
    date_start = date_end + relativedelta(months=-12)
    date_end = date_end.strftime("%Y-%m-%d")
    date_start = date_start.strftime("%Y-%m-%d")

    return date_start, date_end

def get_most_recent_date(table):
    '''
    Find the most recent date of data in the specified Carto table
    INPUT   table: name of table in Carto we want to find the most recent date for (string)
    RETURN  most_recent_date: most recent date of data in the Carto table, found in the TIME_FIELD column of the table (datetime object)
    '''
    # get dates in TIME_FIELD column
    r = cartosql.getFields(TIME_FIELD, table, f='csv', post=True, user=CARTO_USER, key=CARTO_KEY)
    # turn the response into a list of dates
    dates = r.text.split('\r\n')[1:-1]
    # sort the dates from oldest to newest
    dates.sort()
    # turn the last (newest) date into a datetime object
    most_recent_date = datetime.datetime.strptime(dates[-1], '%Y-%m-%d %H:%M:%S')

    return most_recent_date

def create_headers():
    '''
    Create headers to perform authorized actions on API

    '''
    return {
        'Content-Type': "application/json",
        'Authorization': "{}".format(os.getenv('apiToken')),
    }

def pull_layers_from_API(dataset_id):
    '''
    Pull dictionary of current layers from API
    INPUT   dataset_id: Resource Watch API dataset ID (string)
    RETURN  layer_dict: dictionary of layers (dictionary of strings)
    '''
    # generate url to access layer configs for this dataset in back office
    rw_api_url = 'https://api.resourcewatch.org/v1/dataset/{}/layer?page[size]=100'.format(dataset_id)
    # request data
    r = requests.get(rw_api_url)
    # convert response into json and make dictionary of layers
    layer_dict = json.loads(r.content.decode('utf-8'))['data']
    return layer_dict

def update_layer(layer, title):
    '''
    Update layers in Resource Watch back office.
    INPUT   layer: layer that will be updated (string)
            title: current title of the layer (string)
    '''
    # get current date being used from title by string manupulation
    old_date_text = title.split(' ACLED')[0]

    # get current date
    current_date = datetime.datetime.now()    
    # get text for new date end which will be the current date
    new_date_end = current_date.strftime("%B %d, %Y")
    # get most recent starting date, 30 days ago
    new_date_start = (current_date - datetime.timedelta(days=29))
    new_date_start = datetime.datetime.strftime(new_date_start, "%B %d, %Y")
    # construct new date range by joining new start date and new end date
    new_date_text = new_date_start + ' - ' + new_date_end
    
    # replace date in layer's title with new date
    layer['attributes']['name'] = layer['attributes']['name'].replace(old_date_text, new_date_text)

    # send patch to API to replace layers
    # generate url to patch layer
    rw_api_url_layer = "https://api.resourcewatch.org/v1/dataset/{dataset_id}/layer/{layer_id}".format(
        dataset_id=layer['attributes']['dataset'], layer_id=layer['id'])
    # create payload with new title and layer configuration
    payload = {
        'application': ['rw'],
        'name': layer['attributes']['name']
    }
    # patch API with updates
    r = requests.request('PATCH', rw_api_url_layer, data=json.dumps(payload), headers=create_headers())
    # check response
    # if we get a 200, the layers have been replaced
    # if we get a 504 (gateway timeout) - the layers are still being replaced, but it worked
    if r.ok or r.status_code==504:
        logging.info('Layer replaced: {}'.format(layer['id']))
    else:
        logging.error('Error replacing layer: {} ({})'.format(layer['id'], r.status_code))
        
def updateResourceWatch(num_new):
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date' and updating any dates on layers
    INPUT   num_new: number of new rows in Carto table (integer)
    '''
    # If there are new entries in the Carto table
    if num_new>0:
        # Update dataset's last update date on Resource Watch
        most_recent_date = get_most_recent_date(CARTO_TABLE)
        lastUpdateDate(DATASET_ID, most_recent_date)
        # Update the dates on layer legends
        logging.info('Updating {}'.format(CARTO_TABLE))
        # pull dictionary of current layers from API
        layer_dict = pull_layers_from_API(DATASET_ID)
        # go through each layer, pull the definition and update
        for layer in layer_dict:
            # get current layer titile
            cur_title = layer['attributes']['name'] 
            # get layer description
            lyr_dscrptn = layer['attributes']['description']       
            # if we are processing the layer that shows latest 30 days data
            if lyr_dscrptn.startswith('Records of violence and protests from the past 30 days'):
                # replace layer title with new dates
                update_layer(layer, cur_title)

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # clear the table before starting, if specified
    if CLEAR_TABLE_FIRST:
        logging.info("clearing table")
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

    # Fetch, process, and upload new data
    new_ids = processNewData(SOURCE_URL)
    # find the length of new data that were uploaded to Carto
    num_new = len(new_ids)

    # Update Resource Watch
    #updateResourceWatch(num_new)

    logging.info('SUCCESS')
