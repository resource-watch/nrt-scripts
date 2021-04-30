import os
import logging
import sys
from collections import OrderedDict
import datetime
import cartosql
import requests

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = False

# name of data directory in Docker container
DATA_DIR = 'data'

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'soc_048_organized_violence_events_nrt'

# column of table that can be used as an unique ID (UID)
UID_FIELD = 'uid'

# column that stores datetime information
TIME_FIELD = 'date_start'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA = OrderedDict([
    ("uid", "text"),
    ("the_geom", "geometry"),
    ("date_start", "timestamp"),
    ("date_end", "timestamp"),
    ("active_year", "text"),
    ("code_status", "text"),
    ("type_of_violence", "numeric"),
    ("conflict_dset_id", "text"),
    ("conflict_new_id", "numeric"),
    ("conflict_name", "text"),
    ("dyad_dset_id", "text"),
    ("dyad_new_id", "numeric"),
    ("dyad_name", "text"),
    ("size_a_dset_id", "text"),
    ("size_a_new_id", "numeric"),
    ("side_a", "text"),
    ("side_b_dset_id", "text"),
    ("side_b_new_id", "text"),
    ("side_b", "text"),
    ("number_of_sources", "numeric"),
    ("source_article", "text"),
    ("source_office", "text"),
    ("source_date", "text"),
    ("source_headline", "text"),
    ("source_original", "text"),
    ("where_prec", "numeric"),
    ("where_coordinates", "text"),
    ("where_description", "text"),
    ("adm_1", "text"),
    ("adm_2", "text"),
    ("priogrid_gid", "numeric"),
    ("country", "text"),
    ("country_id", "numeric"),
    ("region", "text"),
    ("event_clarity", "numeric"),
    ("date_prec", "numeric"),
    ("deaths_a", "numeric"),
    ("deaths_b", "numeric"),
    ("deaths_civilians", "numeric"),
    ("deaths_unknown", "numeric"),
    ("best", "numeric"),
    ("high", "numeric"),
    ("low", "numeric"),
    ("gwnoa", "text"),
    ("gwnob", "numeric")
])

# how many rows can be stored in the Carto table before the oldest ones are deleted?
MAXROWS = 1000000

# format of dates in Carto table
DATE_FORMAT = '%Y-%m-%d'

# url for UCDP Georeferenced Event Dataset data
HISTORY_URL = 'http://ucdpapi.pcr.uu.se/api/gedevents/20.1?pagesize=1000&page={page}'

# url for UCDP Georeferenced Event Dataset data formatted for a specific start date
LATEST_URL = 'https://ucdpapi.pcr.uu.se/api/gedevents/20.1?pagesize=1000&page={page}&StartDate=%7B%7B%7B{start_date}%7D%7D%7D'
# Do we want to get historical data?
PROCESS_HISTORY = False

# Specify how many days we want to go back from today's date to search for data
DAYS_TO_LOOK_BACK = 400

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = '9b6e6bce-efce-49a5-b603-385b8dae29e0'

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

def genUID(obs):
    '''Generate unique id using variable 'id' from input json
    INPUT   obs: single row of data in json format (json)
    RETURN unique id for row (string)
    '''
    return str(obs['id'])

def fetchResults(page, start_date=None):
    '''Generate the url and pull data for the selected start time and page
    INPUT   page: page number that we want to search for (string)
            start_date: date from which we want to begin search for data (string)
    RETURN list of data rows for url (list of dictionaries)
    '''
    if PROCESS_HISTORY:
        # if want to get all data to date, don't specify any start_date
        # generate the url and pull data
        return requests.get(HISTORY_URL.format(page=page)).json()['Result']
    else:
        # generate the url and pull data using a starting date specified by the start_date variable
        return requests.get(LATEST_URL.format(page=page, start_date=start_date)).json()['Result']

def genRow(obs):
    '''
    List of new data from each page in the retrieved url
    INPUT   obs: single row of data in json format (json)
    RETURN  row: list of new data from the input row of data (list)
    '''

    # generate unique id from 'id' variable in source data
    uid = genUID(obs)
    # create an empty list to store the processed data for this row that we will send to Carto
    row = []
    # go through each column in the Carto table
    for field in CARTO_SCHEMA.keys():
        # if we are fetching data for geometry column
        if field == 'the_geom':
            # construct geojson geometry
            geom = {
                "type": "Point",
                "coordinates": [
                    obs['longitude'],
                    obs['latitude']
                ]
            }
            # add geojson geometry to the list of data from this row
            row.append(geom)
        # if we are fetching data for unique id column
        elif field == UID_FIELD:
            # add the unique id to the list of data from this row
            row.append(uid)
        else:
            try:
                if obs[field]:
                    # add data for remaining fields to the list of data from this row
                    row.append(obs[field])
                else:
                    logging.debug('Field {} was empty'.format(field))
                    # if the column we are trying to retrieve is empty in the source data, store None
                    row.append(None)
            except:
                logging.debug('{} not available for this row'.format(field))
                # if the column we are trying to retrieve doesn't exist in the source data, store None
                row.append(None)
    return row

def keep_if_new(obs, existing_ids):
    '''
    Check if the unique id already exist in our Carto table
    INPUT   obs: single row of data in json format (json)
            existing_ids: list of unique IDs that we already have in our Carto table (list of strings)
    RETURN    is the unique id a new observation that needs to be sent to the Carto table (boolean)
    '''
    # get the unique id from first variable of 'Result' feature
    # if the id is already in the table, return False (so we can drop this observation)
    if obs[0] in existing_ids:
        return False
    else:
        # if the unique id don't exist in existing_ids, add it
        existing_ids.append(obs[0])
        # return True (so we can keep this observation)
        return True

def processNewData(existing_ids):
    '''
    Fetch, process and upload new data
    INPUT  existing_ids: list of unique IDs that we already have in our Carto table (list of strings)
    RETURN  total_new: number of rows of new data sent to Carto table (integer)
    '''

    # if we want to process all historical data
    if PROCESS_HISTORY:
        # set start date for collecting data
        start_date = '1900-01-01'
        # generate and retrieve the url to get the contents from page 0 and then
        # get the total number of pages from the 'TotalPages' feature
        num_pages = requests.get(HISTORY_URL.format(page=0)).json()['TotalPages']
    else:
        # get the start date by going back number of days set by the "DAYS_TO_LOOK_BACK" variable
        # convert the datetime to string in the format set by DATE_FORMAT variable
        start_date = (datetime.datetime.today() - datetime.timedelta(days=DAYS_TO_LOOK_BACK)).strftime(DATE_FORMAT)
        # generate and retrieve the url to get the contents from page 0 for the specified start date and then
        # get the total number of pages from the 'TotalPages' feature
        num_pages = requests.get(LATEST_URL.format(page=0, start_date=start_date)).json()['TotalPages']

    logging.info('Number of pages: {}'.format(num_pages))

    # set the length (number of rows) of new_data to 0 as starting point
    total_new = 0
    # loop through each of the pages in the retrieved url and process the data
    for page in range(num_pages):
        logging.info('Processing page {}/{}'.format(page, num_pages))
        # generate the url and pull data for the selected interval from current page
        results = fetchResults(page, start_date)
        # get list of new data rows from fetched results
        parsed_rows = map(genRow, results)
        # check if each row already exists in our Carto table
        # if it doesn't, keep it in the list of new rows to send to Carto
        # if it does, drop it from the list of new rows
        # also add it to existing_ids list, since it will be sent to Carto now
        new_rows = list(filter(lambda row: keep_if_new(row, existing_ids), parsed_rows))
        # length (number of rows) of new_data   
        new_count = len(new_rows)
        # if we have found new data to process
        if new_count:
            logging.info('Pushing {} new rows'.format(new_count))
            # insert new data into the Carto table
            cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                                CARTO_SCHEMA.values(), new_rows, user=CARTO_USER, key=CARTO_KEY)
            # add this to the number of rows of new data sent to Carto table
            total_new += new_count

    return total_new

def deleteExcessRows(table, max_rows, time_field, max_age=''):
    ''' 
    Delete rows that are older than a certain threshold and also bring count down to max_rows
    INPUT   table: name of table in Carto from which we will delete excess rows (string)
            max_rows: maximum rows that can be stored in the Carto table (integer)
            time_field: column that stores datetime information (string) 
            max_age(optional): oldest date that can be stored in the Carto table (datetime object)
    ''' 

    # initialize number of rows that will be dropped as 0
    num_dropped = 0
    # check if max_age is a datetime object
    if isinstance(max_age, datetime.datetime):
        # convert max_age to a string 
        max_age = max_age.isoformat()

    # if the max_age variable exists
    if max_age:
        # delete rows from table which are older than the max_age
        r = cartosql.deleteRows(table, "{} < '{}'".format(time_field, max_age), user=CARTO_USER, key=CARTO_KEY)
        # get the number of rows that were dropped from the table
        num_dropped = r.json()['total_rows']

    # get cartodb_ids from carto table sorted by date (new->old)
    r = cartosql.getFields('cartodb_id', table, order='{} desc'.format(time_field),
                           f='csv', user=CARTO_USER, key=CARTO_KEY)
    # turn response into a list of strings of the ids
    ids = r.text.split('\r\n')[1:-1]

    # if number of rows is greater than max_rows, delete excess rows
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[max_rows:], user=CARTO_USER, key=CARTO_KEY)
        # get the number of rows that have been dropped from the table
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))

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

    # Update the dates on layer legends - TO BE ADDED IN FUTURE

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
    existing_ids = checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)

    # Fetch, process, and upload new data
    new_count = processNewData(existing_ids)

    # Remove old observations
    deleteExcessRows(CARTO_TABLE, MAXROWS, TIME_FIELD)

    # Update Resource Watch
    updateResourceWatch(new_count)

    logging.info('SUCCESS')
