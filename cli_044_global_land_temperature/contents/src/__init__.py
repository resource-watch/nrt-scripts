import logging
import sys
import os
import time
from collections import OrderedDict
import cartosql
import requests
from requests.auth import HTTPBasicAuth
import datetime
from bs4 import BeautifulSoup

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = False

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# Earthdata username and API key for account to access source url
EARTHDATA_USER = os.getenv('EARTHDATA_USER')
EARTHDATA_KEY = os.getenv('EARTHDATA_KEY')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'cli_044_global_land_temperature'

# column of table that can be used as a unique ID (UID)
UID_FIELD = 'date'

# column that stores datetime information
TIME_FIELD = 'date'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA = OrderedDict([
        ('date', 'timestamp'),
        ('no_smoothing', 'numeric'),
        ('lowess_5_smoothing', 'numeric')
    ])

# how many rows can be stored in the Carto table before the oldest ones are deleted?
MAX_ROWS = 1000000

# url for Land-Ocean Temperature Index dataset
SOURCE_URL = 'https://climate.nasa.gov/vital-signs/global-temperature/'

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = '917f1945-fff9-4b6f-8290-4f4b9417079e'

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

def deleteExcessRows(table, max_rows, time_field):
    ''' 
    Delete rows to bring count down to max_rows
    INPUT   table: name of table in Carto from which we will delete excess rows (string)
            max_rows: maximum rows that can be stored in the Carto table (integer)
            time_field: column that stores datetime information (string) 
    RETURN  num_dropped: number of rows that have been dropped from the table (integer)
    ''' 
    # initialize number of rows that will be dropped as 0
    num_dropped = 0
    # get cartodb_ids from carto table sorted by date (new->old)
    r = cartosql.getFields('cartodb_id', table, order='{} desc'.format(time_field),
                           f='csv', user=CARTO_USER, key=CARTO_KEY)
    # turn response into a list of strings of the ids
    ids = r.text.split('\r\n')[1:-1]

    # if number of rows is greater than max_rows, delete excess rows
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[max_rows:], CARTO_USER, CARTO_KEY)
        # get the number of rows that have been dropped from the table
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))

def fetchDataFileName(url):
    ''' 
    Get the link from source url for which we want to download data
    INPUT   url: source url to download data (string)
    RETURN  https_link: link for source data (string)
    '''  
    # pull website content from the source url where data for arctic ice mass is stored
    r = requests.get(url, auth=HTTPBasicAuth(EARTHDATA_USER, EARTHDATA_KEY), stream=True)
    # use BeautifulSoup to read the content as a nested data structure
    soup = BeautifulSoup(r.text, 'html.parser')
    # create a boolean variable which will be set to "True" once the desired file is found
    already_found = False

    # extract all the <a> tags within the html content. The <a> tags are used to mark links, so 
    # we will be able to find the files available for download marked with these tags.
    links = soup.findAll('a')
    # There are some anchors (<a> tags) without href attribute
    # first filter your links for the existence of the href attribute
    # https://stackoverflow.com/questions/52398738/python-sort-to-avoid-keyerror-href?noredirect=1&lq=1
    links = filter(lambda x: x.has_attr('href'), links)
    # loop through each link to find the link for Land-Ocean Temperature Index data
    for item in links:
        # if one of the links available to download is a text file 
        # & contains the word 'gistemp'
        if item['href'].endswith(".txt") and 'gistemp' in item['href']:
            if already_found:
                logging.warning("There are multiple links which match criteria, passing most recent")
            # get the link   
            https_link = item['href']
            # set this variable to "True" since we found the desired file
            already_found = True
    if already_found:
        # if successful, log that the link was found successfully
        logging.info("Selected https: {}".format(https_link))
    else:
        # if unsuccessful, log an error that the link was not found
        logging.warning("No valid link found")

    return(https_link)

def tryRetrieveData(url, resource_location, timeout=300, encoding='utf-8'):
    ''' 
    Download data from the source
    INPUT   url: source url to download data (string)
            resource_location: link for source data (string)
            timeout: how many seconds we will wait to get the data from url (integer) 
            encoding: encoding of the url content (string)
    RETURN  res_rows: list of lines in the source data file (list of strings)
    '''  
    # set the start time as the current time so that we can time how long it takes to pull the data 
    # (returns the number of seconds passed since epoch)
    start = time.time()
    # elapsed time is initialized with zero
    elapsed = 0

    # try to fetch data from generated url while elapsed time is less than the allowed time
    while elapsed < timeout:
        # measures the elapsed time since start
        elapsed = time.time() - start
        try:
            with requests.get(resource_location, auth=HTTPBasicAuth(EARTHDATA_USER, EARTHDATA_KEY), stream=True) as f:
                # split the lines at line boundaries and get the original string from the encoded string
                res_rows = f.content.decode(encoding).splitlines()
                return(res_rows)
        except:
            logging.error("Unable to retrieve resource on this attempt.")
            # if the request fails, wait 5 seconds before moving on to the next attempt to fetch the data
            time.sleep(5)
    # after failing to fetch data within the allowed time, log that the data could not be fetched
    logging.error("Unable to retrive resource before timeout of {} seconds".format(timeout))

    return([])

def insertIfNew(newUID, newValues, existing_ids, new_data):
    '''
    For data pulled from the source data file, check whether it is already in our table. If not, add it to the queue for processing
    INPUT   newUID: date for the current row of data (string)
            newValues: date, unsmoothed temperature change and Lowess smoothed temperature change for current row of data (list of strings)
            existing_ids: list of date IDs that we already have in our Carto table (list of strings)
            new_data: dictionary of new data to be added to Carto, in which the key is the date and the value is a list of strings 
            containing date, unsmoothed temperature change and Lowess smoothed (five year) temperature change for new data (dictionary)
    RETURN  new_data: updated dictionary of new data to be added to Carto, in which the input newValues have been added (dictionary)
    '''
    # get dates that are already in the table along with the new dates that are already processed
    seen_ids = existing_ids + list(new_data.keys())
    # if the current new date is not in the existing table and has not processed yet, add it to the dictionary of new data
    if newUID not in seen_ids:
        new_data[newUID] = newValues
        logging.debug("Adding {} data to table".format(newUID))
    else:
        logging.debug("{} data already in table".format(newUID))

    return(new_data)   

def processData(url, existing_ids, date_format='%Y-%m-%d %H:%M:%S'):
    '''
    Fetch, process and upload new data
    INPUT   url: url where you can find the download link for the source data (string)
            existing_ids: list of date IDs that we already have in our Carto table (list of strings)
            date_format: format of dates in Carto table (string)
    RETURN  num_new: number of rows of new data sent to Carto table (integer)
    '''
    # initialize variable to store number of new rows sent to Carto
    num_new = 0
    # Get the link from source url for which we want to download data
    resource_location = fetchDataFileName(url)
    # get the data from source as a list of strings, with each string holding one line from the source data file
    res_rows = tryRetrieveData(url, resource_location)
    # create an empty dictionary to store new data (data that's not already in our Carto table)
    new_data = {}
    # remove headers by deleting first five rows
    # !!!I TRIED TO FIND MORE POLISHED WAY TO ACHIEVE THIS BUT NONE OF THEM WERE WORKING CORRECTLY!!!
    res_rows = res_rows[5:]

    # go through each line of content retrieved from source
    for row in res_rows:
        # split line by space to get each columns as separate elements
        row = row.split()
        logging.debug("Processing row: {}".format(row))
        # get year from the first column
        year = int(row[0])
        # construct datetime object using year and first day of January 
        date_obj = datetime.datetime(year, 1, 1)
        # convert datetime object to string formatted according to date_pattern
        date = date_obj.strftime(date_format)
        # get unsmoothed temperature change by accessing the second column 
        no_smoothing = float(row[1])
        # get five year Lowess smoothed temperature change by accessing the last column
        lowess_smoothing = float(row[-1])
        # store all the variables into a list
        values = [date, no_smoothing, lowess_smoothing]
        # For new date, check whether this is already in our table. 
        # If not, add it to the queue for processing
        new_data = insertIfNew(date, values, existing_ids, new_data)

    # if we have found new data to process
    if len(new_data):
        num_new += len(new_data)
        # create a list of new data
        new_data = list(new_data.values())
        # insert new data into the carto table
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), new_data, user=CARTO_USER, key=CARTO_KEY)

    return(num_new)

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
    logging.info('Fetching new data')
    num_new = processData(SOURCE_URL, existing_ids)
    logging.info('Previous rows: {},  New rows: {}'.format(len(existing_ids), num_new))

    # Delete data to get back to MAX_ROWS
    deleteExcessRows(CARTO_TABLE, MAX_ROWS, TIME_FIELD)

    # Update Resource Watch
    updateResourceWatch(num_new)

    logging.info("SUCCESS")
