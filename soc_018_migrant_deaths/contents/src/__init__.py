import logging
import sys
import requests
import csv
from collections import OrderedDict
import datetime
import cartosql
import os
import json

'''
---------------------------------Important Note--------------------------------------------------
# this data source regularly alternates between names for 'Region'
# any time they use a new name, add it to the REGION_NAMES list
# this script uses whichever of these is in the table as the region so that we don't get an error
---------------------------------Important Note--------------------------------------------------
'''
REGION_NAMES = ['Region', 'Region of Incident']

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = False

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'soc_018_missing_migrants'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA = OrderedDict([
    ('the_geom', 'geometry'),
    ('uid', 'text'),
    ('Reported_Date', 'timestamp'),
    ('Region', 'text'),
    ('Number_Dead', 'numeric'),
    ('Minimum_Estimated_Number_of_Missing', 'numeric'),
    ('Total_Dead_and_Missing', 'numeric'),
    ('Number_of_Survivors', 'numeric'),
    ('Number_of_Females', 'numeric'),
    ('Number_of_Males', 'numeric'),
    ('Number_of_Children', 'numeric'),
    ('Cause_of_Death', 'text'),
    ('Location_Description', 'text'),
    ('Information_Source', 'text'),
    ('Migration_Route', 'text'),
    ('URL', 'text'),
    ('UNSD_Geographical_Grouping', 'text'),
    ('Source_Quality', 'text')
])

# column of table that can be used as an unique ID (UID)
UID_FIELD = 'uid'

# column that stores datetime information
TIME_FIELD = 'Reported_Date'

# how many rows can be stored in the Carto table before the oldest ones are deleted?
MAX_ROWS = 500000

# oldest date that can be stored in the Carto table before we start deleting
MAX_YEARS = 10
MAX_AGE = datetime.datetime.today() - datetime.timedelta(days=365*MAX_YEARS)

# url for Missing Migrants data
SOURCE_URL = "https://missingmigrants.iom.int/global-figures/{year}/csv"

# format of dates in source csv file
INPUT_DATE_FORMAT = '%B %d, %Y'

# format of dates in Carto table
DATE_FORMAT = '%Y-%m-%d'

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = 'a0aecb8d-07ee-42e6-be3d-e5cabf12b0a9'

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

def processData(src_url, existing_ids):
    '''
    Fetch, process and upload new data
    INPUT   src_url: unformatted url where you can find the source data (string)
            existing_ids: list of unique IDs that we already have in our Carto table (list of strings)
    RETURN  new_ids: list of unique ids of new data sent to Carto table (list of strings)
    '''
    # length (number of rows) of new_data
    # initialize at 1 so that the while loop works during first step
    num_new = 1
    # create a datetime object with today's date and get the year
    year = datetime.datetime.today().year
    # create an empty list to store unique ids of new data we will be sending to Carto table
    new_ids = []

    # Retrieve and process new data; continue until the current year is 
    # older than the oldest year allowed in the table, set by the MAX_AGE variable
    while year > MAX_AGE.year and num_new:
        logging.info("Fetching data for {} from {}".format(year, src_url.format(year=year)))
        # generate the url and pull data for the selected year
        res = requests.get(src_url.format(year=year))
        # read the contents of the url as a csv file
        # return a reader object which will iterate over lines in the given csvfile
        csv_reader = csv.reader(res.iter_lines(decode_unicode=True))
        # Get headers as {'key':column#, ...} replacing spaces with underscores
        # get headers from the csv file
        headers = next(csv_reader, None)
        # create an empty dictionary to store column names and their column index in the csv
        idx = {}
        # loop through each column
        for v, k in enumerate(headers):
            # log the column names
            logging.info('column:{}'.format(k))

            # replace spaces in column names with underscores
            # if the column is for the region name (for which the column name regularly alternates),
            # add both possible names for the column, pointing to the same column index number
            if k in REGION_NAMES:
                for name in REGION_NAMES:
                    idx[name.replace(' ', '_')] = v
            else:
                idx[k.replace(' ', '_')] = v
                logging.info('updated column name:{}'.format(k.replace(' ', '_')))
        logging.info(idx.keys())
        # create an empty list to store each row of new data
        new_rows = []
        # iterate over each line in the reader object
        for row in csv_reader:
            # break if there is no data (0 rows)
            if not len(row):
                break
            # break if there is no data for Location_Coordinates
            if not len(row[idx['Location_Coordinates']]):
                break
            # generate unique id from the 'Web_ID' column
            uid = row[idx['Web_ID']]
            # if the id doesn't already exist in Carto table or 
            # isn't added to the list for sending to Carto yet 
            if uid not in existing_ids and uid not in new_ids:
                # append the id to the list for sending to Carto 
                new_ids.append(uid)
                # create an empty list to store data from this row
                new_row = []
                # go through each column in the Carto table
                for field in CARTO_SCHEMA:
                    # if we are fetching data for unique id column
                    if field == UID_FIELD:
                        # add the unique id to the list of data from this row
                        new_row.append(uid)
                    # if we are fetching data for datetime column
                    elif field == TIME_FIELD:
                        # get time from the 'Reported_Date' column set by TIME_FIELD variable
                        date = row[idx[TIME_FIELD]]
                        # create a datetime object from the date string and then
                        # convert the datetime to string in the format set by DATE_FORMAT variable
                        date = datetime.datetime.strptime(date, INPUT_DATE_FORMAT).strftime(DATE_FORMAT)
                        # add datetime information to the list of data from this row
                        new_row.append(date)
                    # if we are fetching data for geometry column
                    elif field == 'the_geom':
                        # get the value from the column 'Location_Coordinates'
                        # coordinates are stored as 19.456996117519, 5.764623476008
                        # remove the space between the values
                        # split the values using comma to get longitude and latitude
                        lon, lat = row[idx['Location_Coordinates']]\
                            .replace(' ', '').split(',')
                        # construct geojson geometry
                        geometry = {
                            'type': 'Point',
                            'coordinates': [float(lon), float(lat)]
                        }
                        # add geojson geometry to the list of data from this row
                        new_row.append(geometry)
                    else:
                        # for all other columns, we can fetch the data using our column name in Carto
                        # if the column we are trying to retrieve doesn't exist in the source data, store None
                        v = None if row[idx[field]] == '' else row[idx[field]]
                        new_row.append(v)
                # add the list of values from this row to the list of new data
                new_rows.append(new_row)
        # find the length (number of rows) of new_data 
        num_new = len(new_rows)
        # check if new data is available
        if num_new:
            logging.info("Inserting {} new rows".format(num_new))
            # insert new data into the carto table
            cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(),
                                 new_rows, user=CARTO_USER, key=CARTO_KEY)
    return new_ids

def deleteExcessRows(table, max_rows, time_field, max_age=''):
    ''' 
    Delete rows that are older than a certain threshold and also bring count down to max_rows
    INPUT   table: name of table in Carto from which we will delete excess rows (string)
            max_rows: maximum rows that can be stored in the Carto table (integer)
            time_field: column that stores datetime information (string) 
            max_age: oldest date that can be stored in the Carto table (datetime object)
    RETURN  num_dropped: number of rows that have been dropped from the table (integer)
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

    return(num_dropped)

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

def update_layer(layer, new_date):
    '''
    Update layers in Resource Watch back office.
    INPUT   layer: layer that will be updated (string)
            new_date: date of asset to be shown in this layer (datetime)
    '''
    # get current layer titile
    title = layer['attributes']['name'] 
    # get layer description
    lyr_dscrptn = layer['attributes']['description']   
    
    # if we are processing the layer that shows latest 60 days data
    if lyr_dscrptn.endswith('past 60 days.'):
        # get current date being used from title by string manupulation
        old_date_text = title.split(' Migrant')[0]
        # get text for new date end which will be the current date
        new_date_end = new_date.strftime("%B %d, %Y")
        # get most recent starting date, 60 days ago
        new_date_start = (new_date - datetime.timedelta(days=59))
        new_date_start = datetime.datetime.strftime(new_date_start, "%B %d, %Y")
        # construct new date range by joining new start date and new end date
        new_date_text = new_date_start + ' - ' + new_date_end
    # if we are processing the layer that shows 2014-present data
    elif lyr_dscrptn.endswith('2014 to the present.'):    
        # get current date being used from title by string manupulation
        old_date_text = title.split(' Number')[0].split(' - ')[1]
        # get text for new date end which will be the current date
        new_date_text = new_date.strftime("%B %d, %Y")
    
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
        most_recent_date = datetime.datetime.utcnow()
        lastUpdateDate(DATASET_ID, most_recent_date)
        # Update the dates on layer legends
        logging.info('Updating {}'.format(CARTO_TABLE))
        # pull dictionary of current layers from API
        layer_dict = pull_layers_from_API(DATASET_ID)
        # go through each layer, pull the definition and update
        for layer in layer_dict:
            # replace layer title with new dates
            update_layer(layer, most_recent_date)

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
    new_ids = processData(SOURCE_URL, existing_ids)
    # find the length of new data that were uploaded to Carto
    num_new = len(new_ids)

    # Delete data to get back to MAX_ROWS
    num_deleted = deleteExcessRows(CARTO_TABLE, MAX_ROWS, TIME_FIELD, MAX_AGE)

    # Update Resource Watch
    updateResourceWatch(num_new)

    logging.info('SUCCESS')
