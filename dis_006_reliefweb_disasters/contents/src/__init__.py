import logging
import sys
import os
from collections import OrderedDict
import cartosql
import requests
import datetime
import json
import hashlib
import time

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = False

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# maximum number of feature we want to retrieve from source url
LIMIT = 1000

# url for source data
SOURCE_URL = "https://api.reliefweb.int/v1/disasters?preset=latest&limit={}&profile=full".format(LIMIT)

# format of date used in Carto table
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S+00:00'

# column of table that can be used as a unique ID (UID)
UID_FIELD='uid'

# column that stores datetime information
AGE_FIELD = 'date'

# how many rows can be stored in the Carto table before the oldest ones are deleted?
MAX_ROWS = 1000000

# name of tables in Carto where we will upload the data
CARTO_TABLE = 'dis_006_reliefweb_disasters'
CARTO_TABLE_INTERACTION = 'dis_006_reliefweb_disasters_interaction'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA = OrderedDict([
    ("the_geom", "geometry"),
    ("uid", "text"),
    ("event_id", "numeric"),
    ("event_name", "text"),
    ("description", "text"),
    ("status", "text"),
    ("date", "timestamp"),
    ("glide", "text"),
    ("related_glide", "text"),
    ("featured", "text"),
    ("primary_country", "text"),
    ("country_name", "text"),
    ("country_shortname", "text"),
    ("country_iso3", "text"),
    ("current", "text"),
    ("event_type_ids", "text"),
    ("event_types", "text"),
    ("url", "text"),
    ("lon", "numeric"),
    ("lat", "numeric")
])

CARTO_SCHEMA_INTERACTION = OrderedDict([
    ("the_geom", "geometry"),
    ("uid", "text"),
    ("country_name", "text"),
    ("country_shortname", "text"),
    ("country_iso3", "text"),
    ("interaction", "text")
])

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = '4919be3a-c543-4964-a224-83ef801370de'

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

def gen_uid(event_id, country_id):
    '''Generate unique id using event_id and country_id.
       Generate an MD5 sum from the formatted string
    INPUT   event_id: root id of each primary event (string)
            country_id: id for the country (string)
    RETURN  unique id for each country affected by this event (string)
    '''
    # join event_id and country_id using an underscore
    id_str = '{}_{}'.format(event_id, country_id)

    return hashlib.md5(id_str.encode('utf8')).hexdigest()

def processData(existing_ids):
    '''
    Fetch, process and upload new data
    INPUT   existing_ids: list of date IDs that we already have in our Carto table (list of strings)
    RETURN  num_new: number of rows of new data sent to Carto table (integer)
    '''
    # create an empty list to store new data (data that's not already in our Carto table)
    new_data = []
    # create an empty list to store unique ids of new data we will be sending to Carto table
    new_ids = []
    # pull data from the url
    r = requests.get(SOURCE_URL)
    # get the contents from the url
    data_bytes=r.content
    # decode the contents using 'utf8' encoding
    decoded=data_bytes.decode('utf8')
    # load the decoded contents as JSON
    json_data = json.loads(decoded)
    # get the 'data' feature from the JSON
    data_dict = json_data['data']
    # loop through each elements in data_dict
    for entry in data_dict:
        # get the root id of each primary event
        event_id =entry['id']
        # create an empty list to store all secondary event ids for this entry
        ids = []
        # create an empty list to store all secondary event names for this entry
        names = []
        # get the types of secondary events
        for t in entry['fields']['type']:
            # append the id, name of each secondary event to the list of 
            # all secondary event ids, names for this entry
            ids.append(t['id'])
            names.append(t['name'])
        # convert each item in the ids list to a string, and then join them
        ids=', '.join(map(str, ids))
        # convert each item in the names list to a string, and then join them
        names=', '.join(map(str, names))
        # loop through each country affected by this event
        for country in entry['fields']['country']:
            # get the id for the country
            country_id = country['id']
            # generate unique id to record information about each country affected 
            # by this event using primary event id and country id
            uid = gen_uid(event_id, country_id)
            # if the id doesn't already exist in Carto table or 
            # isn't added to the list for sending to Carto yet 
            if uid not in existing_ids + new_ids:
                # append the id to the list for sending to Carto 
                new_ids.append(uid)
                # create an empty list to store data from this row
                row = []
                # go through each column in the Carto table
                for key in CARTO_SCHEMA.keys():
                    try:
                        # if we are fetching data for geometry column
                        if key == 'the_geom':
                            # get longitude and latitude from lon, lat features
                            lon = country['location']['lon']
                            lat = country['location']['lat']
                            # construct geometry using latitude an longitude
                            item = {
                                'type': 'Point',
                                'coordinates': [lon, lat]
                            }
                        # if we are fetching data for unique id column
                        elif key=='uid':
                            # get unique id from uid variable that we generated earlier
                            item = uid
                        # if we are fetching data for event id column
                        elif key=='event_id':
                            # get event id from event_id variable and convert it into an integer
                            item = int(event_id)
                        # if we are fetching data for date column
                        elif key=='date':
                            # get the date from 'created' feature and convert it to a datetime object
                            # formatted according to DATETIME_FORMAT
                            item = datetime.datetime.strptime(entry['fields']['date']['created'],DATETIME_FORMAT)
                        # if we are fetching data for related_glide column
                        elif key=='related_glide':
                            # get the data for this column from 'related_glide' feature
                            # convert each item in the list to a string, and then join them
                            item = ', '.join(map(str, entry['fields']['related_glide']))
                        # if we are fetching data for primary_country column
                        elif key=='primary_country':
                            # get the iso3 country code for primary country from 'iso3' feature
                            item = entry['fields']['primary_country']['iso3']
                        # if we are fetching data for featured column 
                        elif key=='featured':
                            # get the data for this column from 'featrued' feature which is a 
                            # binary 'True' or 'False' value and covert it to a string
                            item = str(entry['fields']['featured'])
                        # if we are fetching data for country_name column
                        elif key=='country_name':
                            # get the name of country from 'name' feature
                            item = country['name']
                        # if we are fetching data for country_shortname column
                        elif key=='country_shortname':
                            # get the short name for country from 'shortname' feature
                            item = country['shortname']
                        # if we are fetching data for country_iso3 column
                        elif key=='country_iso3':
                            # get the iso3 country code for secondary countries from 'iso3' feature
                            item = country['iso3']
                        # if we are fetching data for current column
                        elif key== 'current':
                            # get the data for this column from 'current' feature which is a 
                            # binary 'True' or 'False' value and covert it to a string                            
                            item = str(entry['fields']['current'])
                        # if we are fetching data for event_type_ids column
                        elif key == 'event_type_ids':
                            # get secondary event ids from ids list
                            item = ids
                        # if we are fetching data for event types column
                        elif key == 'event_types':
                            # get secondary event names from names list
                            item = names
                        # if we are fetching data for event name column
                        elif key=='event_name':
                            # get the event name from 'name' feature
                            item = entry['fields']['name']
                        # if we are fetching data for coordinates
                        elif key == 'lon' or 'lat':
                            # get the lat, long information from location feature
                            item = country['location'][key]
                        # for all other columns, we can fetch the data from fields feature 
                        # using our column name in Carto
                        else:
                            item = entry['fields'][key]
                    except KeyError:
                        # if the column we are trying to retrieve doesn't exist in the source data, store None
                        item=None
                    # store the retrieved item from this row to a list
                    row.append(item)
                # add the list of values from this row to the list of new data
                new_data.append(row)
    # find the length (number of rows) of new_data            
    num_new = len(new_ids)
    # if we have found new dates to process
    if num_new:
        # insert new data into the carto table
        logging.info('Adding {} new records'.format(num_new))
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), new_data
                                    , user=CARTO_USER, key=CARTO_KEY)
    return(num_new)

def processInteractions():

    # get all the rows from Carto table where the column current is equal to True
    r = cartosql.get("SELECT * FROM {} WHERE current='True'".format(CARTO_TABLE),
                     user=CARTO_USER, key=CARTO_KEY)
    # turn the response into a JSON
    interaction_data = r.json()['rows']
    # initialize number of tries to fetch data as zero
    try_num = 0
    # if we didn't get data back, wait a few minutes and try again
    while not len(interaction_data):
        logging.info('Sleeping and trying again.')
        # increase the count of number of tries
        try_num+=1
        # wait for 300 seconds
        time.sleep(300)
        # turn the response into a JSON
        interaction_data = r.json()['rows']
        # stop trying if we can't get data within five tries
        if try_num >5:
            logging.error('Problem fetching data to generate interactions')
            exit()
    # create an empty list to store country code for countries in interaction_data
    countries_with_interaction=[]
    # loop through each entry in interaction_data 
    for interaction in interaction_data:
        # get the country code from 'country_iso3' feature
        ctry = interaction['country_iso3']
        # if this country code doesn't already exist in countries_with_interaction list,
        # add it to the list
        if ctry not in countries_with_interaction:
            countries_with_interaction.append(ctry)
    # check if the CARTO_TABLE_INTERACTION table already exists in Carto
    if cartosql.tableExists(CARTO_TABLE_INTERACTION, user=CARTO_USER, key=CARTO_KEY):
        # delete all the rows from the table
        cartosql.deleteRows(CARTO_TABLE_INTERACTION, 'cartodb_id IS NOT NULL', user=CARTO_USER, key=CARTO_KEY)
    else:
        # if the table does not exist, create it with columns based on the schema input
        logging.info('Table {} does not exist, creating'.format(CARTO_TABLE_INTERACTION))
        cartosql.createTable(CARTO_TABLE_INTERACTION, CARTO_SCHEMA_INTERACTION, user=CARTO_USER, key=CARTO_KEY)
    # create an empty list to store new data for this table
    new_interactions=[]
    # loop through each entry in countries_with_interaction
    for ctry in countries_with_interaction:
        # get all the rows from Carto table where the column current is equal to True and 
        # country_iso3 is equal to current entry in the loop
        r = cartosql.get("SELECT * FROM {} WHERE current='True' AND country_iso3='{}'".format(CARTO_TABLE, ctry),
                         user=CARTO_USER, key=CARTO_KEY)
        # turn the reponse into a JSON
        ctry_interaction_data = r.json()['rows']
        # initialize the number of interaction to 1
        event_num=1
        # loop through each feature in the ctry_interaction_data
        for interaction in ctry_interaction_data:
            # split 'event_name' on ": " and get the second element after split so that we just get the name of 
            # the event in cases like: "Central and Latin America: Dengue Outbreak - Jun 2019"
            event = interaction['event_name'].split(": ",1)
            # if we are on the first iteration
            if event_num == 1:
                # if there are only one event
                if len(event)==1:
                    # generate interaction to display using first element of event variable and url for the event
                    interaction_str = '{} ({})'.format(event[0], interaction['url'])
                # if there are more than one event
                else:
                    # generate interaction to display using second element of event variable and url for the event
                    interaction_str = '{} ({})'.format(event[1], interaction['url'])
            # for second and future iterations
            else:
                # if there are only one event
                if len(event)==1:
                    # generate interaction to display using interaction_str from previous iterations and adding
                    # first element of event variable and url for the event in current iteration
                    interaction_str = interaction_str + '; ' + '{} ({})'.format(event[0], interaction['url'])
                # if there are more than one event
                else:
                    # generate interaction to display using interaction_str from previous iterations and adding
                    # first element of event variable and url for the event in current iteration
                    interaction_str = interaction_str + '; ' + '{} ({})'.format(event[1], interaction['url'])
            # increase event_num by 1 for next iteration
            event_num+=1

        # if ctry_interaction_data variable contains data
        if ctry_interaction_data:
            # create an empty list to store data from this row
            row = []
            # go through each column in the Carto table
            for key in CARTO_SCHEMA_INTERACTION.keys():
                try:
                    # if we are fetching data for geometry column
                    if key == 'the_geom':
                        # get longitude and latitude from lon, lat features
                        lon = ctry_interaction_data[0]['lon']
                        lat = ctry_interaction_data[0]['lat']
                        # construct geometry using latitude an longitude
                        item = {
                            'type': 'Point',
                            'coordinates': [lon, lat]
                        }
                    # if we are fetching data for interraction column    
                    elif key=='interaction':
                        # get interaction from interaction_str variable
                        item=interaction_str
                    else:
                        # for all other columns, we can fetch the data from ctry_interaction_data 
                        # feature using our column name in Carto
                        item = ctry_interaction_data[0][key]
                except KeyError:
                    # if the column we are trying to retrieve doesn't exist in the source data, store None
                    item=None
                # store the retrieved item from this row to a list
                row.append(item)
            # add the list of values from this row to the list of new_interactions
            new_interactions.append(row)

    logging.info('Adding {} new interactions'.format(len(new_interactions)))
    # insert new data into the carto table
    cartosql.blockInsertRows(CARTO_TABLE_INTERACTION, CARTO_SCHEMA_INTERACTION.keys(), CARTO_SCHEMA_INTERACTION.values(), 
                                new_interactions, user=CARTO_USER, key=CARTO_KEY)

def deleteExcessRows(table, max_rows, age_field):
    ''' 
    Delete rows that are older than a certain threshold 
    INPUT   table: name of table in Carto from which we will delete excess rows (string)
            max_rows: maximum rows that can be stored in the Carto table (integer)
            age_field: column that stores datetime information (string) 
    RETURN  num_dropped: number of rows that have been dropped from the table (integer)
    ''' 
    # initialize number of rows that will be dropped as 0
    num_dropped = 0

    # get cartodb_ids from carto table sorted by date (old->new)
    r = cartosql.getFields('cartodb_id', table, order='{}'.format(age_field.lower()),
                           f='csv')
    # turn response into a list of strings of the ids
    ids = r.text.split('\r\n')[1:-1]

    # if number of rows is greater than max_rows, delete excess rows
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[:-max_rows], user=CARTO_USER, key=CARTO_KEY)
        # get the number of rows that have been dropped from the table
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))

    return num_dropped

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
    existing_ids = checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD)

    # Fetch, process, and upload new data
    logging.info('Fetching new data')
    num_new = processData(existing_ids)
    logging.info('Processing interactions')
    processInteractions()

    # Delete data to get back to MAX_ROWS
    logging.info('Deleting excess rows')
    num_dropped = deleteExcessRows(CARTO_TABLE, MAX_ROWS, AGE_FIELD)

    # Update Resource Watch
    updateResourceWatch(num_new)

    logging.info("SUCCESS")
