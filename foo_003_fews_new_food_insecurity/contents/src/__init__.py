import os
import logging
import sys
import urllib
import zipfile
import datetime
import re
from dateutil.relativedelta import relativedelta
import fiona
from bs4 import BeautifulSoup
from collections import OrderedDict
from shapely import geometry
import cartosql
import requests

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = False

# name of data directory in Docker container
DATA_DIR = './data'

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'foo_003_fews_net_food_insecurity_test'

# column of table that can be used as an unique ID (UID)
UID_FIELD = '_uid'

# column that stores datetime information
TIME_FIELD = 'start_date'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA = OrderedDict([
    ('the_geom', 'geometry'),
    ('admin0', 'text'),
    ('admin1', 'text'),
    ('_uid', 'text'),
    ('start_date', 'timestamp'),
    ('end_date', 'timestamp'),
    ('ifc_type', 'text'),
    ('ifc', 'numeric')
])

# how many rows can be stored in the Carto table before the oldest ones are deleted?
MAXROWS = 1000000

# Format of date used in source files
DATE_FORMAT = "%B-%Y"

# format of dates in Carto table
DATETIME_FORMAT = '%Y%m%dT00:00:00Z'

# url for source data
SOURCE_URL = 'https://fews.net/content/{country}-food-security-classification-{date}'

# oldest date that can be stored in the Carto table before we start deleting
MAXAGE = datetime.datetime.today() - datetime.timedelta(days=365*5)

# these two variables are used as parameters for the shapely simplify function, which simplifies the geometry of the shapefiles
# please read the documentation for further details:
# https://shapely.readthedocs.io/en/stable/manual.html#object.simplify
# all points in the simplified object will be within this tolerance level of difference from the original geometry
SIMPLIFICATION_TOLERANCE = .04
# decide whether to preserve topology of the geometry
# a slower algorithm is used that preserves topology
PRESERVE_TOPOLOGY = True

# minimum number of months we want to check back through for data
MINDATES = 6

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
#DATASET_ID = 'ac6dcdb3-2beb-4c66-9f83-565c16c2c914'

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

# Generate UID
def genUID(date, region, ifc_type, pos_in_shp):
    '''Generate unique id using date, region, time period and feature index in retrieved GeoJSON
    INPUT   date: date for which we want to generate id (string)
            region: region for which we are collecting data (string)
            ifc_type: time period of the data. (string) 
                ifc_type can be: 
                CS = "current status",
                ML1 = "most likely status in next four months"
                ML2 = "most likely status in the following four months"
            pos_in_shp: index of the feature in GeoJSON (integer)
    RETURN unique id for row (string)
    '''
    return str('{}_{}_{}_{}'.format(date, region, ifc_type, pos_in_shp))

def findcountries():
    url = 'https://fews.net/fews-data/333'
    list_countries = []
    with urllib.request.urlopen(url) as f:
        # use BeautifulSoup to read the content as a nested data structure
        soup = BeautifulSoup(f, features="lxml")
        divs = soup.find_all("div", {"class": "countries-filter-container"})
        for div in divs:
            for string in div.strings:
                if string not in ['Global', 'Central America and Caribbean', 'Central Asia', 'East Africa', 'Southern Africa', 'West Africa']:
                    list_countries.append(string)
    
    return list_countries

def findShps(zfile):
    '''
    Check if the zipfile contains all the expected shapefiles and return them as a dictionary
    INPUT   zfile: zipfile containing retrieved data from source url (list of strings)
    RETURN  files: dictionary with ifc_type as key and shapefiles as value (dictionary)
    '''
    # create an empty dictionary to store the path of shapefiles
    files = {}
    # loop through all files in the zipped file
    with zipfile.ZipFile(zfile) as z:
        for f in z.namelist():
            # check if the file is a shapefile
            if os.path.splitext(f)[1] == '.shp':
                # check if the file contains the word 'CS'
                if 'CS' in f:
                    # add the file to the dictionary
                    files['CS'] = f
                # check if the file contains the word 'ML1'
                elif 'ML1' in f:
                    # add the file to the dictionary
                    files['ML1'] = f
                # check if the file contains the word 'ML2'
                elif 'ML2' in f:
                    # add the file to the dictionary
                    files['ML2'] = f
    # if we haven't found all three desired shapefiles, send a log
    if len(files) != 3:
        logging.info('There should be 3 shapefiles: CS, ML1, ML2')

    return files


def simplifyGeom(geom):
    '''
    Return a simplified representation of the geometric object
    INPUT   geom: geojson of current geometry (geojson)
    RETURN  simplified geometry (geojson)
    '''
    # create a shapely geometry from the input geojson
    shp = geometry.shape(geom)
    # simplify the geometry
    simp = shp.simplify(SIMPLIFICATION_TOLERANCE, PRESERVE_TOPOLOGY)

    return geometry.mapping(simp)


def processNewData(existing_ids):
    '''
    Fetch, process and upload new data
    INPUT   existing_ids: list of unique IDs that we already have in our Carto table (list of strings)
    RETURN  num_new: number of rows of new data sent to Carto table (integer)
    '''
    # create an empty list to store unique ids of new data we will be sending to Carto table
    new_ids = []

    # Get today's date and truncate to monthly resolution (this will show the date as the first of the current month)
    date = datetime.datetime.strptime(
        datetime.datetime.today().strftime(DATE_FORMAT), DATE_FORMAT)

    # Retrieve and process new data; continue until the current date is 
    # older than the oldest date allowed in the table, set by the MAX_AGE variable

    for country in findcountries():
        # loop through each country
        # initiate a variable to indicate whether data has been fetched 
        
        while date > MAXAGE:
            # iterate backwards 1 month at a time
            date -= relativedelta(months=1)
            # the latest month of the data
            date_end = date + relativedelta(months=7)
            # convert datetime object to string
            datestr = '{}-{}'.format(date.strftime(DATE_FORMAT), date_end.strftime(DATE_FORMAT)).lower()
            # create an empty list to store each row of new data
            rows = []

            logging.info('Fetching data for {} between {}'.format(country, datestr))
            # construct the url to fetch data for this country
            url = SOURCE_URL.format(country = country.lower().replace(' ', '-'), date = datestr)

            try:
                with urllib.request.urlopen(url) as f:
                    # use BeautifulSoup to read the content as a nested data structure
                    soup = BeautifulSoup(f, features="lxml")
                    # find the hrefs with the right format
                    hrefs = soup.find_all("a", href=re.compile("https://fdw.fews.net/api/ipcpackage/"))
                    # extract the download link 
                    link = hrefs[0]['href']
                    # construct a filename for the data that will be downloaded 
                    filename = '{}_{}.zip'.format(country, datestr)
                    # contruct the path to the location of downloaded data 
                    tmpfile = os.path.join(DATA_DIR, filename)
                    # download the data 
                    urllib.request.urlretrieve(link, tmpfile)
                    # Parse fetched data and generate unique ids
                    logging.info('Parsing data for {}'.format(country))
                    # check if the tmpfile contains all the expected shapefiles
                    # store the names of the shapefiles for each time period (CS, ML1, ML2) in the dictionary 'shpfiles'
                    shpfiles = findShps(tmpfile)
                     # process each shapefile
                    for ifc_type, shpfile in shpfiles.items():
                        # format path for the shapefiles
                        shpfile = '/{}'.format(shpfile)
                        # format path for the zipfiles
                        zfile = 'zip://{}'.format(tmpfile)
                        # set start and end date as current date
                        start_date = date
                        end_date = date
                        # if the shapefile is related to near-term projections
                        if ifc_type == 'ML1':
                            # set end date as four months from current date 
                            end_date = date + relativedelta(months=4)
                        # if the shapefile is related to medium-term projections
                        elif ifc_type == 'ML2':
                            # set start_date as four months from current date 
                            start_date = date + relativedelta(months=4)
                            # set end_date as eight months from current date 
                            end_date = date + relativedelta(months=8)
                        # open each shapefile as GeoJSON and process them
                        with fiona.open(shpfile, 'r', vfs=zfile) as shp:
                            logging.debug('Schema: {}'.format(shp.schema))
                            # set the index for the feature position
                            pos_in_shp = 0
                            # loop through each features in the GeoJSON
                            for obs in shp:
                                # generate unique id by using date, region, time period and index of the feature
                                uid = genUID(date.strftime('%Y%m'), country.lower().replace(' ', '_'), ifc_type, pos_in_shp)
                                # if the id doesn't already exist in Carto table or 
                                # isn't added to the list for sending to Carto yet 
                                if uid not in existing_ids and uid not in new_ids:
                                    # append the id to the list for sending to Carto 
                                    new_ids.append(uid)
                                    # create an empty list to store data from this row
                                    row = []
                                    # go through each column in the Carto table
                                    for field in CARTO_SCHEMA.keys():
                                        # if we are fetching data for geometry column
                                        if field == 'the_geom':
                                            # get geometry from the 'geometry' feature in GeoJSON,
                                            # simplify complex polygons, and
                                            # add geometry to the list of data from this row
                                            row.append(simplifyGeom(obs['geometry']))
                                        elif field == 'admin0':
                                            row.append(obs['properties']['ADMIN0'])
                                        elif field == 'admin1':
                                            row.append(obs['properties']['ADMIN1'])
                                        # if we are fetching data for unique id column
                                        elif field == UID_FIELD:
                                            # add the unique id to the list of data from this row
                                            row.append(uid)
                                        # if we are fetching data for time period column
                                        elif field == 'ifc_type':
                                            # add the time period to the list of data from this row
                                            row.append(ifc_type)
                                        # if we are fetching data for Food Insecurity Status column
                                        elif field == 'ifc':
                                            # get food insecurity status from ifc_type variable of 
                                            # properties feature and add to list of data from this row
                                            row.append(obs['properties'][ifc_type])
                                        # if we are fetching data for start_date column
                                        elif field == 'start_date':
                                            # convert the datetime for start_date to string 
                                            # add start_date to the list of data from this row
                                            row.append(start_date.strftime(DATETIME_FORMAT))
                                        # if we are fetching data for end_date column
                                        elif field == 'end_date':
                                            # convert the datetime for end_date to string 
                                            # add end_date to the list of data from this row
                                            row.append(end_date.strftime(DATETIME_FORMAT))
                                        else:
                                            rows
                                    # add the list of values from this row to the list of new data
                                    rows.append(row)
                                    # move to the next feature in the geojson
                                    pos_in_shp += 1

                    # Delete local files
                    os.remove(tmpfile)
                # since data has been downloaded, there is no need to go to a previous date 
                break
        
            except Exception as e:
                logging.info('Data for {} during {} not available'.format(country, datestr))
                # skip dates that don't work
                continue

        # find the length (number of rows) of new_data 
        new_count = len(rows)
        # check if new data is available
        if new_count:
            logging.info('Pushing {} new rows: {} for {}'.format(new_count, ifc_type, date))
            # insert new data into the carto table
            cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                                CARTO_SCHEMA.values(), rows, user=CARTO_USER, key=CARTO_KEY)
    # length (number of rows) of new_data 
    num_new = len(new_ids)

    return num_new

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
    # only check times for current state (CS) because dates associated with projections are
    # in the future and don't make sense to list as our most recent update date
    r = cartosql.getFields(TIME_FIELD, table, where="ifc_type LIKE 'CS'", f='csv', post=True)
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
    num_new = processNewData(existing_ids)

    # Remove old observations
    deleteExcessRows(CARTO_TABLE, MAXROWS, TIME_FIELD, MAXAGE)

    # Update Resource Watch
    updateResourceWatch(num_new)

    logging.info('SUCCESS')
