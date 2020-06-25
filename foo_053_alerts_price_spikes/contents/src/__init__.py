import os
import logging
import sys
import requests
from collections import OrderedDict
import datetime
import cartosql
from functools import reduce
from shapely import wkb
import shapely
import numpy as np
import json
import hashlib

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = False

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# url for ALPS (alerts for price spikes) data
ALPS_URL = 'http://dataviz.vam.wfp.org/api/GetAlps?ac={country_code}'

# url for markets data
MARKETS_URL = 'http://dataviz.vam.wfp.org/api/GetMarkets?ac={country_code}'

# Do we want to process interactions for all ALPS data?
PROCESS_HISTORY_INTERACTIONS=False

# format of date used in Carto table
DATE_FORMAT = '%Y/%m/%d'

# name of tables in Carto where we will upload the alps data
CARTO_ALPS_TABLE = 'foo_053a_alerts_for_price_spikes'

# column names and types for alps data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_ALPS_SCHEMA = OrderedDict([
    ("uid", "text"),
    ("date", "timestamp"),
    ("currency","text"),
    ("mktid","int"),
    ("cmid","int"),
    ("ptid","int"),
    ("umid","int"),
    ("catid","int"),
    ("unit","text"),
    ("cmname","text"),
    ("category","text"),
    ("mktname","text"),
    ("admname","text"),
    ("adm1id","int"),
    ("sn","text"),
    ("forecast","text"),
    ("mp_price","numeric"),
    ("trend","numeric"),
    ("pewi","numeric"),
    ("alps","text")
])

# name of tables in Carto where we will upload the markets data
CARTO_MARKET_TABLE = 'foo_053b_monitored_markets'

# column names and types for markets data table
CARTO_MARKET_SCHEMA = OrderedDict([
    ("uid", "text"),
    ("the_geom", "geometry"),
    ("region_name", "text"),
    ("region_id", "int"),
    ("market_name", "text"),
    ("market_id", "int"),
])

# name of tables in Carto where we will upload the market interaction data
CARTO_INTERACTION_TABLE = 'foo_053c_market_interaction'

# column names and types for interaction data table
CARTO_INTERACTION_SCHEMA = OrderedDict([
    ("uid", "text"),
    ("the_geom", "geometry"),
    ("region_name", "text"),
    ("region_id", "int"),
    ("market_name", "text"),
    ("market_id", "int"),
    ("category", "text"),
    ("market_interaction", "text"),
    ("highest_pewi", "numeric"),
    ("highest_alps", "text"),
    ("oldest_interaction_date", "timestamp"),

])
#(name of category in interaction table, sql query from source data)
# these are different because source data has typos
CATEGORIES = OrderedDict([
    ('cereals and tubers', 'cereals and tubers'),
    ('milk and dairy', 'milk and %'),
    ('oil and fats', 'oil and fats'),
    ('pulses and nuts','pulses and nuts'),
    ('vegetables and fruits', 'vegetables and fruits'),
    ('miscellaneous food', 'miscellaneous food')])

# list of carto tables that we will process
CARTO_TABLES = [CARTO_ALPS_TABLE, CARTO_MARKET_TABLE, CARTO_INTERACTION_TABLE]

# column of table that can be used as a unique ID (UID)
UID_FIELD = 'uid'

# column that stores datetime information
TIME_FIELD = 'date'

# column that stores datetime information for interaction table
INTERACTION_TIME_FIELD = "oldest_interaction_date"

# Format of the text to display in interaction using commodity number, commodity name, alert level and date
INTERACTION_STRING_FORMAT = "[{num}] {commodity} markets were at a '{alps}' level as of {date}"

# specify how many months back we want to display alerts for
LOOKBACK = 3

# how many rows can be stored in the Carto table before the oldest ones are deleted?
MAXROWS = 1000000

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = 'acf42a1b-104b-4f81-acd0-549f805873fb'

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

def genAlpsUID(sn, date, forecast):
    '''
    Generate unique id for a market in the alps table using 'sn' variable, date and 
    the availability of forecast data. Generate an MD5 sum from the formatted string
    INPUT   sn: combination of all ids for the market (integer)
            date: date for the current observation (Datetime object)
            forecast: availability of forecast data (string)
    RETURN  unique id for the market (string)
    '''
    # join sn, date and forecast availablity using underscores
    return '{}_{}_{}'.format(sn, date, forecast)

def genMarketUID(rid, mid, mname):
    '''
    Generate unique id for a market in the markets table using region id, market id and market name
    Generate an MD5 sum from the formatted string
    INPUT   rid: region id for the market (integer)
            mid: id for the market (string)
            mname: name of market (string)
    RETURN  unique id for the market (string)
    '''
    # join region id, market id and market name using underscores
    id_str = '{}_{}_{}'.format(rid, mid, mname)
    return hashlib.md5(id_str.encode('utf8')).hexdigest()

def genInteractionUID(rid, mid, mname, food_category):
    '''
    Generate unique id for a market in interaction table using region id, market id, 
    market name and food category. Generate an MD5 sum from the formatted string
    INPUT   rid: region id for the market (integer)
            mid: id for the market (string)
            mname: name of market (string)
            food_category: category of the food (string)
    RETURN  unique id for the market (string)
    '''
    # join region id, market id, market name and food category using underscores
    id_str = '{}_{}_{}_{}'.format(rid, mid, mname, food_category)
    return hashlib.md5(id_str.encode('utf8')).hexdigest()


def parseMarkets(region_scale, existing_markets):
    '''
    Parse markets data excluding existing observations
    INPUT   region_scale: information about each regional market (JSON feature)
            existing_markets: list of unique market IDs that we already have in our Carto table (list of strings)
    RETURN  new_rows: list of new rows of data found for the input market (list of strings)
    '''
    # Happens w/ 'National Average' entries
    # if 'items' variable doesn't exist in the parsed JSON
    if 'items' not in region_scale:
        logging.debug('Unfamiliar structure, probably National Average entry')
        # return None for every columns in Carto
        return [None]*len(CARTO_MARKET_SCHEMA)
    # create an empty list to store new data (data that's not already in our Carto table)
    new_rows = []
    # get the id of the region from 'id' variable
    region_id = region_scale['id']
    # get the name of the region from 'text' variable
    region_name = region_scale['text']
    # loop each market in the region
    for mkt in region_scale['items']:
        # get the market id from 'id' variable and remove texts from the begining of the id
        market_id = mkt['id'].replace('mk', '')
        # get the name of the market from 'text' variable
        market_name = mkt['text']
        # get the latitude and longitude from 'lat', 'lon' variable
        # construct geometry of the market using lat, lon information
        geom = {
            "type": "Point",
            "coordinates": [
                mkt['lon'],
                mkt['lat']
            ]
        }
        # generate unique id for the market using region id, market id and market name
        uid = genMarketUID(region_id, market_id, market_name)
        # if the unique id doesn't already exist in our Carto table
        if uid not in existing_markets:
            # append the id to existing_markets list
            existing_markets.append(uid)
            # create an empty list to store data from this row
            row = []
            # go through each column in the Carto table
            for field in CARTO_MARKET_SCHEMA.keys():
                # if we are fetching data for unique id column
                if field == 'uid':
                    # add the unique id to the list of data from this row
                    row.append(uid)
                # if we are fetching data for geometry column
                elif field == 'the_geom':
                    # add geometry to the list of data from this row
                    row.append(geom)
                # if we are fetching data for region name column
                elif field == 'region_name':
                    # add region name to the list of data from this row
                    row.append(region_name)
                # if we are fetching data for region id column
                elif field == 'region_id':
                    # add region id to the list of data from this row
                    row.append(region_id)
                # if we are fetching data for market name column
                elif field == 'market_name':
                    # add market name to the list of data from this row
                    row.append(market_name)
                # if we are fetching data for market id column
                elif field == 'market_id':
                    # add market id to the list of data from this row
                    row.append(market_id)
            # add the list of values from this row to the list of new data
            new_rows.append(row)

    return new_rows


def stepForward(start):
    '''
    Move forward by a certain number of days
    INPUT   start: start date for the current observation (datetime object)
    RETURN  start date for next observation (datetime object)
    '''    
    # go forward by 31 days and then replace the day by 15th of the month
    return (start + datetime.timedelta(days=31)).replace(day=15)

def assignALPS(pewi):
    '''
    Based on the ALPS indicator value, assign the markets to one of four situations
    INPUT   pewi: ALPS indicator value for the current observation (integer)
    RETURN  assigned situation for current observation (string)
    ''' 
    # if alps indicator value is less than 0.25, assign condition as 'Normal'
    if pewi < .25:
        return 'Normal'
    # if alps indicator value is less than 1, assign condition as 'Stress'
    elif pewi < 1:
        return 'Stress'
    # if alps indicator value is less than 2, assign condition as 'Alert'
    elif pewi < 2:
        return 'Alert'
    # for all other cases, assign condition as 'Crisis'
    else:
        return 'Crisis'

def parseAlps(market_data, existing_alps):
    '''
    For a particular market, get any dates of data that are new. Each date is a new row.
    INPUT   market_data: information about each regional market (JSON feature)
            existing_alps: list of unique alps IDs that we already have in our Carto table (list of strings)
    RETURN  new_rows: list of new rows of data found for the input market (list of strings)
    '''
    # Happens w/ 'National Average' entries
    # if 'admname' variable doesn't exist in the parsed JSON
    if 'admname' not in market_data:
        logging.debug('Unfamiliar structure, probably National Average entry')
        # return None for every columns in Carto
        return [[None]*len(CARTO_ALPS_SCHEMA)]
    # create an empty list to store new data (data that's not already in our Carto table)
    new_rows = []
    # These are not always the same length, i.e. 23
    # FLAG FOR WFP
    # number of values in 'mp_price', 'trend' and 'pewi' variables are not always same
    # so, we will choose the minimum length to make sure we have data for each variable
    num_obs = min(len(market_data['mp_price']), len(market_data['trend']), len(market_data['pewi']))
    # initialize the availability of forecast data as 'True' 
    run_forecast = True
    try:
        # get the forecast data from 'f_price', 'p_trend' and 'f_pewi' variables
        # choose the minimum length to make sure we have data for each variable
        num_forecast = min(len(market_data['f_price']), len(market_data['p_trend']), len(market_data['f_pewi']))
    except:
        logging.debug('No forecast')
        # set the availability of forecast data as 'False' if we couldn't retrieve forecast data variables
        run_forecast = False
    # get the start date from 'startdate' variable and convert it to a datetime object formatted according
    # to the variable DATE_FORMAT
    date = datetime.datetime.strptime(market_data['startdate'], DATE_FORMAT)
    # go through each observation
    for i in range(num_obs):
        # get the market price data for the current observation
        mp_price = market_data['mp_price'][i]
        # get the market trend data for the current observation
        trend = market_data['trend'][i]
        # get the market pewi data for the current observation
        pewi = market_data['pewi'][i]

        # This data point will be filtered out later
        # if we couldn't retrieve any data for pewi variable
        if not pewi:
            logging.debug('No alert data for this month')
            # return None for every columns in Carto
            new_rows.append([None]*len(CARTO_ALPS_SCHEMA))
            # get start date for next iteration
            date = stepForward(date)
            # since we couldn't retrieve any data, go to the next iteration
            continue
        # Based on the ALPS indicator value, assign the markets to one of four situations
        # If we get here, pewi is not null
        alps = assignALPS(pewi)
        # generate unique id for the market using 'sn' variable, date and 
        # the availability of forecast data
        uid = genAlpsUID(market_data['sn'], date, False)
        # if the unique id doesn't already exist in our Carto table
        if uid not in existing_alps:
            # append the id to existing_alps list
            existing_alps.append(uid)
            # create an empty list to store data from this row
            row = []
            # go through each column in the Carto table
            for field in CARTO_ALPS_SCHEMA.keys():
                # if we are fetching data for unique id column
                if field == 'uid':
                    # add the unique id to the list of data from this row
                    row.append(uid)
                # if we are fetching data for market price column
                elif field == 'mp_price':
                    # add the market price to the list of data from this row
                    row.append(mp_price)
                # if we are fetching data for trend column
                elif field == 'trend':
                    # add the trend to the list of data from this row
                    row.append(trend)
                # if we are fetching data for pewi column
                elif field == 'pewi':
                    # add the pewi to the list of data from this row
                    row.append(pewi)
                # if we are fetching data for alps column
                elif field == 'alps':
                    # add the alps data to the list of data from this row
                    row.append(alps)
                # if we are fetching data for datetime column
                elif field == 'date':
                    # convert datetime to string and format according to DATE_FORMAT
                    # add the formatted date to the list of data from this row
                    row.append(date.strftime(DATE_FORMAT))
                # if we are fetching data for forecast column
                elif field == 'forecast':
                    # add False for this column to the list of data from this row
                    row.append(False)
                else:
                    # for all other columns, we can fetch the data from fields feature 
                    # using our column name in Carto
                    row.append(market_data[field])
            # add the list of values from this row to the list of new data
            new_rows.append(row)
        # get start date for next iteration
        date = stepForward(date)
    # if forecast data is available
    if run_forecast:
        # go through each observation in forecast data
        for i in range(num_forecast):
            # get the forecasted market price data for the current observation
            f_price = market_data['f_price'][i]
            # get the forecasted market trend data for the current observation
            p_trend = market_data['p_trend'][i]
            # get the forecasted market pewi data for the current observation
            f_pewi = market_data['f_pewi'][i]

            # This data point will be filtered out later
            # if we couldn't retrieve any data for f_pewi variable
            if not f_pewi:
                logging.debug('No alert data forecast for this month')
                # return None for every columns in Carto
                new_rows.append([None]*len(CARTO_ALPS_SCHEMA))
                # get start date for next iteration
                date = stepForward(date)
                # since we couldn't retrieve any data, go to the next iteration
                continue
            # Based on the ALPS indicator value, assign the markets to one of four situations
            # If get here, that that pewi is not null
            f_alps = assignALPS(f_pewi)
            # generate unique id for the market using 'sn' variable, date and 
            # the availability of forecast data (set to True in this case 
            # since we are processing forecasted date now)
            uid = genAlpsUID(market_data['sn'], date, True)
            # if the unique id doesn't already exist in our Carto table
            if uid not in existing_alps:
                # append the id to existing_alps list
                existing_alps.append(uid)
                # create an empty list to store data from this row
                row = []
                # go through each column in the Carto table
                for field in CARTO_ALPS_SCHEMA.keys():
                    # if we are fetching data for unique id column
                    if field == 'uid':
                        # add the unique id to the list of data from this row
                        row.append(uid)
                    # if we are fetching data for market price column
                    elif field == 'mp_price':
                        # add the forecasted market price to the list of data from this row
                        row.append(f_price)
                    # if we are fetching data for trend column
                    elif field == 'trend':
                        # add the forecasted trend to the list of data from this row
                        row.append(p_trend)
                    # if we are fetching data for pewi column
                    elif field == 'pewi':
                        # add the forecasted pewi to the list of data from this row
                        row.append(f_pewi)
                    # if we are fetching data for alps column
                    elif field == 'alps':
                        # add the forecasted alps data to the list of data from this row
                        row.append(f_alps)
                    # if we are fetching data for datetime column
                    elif field == 'date':
                        # convert datetime to string and format according to DATE_FORMAT
                        # add the formatted date to the list of data from this row
                        row.append(date.strftime(DATE_FORMAT))
                    # if we are fetching data for forecast column
                    elif field == 'forecast':
                        # add True for this column to the list of data from this row
                        row.append(True)
                    else:
                        # for all other columns, we can fetch the data from fields feature 
                        # using our column name in Carto
                        row.append(market_data[field])
                # add the list of values from this row to the list of new data
                new_rows.append(row)
            # get start date for next iteration
            date = stepForward(date)

    return new_rows

def flatten(lst, items):
    '''
    Add elements from the list 'items' to the end of the list 'lst'
    INPUT   lst: list to which we will append values (list of strings)
            items: list that will be appended to the other list (list of strings)
    RETURN  lst: combined list of the two input list (list of strings)
    '''
    lst.extend(items)
    return lst

def clean_null_rows(row):
    '''
    Clean any rows that are all None
    INPUT   row: input row to check for values (list of strings)
    RETURN  'True' if the row contain True values, else return 'False'
    '''
    return any(row)

def processNewData(existing_markets, existing_alps):
    '''
    Fetch, process and upload new data
    INPUT   existing_markets: list of unique IDs that we already have in our markets Carto table (list of strings)
            existing_alps: list of unique IDs that we already have in our alps Carto table (list of strings)
    RETURN  num_new_markets: number of rows of new data sent to markets Carto table (integer)
            num_new_alps: number of rows of new data sent to alps Carto table (integer)
            markets_updated: list of new unique market IDs (list of strings)
    '''
    num_new_markets = 0
    num_new_alps = 0
    markets_updated = []

    #get list of country codes
    #countries = pd.read_csv('http://vam.wfp.org/sites/data/api/adm0code.csv')
    #country_codes=countries['ADM0_CODE'].tolist()
    #csv stopped loading, so I have manually added codes here:
    country_codes = [1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 
    46, 47, 48, 49, 50, 51, 53, 52, 54, 55, 56, 57, 58, 59, 60, 61, 66, 62, 63, 64, 65, 67, 68, 69, 40763, 70, 71, 72, 73, 40765, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89,
     90, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 103, 104, 106, 105, 107, 108, 40760, 109, 110, 111, 33364, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 40781, 126, 127, 
     128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 40762, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 
     163, 164, 165, 166, 167, 2647, 168, 169, 170, 171, 172, 173, 174, 175, 177, 176, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 
     198, 199, 200, 201, 202, 206, 203, 204, 205, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 2648, 220, 221, 222, 223, 224, 225, 226, 227, 228, 70001, 229, 230, 231, 
     999, 40764, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 256, 253, 254, 255, 257, 259, 258, 260, 261, 262, 263, 264, 265, 266, 
     268, 269, 270, 271]
    # get and parse each page; stop when no new results or 200 pages
    for country_code in country_codes:
        # 1. Fetch new data
        logging.info("Fetching country code {}".format(country_code))
        # initialize number of tries to fetch data as zero
        try_num=0
        try:
            # pull markets data from the url as a request response JSON
            markets = requests.get(MARKETS_URL.format(country_code=country_code)).json()
            # pull alerts for price spikes (alps) data from the url as a request response JSON
            alps = requests.get(ALPS_URL.format(country_code=country_code)).json()
        except Exception as e:
            # stop trying if we can't get data within two tries
            if try_num < 2:
                # increase the count of number of tries
                try_num+=1
            else:
                logging.error(e)

        # Parse regional market data excluding existing observations
        new_markets = [parseMarkets(mkt, existing_markets) for mkt in markets]

        # Parse alps data excluding existing observations
        # returns a 3D list, 1st dimension represents a particular market
        # 2nd dimension represents the time steps that are new
        # 3rd dimention represents the columns of the Carto table for that market and time step
        new_alps = [parseAlps(alp, existing_alps) for alp in alps]

        logging.debug('Country {} Data: After map:'.format(country_code))
        logging.debug(new_markets)
        logging.debug(new_alps)

        # removes market dimension of array
        # now each element of the array is just a row to be inserted into Carto table
        new_markets = reduce(flatten, new_markets , [])
        new_alps = reduce(flatten, new_alps, [])

        logging.debug('Country {} Data: After reduce:'.format(country_code))
        logging.debug(new_markets)
        logging.debug(new_alps)

        # Ensure new_<rows> is a list of lists, even if only one element
        if len(new_markets):
            # if the first element of new_markets is not a list
            if type(new_markets[0]) != list:
                # convert it to a list
                new_markets = [new_markets]
        if len(new_alps):
            # if the first element of new_alps is not a list
            if type(new_alps[0]) != list:
                # convert it to a list
                new_alps = [new_alps]

        # Clean any rows that are all None
        new_markets = list(filter(clean_null_rows, new_markets))
        new_alps = list(filter(clean_null_rows, new_alps))

        # Check which market ids were updated so that we can update their interactions
        # get only markets that have been updated
        # get market id index
        if len(new_alps)>0:
            for entry in new_alps:
                # Generate unique id for a market using region id, market id and market name
                uid=genMarketUID(entry[list(CARTO_ALPS_SCHEMA.keys()).index("adm1id")],
                                 entry[list(CARTO_ALPS_SCHEMA.keys()).index("mktid")],
                                 entry[list(CARTO_ALPS_SCHEMA.keys()).index("mktname")])
                # save the ids for the markets that have been updated
                markets_updated.append(uid)
            # filter and get only the unique ids and convert them to a list
            markets_updated = np.unique(markets_updated).tolist()
        logging.debug('Country {} Data: After filter:'.format(country_code))
        logging.debug(new_markets)
        logging.debug(new_alps)
        # number of rows of new data that will be sent to markets Carto table
        num_new_markets += len(new_markets)
        # number of rows of new data that will be sent to alps Carto table
        num_new_alps += len(new_alps)

        # Insert new rows
        # if we have found new data to process
        if len(new_markets):
            # insert new data into the markets carto table
            logging.info('Pushing {} new Markets rows'.format(len(new_markets)))
            cartosql.insertRows(CARTO_MARKET_TABLE, CARTO_MARKET_SCHEMA.keys(),
                                CARTO_MARKET_SCHEMA.values(), new_markets,
                                user=CARTO_USER, key =CARTO_KEY)
        # if we have found new data to process
        if len(new_alps):
            # insert new data into the alps carto table
            logging.info('Pushing {} new ALPS rows'.format(len(new_alps)))
            cartosql.insertRows(CARTO_ALPS_TABLE, CARTO_ALPS_SCHEMA.keys(),
                                CARTO_ALPS_SCHEMA.values(), new_alps,
                                user=CARTO_USER, key =CARTO_KEY)


    return num_new_markets, num_new_alps, markets_updated

def processInteractions(markets_updated):
    '''
    Process and upload data for interaction table. This additional interaction table was created to 
    display the multiple food commodities in a market as a single row in the Carto table. 
    For each geometry, only one row of data will be used on Resource Watch map interactions, so when 
    there are multiple food commodities for a given geometry, we need to compact them into a single row so that 
    all the information will be shown on an interaction.
    INPUT   markets_updated: list of new market IDs (list of strings)
    RETURN  num_new_interactions: number of rows of new data sent to market interactions Carto table (integer)
    '''
    # initialize number of rows of new data sent to Carto table as zero
    num_new_interactions = 0
    # if we want to process interactions for all ALPS data
    if PROCESS_HISTORY_INTERACTIONS==True:
        logging.info('Processing interactions for all ALPS data')
        # get ids for all markets
        markets_to_process = getIds(CARTO_MARKET_TABLE, 'uid')

    else:
        logging.info('Getting IDs of interactions that should be updated')
        # get a list of all the values from 'region_id', 'market_id', 'market_name' columns where oldest interaction date is than three months ago
        r = cartosql.getFields(['region_id', 'market_id', 'market_name'], CARTO_INTERACTION_TABLE, where="{} < current_date - interval '{}' month".format(INTERACTION_TIME_FIELD, LOOKBACK),
                               f='csv', user=CARTO_USER, key=CARTO_KEY)
        # turn the response into a list of strings, removing the first and last entries (header and an empty space at end)
        # split each list using ',' to separately retrieve region_id, market_id and market_name
        old_ids = [market.split(',') for market in r.text.split('\r\n')[1:-1]]
        # Generate unique id for each market using region id, market id and market name
        old_market_uids = [genMarketUID(old_id[0], old_id[1], old_id[2]) for old_id in old_ids]

        logging.info('Processing interactions for new ALPS data and re-processing interactions that are out of date')
        # get unique ids for new as well as outdated ALPS date
        markets_to_process = np.unique(markets_updated + old_market_uids)
    # initialize number of markets as one
    market_num = 1
    logging.info('{} markets to update interactions for'.format(len(markets_to_process)))
    # go through each market that was updated and create the correct rows for them
    for m_uid in markets_to_process:
        logging.info('processing {} out of {} markets'.format(market_num, len(markets_to_process)))
        # create an empty list to store new data for this table
        new_rows = []
        # loop through each category in interaction table
        for food_category, sql_query in CATEGORIES.items():
            # initialize number of tries to fetch data as zero
            try_num=1
            # stop trying if we can't get data within three tries
            while try_num <=3:
                try:
                    # get information about market
                    r = cartosql.get("SELECT * FROM {} WHERE uid='{}'".format(CARTO_MARKET_TABLE, m_uid),
                                     user=CARTO_USER, key=CARTO_KEY)
                    # break out of the loop if we couldn't find any rows 
                    if r.json()['total_rows']==0:
                        #logging.info('No rows for interaction')
                        alps_entries=[]
                        break
                    # turn the reponse into a JSON
                    market_entry = r.json()['rows'][0]

                    # get information about food prices at market
                    # SQL gets most recent entry for each commodity at each market that is NOT a forecast
                    request = "SELECT DISTINCT ON (mktid, cmname) * FROM {table} WHERE mktid={market_id} AND mktname='{market_name}' AND adm1id={region_id} AND category LIKE '{cat_name}' AND date > current_date - interval '{x}' month AND forecast = 'False' ORDER  BY mktid, cmname, date desc".format(
                        table=CARTO_ALPS_TABLE, market_id=market_entry['market_id'],
                        market_name=market_entry['market_name'].replace("'", "''"), region_id=market_entry['region_id'],
                        cat_name=sql_query, x=LOOKBACK)
                    # get all the rows from Carto table which satisfy the above SQL query
                    r = cartosql.get(request, user=CARTO_USER, key=CARTO_KEY)
                    # turn the reponse into a JSON
                    alps_entries = r.json()['rows']
                    break
                except:
                    # increase the count of number of tries
                    try_num += 1
            # Generate unique id for a market using region id, market id, market name and food category
            uid = genInteractionUID(market_entry['region_id'], market_entry['market_id'], market_entry['market_name'],
                                    food_category)
            # if we found information for food prices at market
            if alps_entries:
                # initialize the number of commodities to 1
                commodity_num=1
                # loop through each commodities in a market
                for entry in alps_entries:
                    # if we are processing the first food commodity in a market
                    if commodity_num==1:
                        # generate the text to display in interaction using commodity number, commodity name, alert level and date
                        interaction_string = INTERACTION_STRING_FORMAT.format(num=commodity_num, commodity=entry['cmname'], alps=entry['alps'].lower(), date=entry['date'][:10])
                    # if there are more than one commodities, add the interaction_string to the existing one(s), separated by a semicolon
                    else:
                        interaction_string = interaction_string + '; ' + INTERACTION_STRING_FORMAT.format(num=commodity_num, commodity=entry['cmname'], alps=entry['alps'].lower(), date=entry['date'][:10])
                    # increase commodity_num by 1 for next iteration
                    commodity_num+=1
                # create an empty list to store data from this row
                row = []
                # go through each column in the Carto table
                for field in CARTO_INTERACTION_SCHEMA.keys():
                    # if we are fetching data for unique id column
                    if field == 'uid':
                        # add the unique id to the list of data from this row
                        row.append(uid)
                    # if we are fetching data for market id column
                    elif field == 'market_id':
                        # get market_id from market_entry and convert it to a integer
                        # add market id to the list of data from this row
                        row.append(int(market_entry['market_id']))
                    # if we are fetching data for geometry column
                    elif field == 'the_geom':
                        # Return a geometric object from a Well Known Binary (WKB) representation
                        shapely_point = wkb.loads(market_entry['the_geom'], hex=True)
                        # load the geometric object as JSON
                        json_point = json.loads(json.dumps(shapely.geometry.mapping(shapely_point)))
                        # add the processed geometry to the list of data from this row
                        row.append(json_point)
                    # if we are fetching data for region name column
                    elif field == 'region_name':
                        # add region name to the list of data from this row
                        row.append(market_entry['region_name'])
                    # if we are fetching data for region id column
                    elif field == 'region_id':
                        # add region id to the list of data from this row
                        row.append(market_entry['region_id'])
                    # if we are fetching data for market name column
                    elif field == 'market_name':
                        # add market name to the list of data from this row
                        row.append(market_entry['market_name'])
                    # if we are fetching data for market interaction column
                    elif field == 'market_interaction':
                        # if there are no values, append None
                        if len(alps_entries) == 0:
                            row.append(None)
                        else:
                            # add interaction_string to the list of data from this row
                            row.append(interaction_string)
                    # if we are fetching data for category column
                    elif field == 'category':
                        # add food category to the list of data from this row
                        row.append(food_category)
                    # if we are fetching data for highest_pewi column
                    elif field == 'highest_pewi':
                        # if there are no values, append None
                        if len(alps_entries) == 0:
                            row.append(None)
                        else:
                            # get the maximum pewi for each observation
                            highest_pewi = max([entry['pewi'] for entry in alps_entries])
                            # add highest_pewi to the list of data from this row
                            row.append(highest_pewi)
                    # if we are fetching data for highest_alps column
                    elif field == 'highest_alps':
                        # if there are no values, append None
                        if len(alps_entries) == 0:
                            row.append(None)
                        else:
                            # get the alert level for highest_pewi using alps indicator value, 
                            highest_alps_category = assignALPS(highest_pewi)
                            # add highest_alps_category to the list of data from this row
                            row.append(highest_alps_category)
                    # if we are fetching data for oldest_interaction_date column
                    elif field == INTERACTION_TIME_FIELD:
                        # if there are no values, append None
                        if len(alps_entries) == 0:
                            row.append(None)
                        else:
                            # get the oldest interaction date for each observation
                            row.append(min(entry['date'] for entry in alps_entries))
                # add the list of values from this row to the list of new_rows
                new_rows.append(row)
                # increase the count of rows of new data sent to Carto table by one
                num_new_interactions+=1
            # delete old entries for the markets that were updated
            try:
                cartosql.deleteRows(CARTO_INTERACTION_TABLE, "{} = '{}'".format(UID_FIELD, uid), user=CARTO_USER,
                                    key=CARTO_KEY)
            except:
                pass
        # send new rows for these markets
        if new_rows:
            cartosql.insertRows(CARTO_INTERACTION_TABLE, CARTO_INTERACTION_SCHEMA.keys(),
                                CARTO_INTERACTION_SCHEMA.values(), new_rows, blocksize=500, user=CARTO_USER, key=CARTO_KEY)
        # increase count of market_num by 1 after we finish processing a market data
        market_num+=1
    return num_new_interactions


def getIds(table, id_field):
    '''
    Get ids from table
    INPUT   table: Carto table to check (string)
            id_field: name of column that we want to use as a unique ID for this table; this will be used to compare the
                    source data to the our table each time we run the script so that we only have to pull data we
                    haven't previously uploaded (string)
    RETURN  list of existing IDs in the table, pulled from the id_field column (list of strings)
    '''
    r = cartosql.getFields(id_field, table, f='csv', user=CARTO_USER, key =CARTO_KEY)
    return r.text.split('\r\n')[1:-1]


def deleteExcessRows(table, max_rows, time_field, max_age=''):
    ''' 
    Delete rows that are older than a certain threshold and also bring count down to max_rows
    INPUT   table: name of table in Carto from which we will delete excess rows (string)
            max_rows: maximum rows that can be stored in the Carto table (integer)
            time_field: column that stores datetime information (string) 
            max_age: oldest date that can be stored in the Carto table (datetime object)
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
    Find the most recent date of data in the specified Carto table where forecast is 'False'
    INPUT   table: name of table in Carto we want to find the most recent date for (string)
    RETURN  most_recent_date: most recent date of data in the Carto table, found in the TIME_FIELD column of the table (datetime object)
    '''
    # get dates in TIME_FIELD column
    r = cartosql.getFields(TIME_FIELD, table, where="forecast = 'False'", f='csv', post=True)
    # turn the response into a list of dates
    dates = r.text.split('\r\n')[1:-1]
    # sort the dates from oldest to newest
    dates.sort()
    # turn the last (newest) date into a datetime object
    most_recent_date = datetime.datetime.strptime(dates[-1], '%Y-%m-%d %H:%M:%S')
    return most_recent_date

def updateResourceWatch():
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date' and updating any dates on layers
    '''
    # Update dataset's last update date on Resource Watch
    most_recent_date = get_most_recent_date(CARTO_ALPS_TABLE)
    lastUpdateDate(DATASET_ID, most_recent_date)

    # Update the dates on layer legends - TO BE ADDED IN FUTURE

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # clear the tables before starting, if specified
    if CLEAR_TABLE_FIRST:
        # loop through each tables that we have for this dataset
        for table in CARTO_TABLES:
            # if the table exists
            if cartosql.tableExists(table, user=CARTO_USER, key=CARTO_KEY):
                # delete all the rows 
                cartosql.deleteRows(table, 'cartodb_id IS NOT NULL', user=CARTO_USER, key=CARTO_KEY)
                # note: we do not delete the entire table because this will cause the dataset visualization on Resource Watch
                # to disappear until we log into Carto and open the table again. If we simply delete all the rows, this
                # problem does not occur

    logging.info('Checking if {} table exists and getting existing IDs.'.format(CARTO_MARKET_TABLE))
    # Check if table exists, create it if it does not
    existing_markets = checkCreateTable(CARTO_MARKET_TABLE, CARTO_MARKET_SCHEMA, UID_FIELD)

    logging.info('Checking if {} table exists and getting existing IDs.'.format(CARTO_ALPS_TABLE))
    # Check if table exists, create it if it does not
    existing_alps = checkCreateTable(CARTO_ALPS_TABLE, CARTO_ALPS_SCHEMA, UID_FIELD, TIME_FIELD)

    logging.info('Checking if {} table exists and getting existing IDs.'.format(CARTO_INTERACTION_TABLE))
    # Check if table exists, create it if it does not
    existing_interactions = checkCreateTable(CARTO_INTERACTION_TABLE, CARTO_INTERACTION_SCHEMA, UID_FIELD, INTERACTION_TIME_FIELD)

    # Iterively fetch, parse and post new data
    num_new_markets, num_new_alps, markets_updated = processNewData(existing_markets, existing_alps)

    # Update Interaction table
    num_new_interactions = processInteractions(markets_updated)

    # Report new data count
    num_existing_markets = num_new_markets + len(existing_markets)
    logging.info('Total market rows: {}, New: {}, Max: {}'.format(
        num_existing_markets, num_new_markets, MAXROWS))

    num_existing_alps = num_new_alps + len(existing_alps)
    logging.info('Total alps rows: {}, New: {}, Max: {}'.format(
        num_existing_alps, num_new_alps, MAXROWS))

    num_existing_interactions = num_new_interactions + len(existing_interactions)
    logging.info('Total interaction rows: {}, New: {}, Max: {}'.format(
        num_existing_interactions, num_new_interactions, MAXROWS))

    # Remove old observations
    deleteExcessRows(CARTO_ALPS_TABLE, MAXROWS, TIME_FIELD) 

    # Update Resource Watch
    updateResourceWatch()

    logging.info('SUCCESS')
