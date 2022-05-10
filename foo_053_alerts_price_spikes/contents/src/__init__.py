import os
import logging
import sys
import requests
from collections import OrderedDict
import datetime
import cartosql
from shapely import wkb
import shapely
import numpy as np
import json
import hashlib
from . import SamplePythonDataBridgesCall as wfpsample
# import dotenv 
import pandas as pd
from dateutil.relativedelta import relativedelta


# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = False

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# World Food Programme API key and secret for fetching the data
WFP_KEY = os.getenv('WFP_KEY')
WFP_SECRET = os.getenv('WFP_SECRET')

# Do we want to process interactions for all ALPS data?
PROCESS_HISTORY_INTERACTIONS = False

# format of date used in Carto table
DATE_FORMAT = '%Y-%m-%dT00:00:00'

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

#name of category in interaction table, sql query from source data
CATEGORIES = OrderedDict([
    ('cereals and tubers', 'cereals and tubers'),
    ('meat, fish and eggs', 'meat, fish and eggs'),
    ('milk and dairy', 'milk and dairy'),
    ('oil and fats', 'oil and fats'),
    ('pulses and nuts','pulses and nuts'),
    ('vegetables and fruits', 'vegetables and fruits'),
    ('miscellaneous food', 'miscellaneous food'),
    ('non-food', 'non-food')])

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
# how many days can be stored in the Carto table before the old data is deleted?
MAXAGE = datetime.datetime.utcnow() - datetime.timedelta(days=365)

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
    INPUT   rid: region id for region in which the market is located (integer)
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

def parseMarkets(mkt, existing_markets):
    '''
    Parse markets data excluding existing observations
    INPUT   mkt: information about each market (JSON feature)
            existing_markets: list of unique market IDs that we already have in our Carto table (list of strings)
    RETURN  new_rows: list of new rows of data found for the input market (list of strings)
    '''
    # create an empty list to store new data (data that's not already in our Carto table)
    row = []

    # get the id of the region from 'admin1Code' variable
    region_id = mkt['admin1Code']
    # get the name of the region from 'admin1Name' variable
    region_name = mkt['admin1Name']
    # get the id of the market from 'marketId' variable
    market_id = mkt['marketId']
    # get the name of the market from 'marketName' variable
    market_name = mkt['marketName']
    # get the latitude and longitude from 'marketLatitude', 'marketLongitude' variable
    # construct geometry of the market using latitude, longitude information
    geom = {
        "type": "Point",
        "coordinates": [
            mkt['marketLongitude'],
            mkt['marketLatitude']
        ]
    }
    # generate unique id for the market using region id, market id and market name
    uid = genMarketUID(region_id, market_id, market_name)
    # if the unique id doesn't already exist in our Carto table
    if uid not in existing_markets:
        # append the id to existing_markets list
        existing_markets.append(uid)
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

    return row

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
    # get the start date from 'commodityPriceDate' variable and convert it to a datetime object formatted according
    # to the variable DATE_FORMAT
    date = datetime.datetime.strptime(market_data['commodityPriceDate'], DATE_FORMAT)
    # get the market price data for the current observation
    mp_price = market_data['analysisValueEstimatedPrice']
    # get the market pewi data for the current observation
    pewi = market_data['analysisValuePewiValue']
    # get the flag (price is forecast or not) for the current observation
    flag = bool(market_data['analysisValuePriceFlag']=='forecast')
    # assign ALPS based on market pewi
    alps_assign = assignALPS(pewi)
    # generate sn variable 
    market_data['sn'] = str(market_data['marketID'])+'_'+str(market_data['commodityID'])+'_'+str(market_data['priceTypeID'])+'_'+str(market_data['commodityUnitID'])
    # generate unique id for the market using 'sn' variable, date and 
    # the availability of forecast data
    uid = genAlpsUID(market_data['sn'], date, flag)
    # create an empty list to store data from this row
    row = []
    # if the unique id doesn't already exist in our Carto table
    if uid not in existing_alps:
        # append the id to existing_alps list
        existing_alps.append(uid)

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
                row.append(None)
            # if we are fetching data for pewi column
            elif field == 'pewi':
                # add the pewi to the list of data from this row
                row.append(pewi)
            # if we are fetching data for alps column
            elif field == 'alps':
                # add the alps data to the list of data from this row
                row.append(alps_assign)
            # if we are fetching data for datetime column
            elif field == 'date':
                # convert datetime to string and format according to DATE_FORMAT
                # add the formatted date to the list of data from this row
                row.append(date.strftime(DATE_FORMAT))
            # if we are fetching data for forecast column
            elif field == 'forecast':
                # add the flag data to the list of data from this row
                row.append(flag)
            elif field == 'sn':
                row.append(market_data['sn'])
            elif field == 'currency':
                row.append(market_data['currencyName'])
            elif field == 'mktid':
                row.append(market_data['marketID'])
            elif field == 'cmid':
                row.append(market_data['commodityID'])
            elif field == 'ptid':
                row.append(market_data['priceTypeID'])
            elif field == 'umid':
                row.append(market_data['commodityUnitID'])
            elif field == 'catid':
                row.append(market_data['categoryId'])
            elif field == 'unit':
                row.append(market_data['commodityUnitName'])
            elif field == 'cmname':
                row.append(market_data['commodityName']+' - '+market_data['priceTypeName'])
            elif field == 'category':
                row.append(market_data['name'])
            elif field == 'mktname':
                row.append(market_data['marketName'])
            elif field == 'admname':
                row.append(market_data['admin1Name'])
            elif field == 'adm1id':
                row.append(market_data['admin1Code'])
    
    return row

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
    RETURN  'True' if the row contains True values, else return 'False' (boolean)
    '''
    return any(row)

def processNewData(existing_markets, existing_alps):
    '''
    Fetch, process and upload new data
    INPUT   existing_markets: list of unique IDs that we already have in our markets Carto table (list of strings)
            existing_alps: list of unique IDs that we already have in our alps Carto table (list of strings)
    RETURN  num_new_markets: number of rows of new data sent to markets Carto table (integer)
            num_new_alps: number of rows of new data sent to alps Carto table (integer)
            markets_updated: list of unique market IDs for markets that were updated (list of strings)
    '''
    # initialize the number of new markets and alps table entries as zero
    num_new_markets = 0
    num_new_alps = 0
    # create an empty list to store the ids of the markets that are updated
    markets_updated = []

    # initialize WFP API
    api = wfpsample.WfpApi(api_key=WFP_KEY, api_secret=WFP_SECRET)

    # pull commodity list from the url as a request response JSON
    com_list = api.get_commodity_list()
    # pull commodity category list from the url as a request response JSON
    com_cat = api.get_commodity_category_list()
    # convert list dictionary to dataframe
    com_list_df = pd.DataFrame(com_list)
    com_cat_df = pd.DataFrame(com_cat)

    # get iso3 code for all countries
    country_codes = []
    for regions in requests.get("https://api.vam.wfp.org/geodata/CountriesInRegion").json():
        country_codes = country_codes + [country['iso3Alpha3'] for country in regions['countryOffices']]
    
    # get and parse each data for each country
    for country_code in country_codes:
        # Fetch new data
        logging.info("Fetching country data for {}".format(country_code))
        # pull markets data from the url as a request response JSON
        markets = api.get_market_list(country_code)
        # pull alerts for price spikes (alps) data from the url as a request response JSON
        alps = api.get_alps(country_code)
        # convert list dictionary to dataframe
        markets_df = pd.DataFrame(markets)
        alps_df = pd.DataFrame(alps)

        # Parse market data excluding existing observations
        # returns a 2D list, 1st dimension represents a particular market
        # 2nd dimention represents the columns of the Carto table for that region and market
        new_markets = [parseMarkets(mkt, existing_markets) for mkt in markets]

        if len(alps)>0:
            # Merge alps dataframe with market dataframe, commodity dataframe, and category dataframe
            alps_cat_df = alps_df.merge(markets_df.loc[:, ['admin1Code', 'admin1Name', 'marketId']], left_on = 'marketID', right_on='marketId', how='inner')
            alps_cat_df = alps_cat_df.merge(com_list_df.loc[:, ['id','categoryId']], left_on='commodityID', right_on='id', how='inner')
            alps_cat_df = alps_cat_df.merge(com_cat_df.loc[:, ['id','name']], left_on='categoryId', right_on='id', how='inner')
            alps_cat_df.drop(['id_x','id_y', 'marketId'], axis=1, inplace=True)
            alps_cat_df.drop_duplicates(inplace=True)
            # replace all NaN with None
            alps_cat_df = alps_cat_df.where((pd.notnull(alps_cat_df)), None)
            alps_cat_df = alps_cat_df.replace({np.nan: None})
            # convert dataframe back to list dictionary
            alps_cat = [dict(x) for i, x in alps_cat_df.iterrows()]

            # Parse alps data excluding existing observations
            # returns a 2D list, 1st dimension represents the time steps that are new
            # 2nd dimention represents the columns of the Carto table for that market and time step
            new_alps = [parseAlps(alp, existing_alps) for alp in alps_cat]
        else:
            new_alps = []

        logging.debug('Country {} Data: After map:'.format(country_code))
        logging.debug(new_markets)
        logging.debug(new_alps)

        # Remove empty List from List
        # using list comprehension
        new_markets = [ele for ele in new_markets if ele != []]
        new_alps = [ele for ele in new_alps if ele != []]

        logging.debug('Country {} Data: After reduce:'.format(country_code))
        logging.debug(new_markets)
        logging.debug(new_alps)

        # Clean any rows that are all None
        new_markets = list(filter(clean_null_rows, new_markets))
        new_alps = list(filter(clean_null_rows, new_alps))

        # Check which market ids were updated so that we can update their interactions
        # get only markets that have been updated
        # get market id index
        if len(new_alps)>0:
            for entry in new_alps:
                # get the unique id for this particular market using its region id, market id and market name
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
        # update the number of rows of new data that will be sent to markets Carto table
        num_new_markets += len(new_markets)
        # update the number of rows of new data that will be sent to alps Carto table
        num_new_alps += len(new_alps)

        # Insert new rows
        # if we have found new market data
        if len(new_markets):
            # insert new data into the markets carto table
            logging.info('Pushing {} new Markets rows'.format(len(new_markets)))
            cartosql.insertRows(CARTO_MARKET_TABLE, CARTO_MARKET_SCHEMA.keys(),
                                CARTO_MARKET_SCHEMA.values(), new_markets,
                                user=CARTO_USER, key =CARTO_KEY, blocksize=100)
        # if we have found new alps data
        if len(new_alps):
            # insert new data into the alps carto table
            logging.info('Pushing {} new ALPS rows'.format(len(new_alps)))
            cartosql.insertRows(CARTO_ALPS_TABLE, CARTO_ALPS_SCHEMA.keys(),
                                CARTO_ALPS_SCHEMA.values(), new_alps,
                                user=CARTO_USER, key =CARTO_KEY, blocksize=100)


    return num_new_markets, num_new_alps, markets_updated

def processInteractions(markets_updated):
    '''
    Process and upload data for interaction table. This additional interaction table was created to 
    display the multiple food commodities in a market as a single row in the Carto table. 
    For each geometry, only one row of data will be used on Resource Watch map interactions, so when 
    there are multiple food commodities for a given geometry, we need to compact them into a single row so that 
    all the information will be shown on an interaction.
    INPUT   markets_updated: list of market IDs for markets that have been updated (list of strings)
    RETURN  num_new_interactions: number of rows of new data sent to market interactions Carto table (integer)
    '''
    # initialize number of rows of new data sent to Carto table as zero
    num_new_interactions = 0
    # if we want to process interactions for all ALPS data
    if PROCESS_HISTORY_INTERACTIONS==True:
        logging.info('Processing interactions for all ALPS data')
        # get ids for all markets
        markets_to_process = getIds(CARTO_MARKET_TABLE, 'uid')

    # otherwise, we will only re-process interactions for markets that have been updated or that have data older than
    # what is allowed (specified by LOOKBACK variable)
    else:
        logging.info('Getting IDs of interactions that should be updated')
        # get a list of all the values from 'region_id', 'market_id', 'market_name' columns where oldest interaction date is more than three months ago
        r = cartosql.getFields(['region_id', 'market_id', 'market_name'], CARTO_INTERACTION_TABLE, where="{} < current_date - interval '{}' month".format(INTERACTION_TIME_FIELD, LOOKBACK),
                               f='csv', user=CARTO_USER, key=CARTO_KEY)
        # turn the response into a list of strings, removing the first and last entries (header and an empty space at end)
        # split each list using ',' to separately retrieve region_id, market_id and market_name
        old_ids = [market.split(',') for market in r.text.split('\r\n')[1:-1]]
        # get the unique id for each market using region id, market id and market name
        old_market_uids = [genMarketUID(old_id[0], old_id[1], old_id[2]) for old_id in old_ids]

        logging.info('Processing interactions for new ALPS data and re-processing interactions that are out of date')
        # get unique ids for new as well as outdated ALPS date
        markets_to_process = np.unique(markets_updated + old_market_uids)
    # initialize number of markets as one
    market_num = 1
    logging.info('{} markets to update interactions for'.format(len(markets_to_process)))
    # go through each market that was updated and update the interaction rows for it
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
                    # if there is more than one commodity, add the interaction_string to the existing one(s), separated by a semicolon
                    else:
                        interaction_string = interaction_string + ';\n' + INTERACTION_STRING_FORMAT.format(num=commodity_num, commodity=entry['cmname'], alps=entry['alps'].lower(), date=entry['date'][:10])
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
            id_field: name of column containing the values/ids we want to pull (string)
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

def get_most_recent_date(table, time_field):
    '''
    Find the most recent date of data in the specified Carto table where forecast is 'False'
    INPUT   table: name of table in Carto we want to find the most recent date for (string)
            time_field:  name of column that will store datetime information (string)
    RETURN  most_recent_date: most recent date of data in the Carto table, found in the TIME_FIELD column of the table (datetime object)
    '''
    # if we are getting date from the interaction table
    if table == 'foo_053c_market_interaction':
        # get dates in INTERACTION_TIME_FIELD column
        r = cartosql.getFields(time_field, table, f='csv', post=True)
    else:
        # get dates in TIME_FIELD column
        r = cartosql.getFields(time_field, table, where="forecast = 'False'", f='csv', post=True)
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

def update_layer(layer, new_date):
    '''
    Update layers in Resource Watch back office.
    INPUT   layer: layer that will be updated (string)
            new_date: latest date to be shown in this layer (datetime)
    '''
    # get current layer titile
    cur_title = layer['attributes']['name']
     
    # get current end date being used from title by string manupulation
    old_date_text = cur_title.split(' Food')[0]
    # get text for new date
    new_date_end = datetime.datetime.strftime(new_date, "%B %d, %Y")
    # get most recent starting date, 3 months ago
    new_date_start = (new_date - relativedelta(months=+3))
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
        
def updateResourceWatch():
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date' and updating any dates on layers
    '''
    # Update dataset's last update date on Resource Watch
    most_recent_date = datetime.datetime.utcnow().date()
    lastUpdateDate(DATASET_ID, most_recent_date)
    
    # Update the dates on layer legends
    logging.info('Updating {}'.format(CARTO_INTERACTION_TABLE))
    # get most recent date from the interaction table
    latest_date_interaction = get_most_recent_date(CARTO_INTERACTION_TABLE, INTERACTION_TIME_FIELD)
    # pull dictionary of current layers from API
    layer_dict = pull_layers_from_API(DATASET_ID)
    # go through each layer, pull the definition and update
    for layer in layer_dict:
        # replace layer title with new dates
        update_layer(layer, latest_date_interaction)
    
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
    deleteExcessRows(CARTO_ALPS_TABLE, MAXROWS, TIME_FIELD, MAXAGE) 

    # Update Resource Watch
    updateResourceWatch()

    logging.info('SUCCESS')
