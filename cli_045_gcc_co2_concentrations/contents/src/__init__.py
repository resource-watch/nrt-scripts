import logging
import sys
import os
from collections import OrderedDict
import cartoframes
import cartosql
import requests
import datetime
import pandas as pd
import wget

# name of data directory in Docker container
DATA_DIR = './data'

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'cli_045_carbon_dioxide_concentration'

# column that stores datetime information
TIME_FIELD = 'date'

# format of dates in Carto table
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA = OrderedDict([
        ('date', 'timestamp'),
        ('average', 'numeric'),
        ('interpolated', 'numeric'),
        ('season_adjusted_trend', 'numeric'),
        ('num_days', 'numeric')
    ])

# url for source data
SOURCE_URL = 'ftp://aftp.cmdl.noaa.gov/products/trends/co2/co2_mm_mlo.txt'

# maximum number of attempts that will be made to download the data
MAX_TRIES = 5

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = 'd287c201-4d7b-4b41-b352-edfcc6f96cb0'

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
FUNCTIONS FOR THIS DATASET

The functions below have been tailored to this specific dataset.
They should all be checked because their format likely will need to be changed.
'''

def decimalToDatetime(dec, date_pattern=DATE_FORMAT):
    ''' 
    Convert a decimal representation of a year to a desired datetime object
    For example: 2016.5 -> 2016-06-01T00:00:00Z
    useful resource: https://stackoverflow.com/questions/20911015/decimal-years-to-datetime-in-python
    INPUT   dec: decimal representation of a year (string)
            date_pattern: format in which we want to convert the input date to (string)
    RETURN  dt: datetime object formatted according to date_pattern (datetime object)
    ''' 
    # convert the date from string to float
    dec = float(dec)
    # convert the date from float to integer to separate out the year (i.e. 2016.5 -> 2016)
    year = int(dec)
    # get the decimal part of the date  (i.e. 2016.5 -> 0.5)
    rem = dec - year
    # create a datetime object for the 1st of January of the year
    base = datetime.datetime(year, 1, 1)
    # generate a complete datetime object to include month, day and time
    dt = base + datetime.timedelta(seconds=(base.replace(year=base.year + 1) - base).total_seconds() * rem)
    
    return(dt)
    

def fetch_data():
    '''
    Download text file from source url and put them into a pandas dataframe
    RETURN  df: dataframe of CO2 concentration data(pandas dataframe)
    '''

    logging.info('Fetching CO2 concentration data')
    # download the data from source url
    loc = wget.download(SOURCE_URL, DATA_DIR) 
    # specify column names for the dataframe
    column_names = ['year','month','date','average','interpolated','season_adjusted_trend','num_days']   
    # create a pandas dataframe using the retrieved text file
    # use whitespace to separate columns; ignore comments, name columns using column_names list
    df = pd.read_csv(loc, delim_whitespace=True, comment='#', header=None, names=column_names)
    # drop redundant columns (year and month)
    df.drop(df.iloc[:, 0:2], inplace = True, axis = 1)
    # convert decimal representation of date to the format specified by the DATE_FORMAT variable
    df['date'] = [decimalToDatetime(x) for x in df['date'].values]

    return df


def processData():
    '''
    Function to download data and upload it to Carto.
    We will first try to get the data for MAX_TRIES then quit
    '''
    # set success to False initially
    success = False
    # initialize tries count as 0
    tries = 0
    # try to get the data from the url for MAX_TRIES 
    while tries < MAX_TRIES and success == False:
        logging.info('Try retrieving CO2 data, try number = {}'.format(tries))
        try:
            # pull CO2 concentration data from source url and format the data into a pandas dataframe
            df = fetch_data()
            # set success as True after retrieving the data to break out of this loop
            success = True
        except Exception as inst:
            logging.info(inst)
            logging.info("Error fetching data, trying again")
            tries = tries + 1
            if tries == MAX_TRIES:
                logging.error("Error fetching data, and max tries reached. See source for last data update.")
    # if we suceessfully collected data from the url
    if success == True:
        # check if the table doesn't already exist in Carto
        if not cartosql.tableExists(CARTO_TABLE, user=CARTO_USER, key=CARTO_KEY):
            logging.info('Table {} does not exist'.format(CARTO_TABLE))
            # if the table does not exist, create it with columns based on the schema input
            cartosql.createTable(CARTO_TABLE, CARTO_SCHEMA)
            # Send dataframe to Carto
            logging.info('Writing to Carto')
            cc = cartoframes.CartoContext(base_url="https://{user}.carto.com/".format(user=CARTO_USER),
                                          api_key=CARTO_KEY)
            cc.write(df, CARTO_TABLE, overwrite=True, privacy='link')
        else:
            # if the table already exists, delete all the rows
            cartosql.deleteRows(CARTO_TABLE, 'cartodb_id IS NOT NULL', user=CARTO_USER, key=CARTO_KEY)
            # Send the processed dataframe to Carto
            logging.info('Writing to Carto')
            cc = cartoframes.CartoContext(base_url="https://{user}.carto.com/".format(user=CARTO_USER),
                                          api_key=CARTO_KEY)
            cc.write(df, CARTO_TABLE, overwrite=True, privacy='link')

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

def updateResourceWatch():
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date' and updating any dates on layers
    '''
    # Update dataset's last update date on Resource Watch
    most_recent_date = get_most_recent_date(CARTO_TABLE)
    lastUpdateDate(DATASET_ID, most_recent_date)

    # Update the dates on layer legends - TO BE ADDED IN FUTURE

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Fetch, process, and upload new data
    processData()

    # Update Resource Watch
    updateResourceWatch()

    logging.info('SUCCESS')
