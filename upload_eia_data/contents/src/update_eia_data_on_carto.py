import logging
import sys
import pandas as pd
import datetime
import requests
import os
from collections import OrderedDict
import shutil
import cartosql
from carto.datasets import DatasetManager
from carto.auth import APIKeyAuthClient
import boto3
from botocore.exceptions import NoCredentialsError
import zipfile
from zipfile import ZipFile


logging.basicConfig(stream = sys.stderr, level = logging.INFO)

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_WRI_RW_USER')
CARTO_KEY = os.getenv('CARTO_WRI_RW_KEY')

# EIA key for fetching data
EIA_KEY = os.getenv('EIA_KEY')

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA = OrderedDict([
    ('country', 'text'),
    ('geography', 'text'),
    ('year', 'numeric'),
    ('unit','text'),
    ('yr_data','numeric'),
    ('datetime', 'timestamp')])

# name of data directory in Docker container
DATA_DIR = 'data'

# pull in sheet with information about each EIA dataset and where it is stored on Carto and the RW API
eia_rw_table = pd.read_csv(
    'https://raw.githubusercontent.com/resource-watch/nrt-scripts/master/upload_eia_data/EIA_RW_dataset_names_ids.csv').set_index(
    'Carto Table')

# get list of all current Carto table names
carto_table_names = cartosql.getTables(user = CARTO_USER, key = CARTO_KEY)

def upload_to_aws(local_file, bucket, s3_file):
    '''
    This function uploads a local file to a specified location on S3 data storage
    INPUT   local_file: location of local file to be transferred to s3 (string)
            bucket: S3 bucket where the data will be uploaded (string)
            s3_file: file name under which the data should be uploaded (string)
    RETURN  whether or not file has successfully been uploaded to AWS (boolean)
    '''
    # set up S3 credentials
    s3 = boto3.client('s3', aws_access_key_id=os.getenv('S3_ACCESS_KEY'),
                      aws_secret_access_key=os.getenv('S3_SECRET_KEY'))
    # try to upload the local file to S3 storage
    try:
        s3.upload_file(local_file, bucket, s3_file)
        logging.info("Upload Successful")
        logging.info("http://{}.s3.amazonaws.com/{}".format(bucket, s3_file))
        return True
    except FileNotFoundError:
        logging.error("The file was not found")
        return False
    except NoCredentialsError:
        logging.error("Credentials not available")
        return False

def fetch_eia_data(table_name):
    '''
    This function fetches and processes data from the EIA API for a specified Carto table
    INPUT   table_name: name of Carto table we want to fetch and process data for (string)
    RETURN  all_eia_data: dataframe of processed data for this table (pandas dataframe)
    '''
    # pull the eia catogory id that is included in this table
    category_id = eia_rw_table.loc[table_name, 'eia_category_id']
    eia_unit = eia_rw_table.loc[table_name, 'eia_unit']

    # insert the url used to find the series ids of the data 
    url = 'http://api.eia.gov/category/?api_key={}&category_id={}'.format(EIA_KEY, category_id)

    # fetch the information of all series of data in this category from the API 
    r = requests.get(url)
    series = r.json()['category']['childseries']

    # store the names, ids and units of the child series in a list of dictionaries
    ids = [{'country': ', '.join(child['name'].split(', ')[1:-1]), 'series_id': child['series_id'], 'units': child['units']} for child in series]
    # subset the dictionary list by EIA unit
    ids = [id for id in ids if id['units'] == eia_unit]

    # create an empty dataframe to store data 
    df = pd.DataFrame()
    # loop through each series id
    for id in ids:
        # construct the API call to fetch data from the series
        data_url = 'http://api.eia.gov/series/?api_key={}&series_id={}'.format(EIA_KEY, id['series_id'])
        logging.info('Fetching data for {}'.format(id['country']))
        # extract the data from the response 
        data = requests.get(data_url).json()['series'][0]
        # create a dataframe with the column 'year' and 'yr_data' from the data 
        df_country = pd.DataFrame({'year':[x for [x,y] in data['data']], 'yr_data': [y for [x,y] in data['data']]})
        # create a new column 'country' to store the country information 
        df_country['country'] = id['country']
        # create a new column 'geography' to store the code of the country/region
        df_country['geography'] = data['geography']
        # create a new column 'unit' to store the units
        df_country['unit'] = data['units']
        # concat the data frame to the larger dataframe created before the loop
        df = pd.concat([df, df_country], ignore_index=True)

    # save the raw data as a csv file 
    raw_data_file = os.path.join(DATA_DIR, f'{table_name[:-5]}_data.csv')
    df.to_csv(raw_data_file, index = False)

    return df, raw_data_file

def process_eia_data(df, table_name):
    '''
    This function processes EIA data for a specified Carto table
    INPUT   df: dataframe to be processed (pandas dataframe)
            table_name: name of Carto table we want to process data for (string)
    RETURN  df: dataframe of processed data for this table (pandas dataframe)
            processed_data_file: processed data file name (string)
    '''
    # reorder the dataframe 
    df = df[['country', 'geography', 'year', 'unit', 'yr_data']]

    # remove duplicated rows 
    df.drop_duplicates(inplace=True)

    # subset the dataframe to remove the data of larger regions that consist of multiple geographies 
    df = df[df.geography.apply(lambda x: ('+' not in x) & (x != 'WLD'))]
    # remove OPEC - South America since it's a duplicate of Venezuela
    df = df[df.country != 'OPEC - South America']

    # convert the data type of 'year' column to int 
    df['year'] = df['year'].astype(int)

    # create a column to store the year information as datetime objects 
    df['datetime'] = [datetime.datetime(x, 1, 1) for x in df['year']]

    # remove rows with no data '--'
    df = df.loc[df.yr_data != '--']
    # remove rows with no data 'NA'
    nan_value = float("NaN")
    df.replace("NA", nan_value, inplace = True)
    df.dropna(subset = ['yr_data'], inplace = True)

    # convert the data type of the column 'yr_data' to float
    df.yr_data = df.yr_data.astype(float)

    # save processed dataset to csv
    processed_data_file = os.path.join(DATA_DIR, table_name+'.csv')
    df.to_csv(processed_data_file, index=False)

    return df, processed_data_file

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
                os.remove(DATA_DIR + '/' + f)
            # if it is not a file, remove it as a folder
            except:
                shutil.rmtree(f, ignore_errors = True)
    except NameError:
        logging.info('No local files to clean.')

def main():
    logging.info('STARTING EIA CARTO UPDATE')

    # create a new sub-directory within your specified dir called 'data'
    if not os.path.exists(DATA_DIR):
        os.mkdir(DATA_DIR)

    # process each Carto table for EIA datasets one at a time
    for table_name, info in eia_rw_table.iterrows():
        # get the dataset name (table name without the '_edit' at the end of the table_name
        dataset_name = table_name[:-5]
        logging.info('Next table to update: {}'.format(dataset_name))

        '''
        Download data and save to your data directory
        '''
        df, raw_data_file = fetch_eia_data(table_name)

        '''
        Process data
        '''
        df, processed_data_file = process_eia_data(df, table_name)

        '''
        Upload processed data to Carto
        '''
        logging.info('Uploading processed data to Carto.')
        # check if table exists
        # if table does not exist, create it
        if not table_name in carto_table_names:
            logging.info(f'Table {table_name} does not exist, creating')
            # Change privacy of table on Carto
            # set up carto authentication using local variables for username (CARTO_USER) and API key (CARTO_KEY)
            auth_client = APIKeyAuthClient(api_key = CARTO_KEY, base_url = f"https://{CARTO_USER}.carto.com/")
            # set up dataset manager with authentication
            dataset_manager = DatasetManager(auth_client)
            # upload dataset to Carto
            dataset = dataset_manager.create(processed_data_file)
            # set dataset privacy to 'Public with link'
            dataset = dataset_manager.get(table_name)
            dataset.privacy = 'LINK'
            dataset.save()
            logging.info('Privacy set to public with link.')

        # if table does exist, clear all the rows so we can upload the latest version
        else:
            logging.info(f'Table {table_name} already exists, clearing rows')

            # delete all the rows
            cartosql.deleteRows(table_name, 'cartodb_id IS NOT NULL', user=CARTO_USER, key=CARTO_KEY)
            # note: we do not delete the entire table because this will cause the dataset visualization on Resource Watch
            # to disappear until we log into Carto and open the table again. If we simply delete all the rows, this
            # problem does not occur

            # insert all data rows for this table
            if len(df):
                cartosql.blockInsertRows(table_name, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), df.values.tolist(), user = CARTO_USER, key = CARTO_KEY)
                logging.info('Success! New rows have been added to Carto.')
            else:
                logging.info('No rows to add to Carto.')

        '''
        Upload original data and processed data to Amazon S3 storage
        '''
        logging.info('Uploading original data to S3.')
        # Copy the raw data into a zipped file to upload to S3
        raw_data_dir = os.path.join(DATA_DIR, dataset_name + '.zip')
        with ZipFile(raw_data_dir, 'w') as zip:
            zip.write(raw_data_file, os.path.basename(raw_data_file), compress_type = zipfile.ZIP_DEFLATED)

        # Upload raw data file to S3
        upload_to_aws(raw_data_dir, 'wri-public-data', 'resourcewatch/' + os.path.basename(raw_data_dir))

        logging.info('Uploading processed data to S3.')
        # Copy the processed data into a zipped file to upload to S3
        processed_data_dir = os.path.join(DATA_DIR, dataset_name + '_edit.zip')
        with ZipFile(processed_data_dir, 'w') as zip:
            zip.write(processed_data_file, os.path.basename(processed_data_file), compress_type = zipfile.ZIP_DEFLATED)

        # Upload processed data file to S3
        upload_to_aws(processed_data_dir, 'wri-public-data', 'resourcewatch/' + os.path.basename(processed_data_dir))

    # Delete local files in Docker container
    delete_local()

    logging.info('SUCCESS')