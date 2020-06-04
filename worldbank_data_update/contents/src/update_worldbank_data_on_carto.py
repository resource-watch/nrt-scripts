import logging
import sys
import pandas as pd
import numpy as np
import datetime
import requests
import os
from collections import OrderedDict
import urllib.request
import cartosql
from carto.datasets import DatasetManager
from carto.auth import APIKeyAuthClient
import boto3
from botocore.exceptions import NoCredentialsError
from zipfile import ZipFile

logging.basicConfig(stream=sys.stderr, level=logging.INFO)

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_WRI_RW_USER')
CARTO_KEY = os.getenv('CARTO_WRI_RW_KEY')

# pull in sheet with information about each World Bank dataset and where it is stored on Carto and the RW API
wb_rw_table = pd.read_csv(
    'https://raw.githubusercontent.com/resource-watch/nrt-scripts/master/upload_worldbank_data/WB_RW_dataset_names_ids.csv').set_index(
    'Carto Table')

# pull in sheet with World Bank name to iso3 conversions
wb_name_to_iso3_conversion = pd.read_csv(
    'https://raw.githubusercontent.com/resource-watch/nrt-scripts/master/upload_worldbank_data/WB_name_to_ISO3.csv').set_index(
    'WB_name')

# get list of all current Carto table names
carto_table_names = cartosql.getTables(user=CARTO_USER, key=CARTO_KEY)

def upload_to_aws(local_file, bucket, s3_file):
    '''
    This function uploads a local file to a specified location on S3 data storage
    INPUT   local_file: location of local file to be transferred to s3 (string)
            bucket: S3 bucket where the data will be uploaded (string)
            s3_file: file name under which the data should be uploaded (string)
    RETURN  whether or not file has successfully been uploaded to AWS (boolean)
    '''
    # set up S3 credentials
    s3 = boto3.client('s3', aws_access_key_id=os.getenv('aws_access_key_id'),
                      aws_secret_access_key=os.getenv('aws_secret_access_key'))
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

def add_iso(name):
    '''
    This function takes a World Bank country name and matches it to the appropriate ISO3 code
    INPUT   name: World Bank name of country (string)
    RETURN  ISO3 code for input country (string)
    '''
    # try to match the country name to the ISO3 code based on loaded conversion table
    try:
        return wb_name_to_iso3_conversion.loc[name, "ISO"]
    # if no match can be found, return None
    except:
        return np.nan

# pull in table of standard Resource Watch country names and ISO codes
sql_statement = 'SELECT iso_a3, name FROM wri_countries_a'
country_html = requests.get(f'https://{CARTO_USER}.carto.com/api/v2/sql?q={sql_statement}')
country_info = pd.DataFrame(country_html.json()['rows'])

def add_rw_name(code):
    '''
    This function takes an ISO3 code and matches it to the standard Resource Watch name for the country
    INPUT   code: ISO3 code for input country (string)
    RETURN  standard Resource Watch name for input country (string)
    '''
    # try to find the RW country name for the ISO3 code based on loaded conversion table
    temp = country_info.loc[country_info['iso_a3'] == code]
    temp = temp['name'].tolist()
    if temp != []:
        return temp[0]
    else:
        return None

def add_rw_code(code):
    '''
    This function takes an ISO3 code and matches it to the standard Resource Watch ISO3 code for the country
    INPUT   code: ISO3 code for input country (string)
    RETURN  standard Resource Watch ISO3 code for input country (string)
    '''
    # try to find the RW ISO3 code for the ISO3 code based on loaded conversion table
    temp = country_info.loc[country_info['iso_a3'] == code]
    temp = temp['iso_a3'].tolist()
    if temp != []:
        return temp[0]
    else:
        return None

def fetch_wb_data(table):
    '''
    This function fetches and processes data from the World Bank API for a specified Carto table
    INPUT   table: name of Carto table we want to fetch and process data for (string)
    RETURN  all_world_bank_data: dataframe of processed data for this table (pandas dataframe)
    '''
    # pull the WB indicators that are included in this table
    indicators = wb_rw_table.loc[table, 'wb_indicators'].split(";")
    # pull the list of column names used in the Carto table associated with each indicator
    value_names = wb_rw_table.loc[table, 'wb_columns'].split(";")
    # pull the list of units associated with each indicator
    units = wb_rw_table.loc[table, 'wb_units'].split(";")

    # pull each of the indicators from the World Bank API one at a time
    for i in range(len(indicators)):
        # get the current indicator
        indicator = indicators[i]
        # get the name of the column it will go into in Carto
        value_name = value_names[i]
        # get the units
        unit = units[i]

        # fetch data for this indicator (only the first 10,000 entries will be returned)
        res = requests.get(
            "http://api.worldbank.org/countries/all/indicators/{}?format=json&per_page=10000".format(indicator))
        # check how many pages of data there are for this indicator
        pages = int(res.json()[0]['pages'])

        # pull the data, one page at a time, appending the data to the json variable
        json = []
        for page in range(pages):
            res = requests.get(
                "http://api.worldbank.org/countries/all/indicators/{}?format=json&per_page=10000&page={}".format(
                    indicator, page + 1))
            json = json + res.json()[1]

        # format into dataframe and only keep relevant columns
        data = pd.io.json.json_normalize(json)
        data = data[["country.value", "date", "value"]]
        # rename these columns
        data.columns = ["country_name", "year", value_name]
        # add a units column
        data['unit' + str(i + 1)] = unit
        # add indicator code column
        data['indicator_code' + str(i + 1)] = indicator
        # standardize time column for ISO time
        data["datetime"] = data.apply(lambda x: datetime.date(int(x['year']), 1, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                                  axis=1)

        # only keep country-level data, not larger political bodies
        drop_patterns = ['Arab World', 'Middle income', 'Europe & Central Asia (IDA & IBRD countries)', 'IDA total',
                         'Latin America & the Caribbean (IDA & IBRD countries)',
                         'Middle East & North Africa (IDA & IBRD countries)', 'blank (ID 268)',
                         'Europe & Central Asia (excluding high income)', 'IBRD only', 'IDA only',
                         'Early-demographic dividend', 'Latin America & the Caribbean (excluding high income)',
                         'Middle East & North Africa', 'Middle East & North Africa (excluding high income)',
                         'Late-demographic dividend', 'Pacific island small states', 'Europe & Central Asia',
                         'European Union', 'High income', 'IDA & IBRD total', 'IDA blend', 'Caribbean small states',
                         'Central Europe and the Baltics', 'East Asia & Pacific',
                         'East Asia & Pacific (excluding high income)', 'Low & middle income',
                         'Lower middle income', 'Other small states', 'East Asia & Pacific (IDA & IBRD countries)',
                         'Euro area', 'OECD members', 'North America',
                         'Middle East & North Africa (excluding high income)', 'Post-demographic dividend',
                         'Small states', 'South Asia', 'Upper middle income', 'World',
                         'Heavily indebted poor countries (HIPC)', 'Least developed countries: UN classification',
                         'blank (ID 267)', 'blank (ID 265)', 'Latin America & Caribbean',
                         'Latin America & Caribbean (excluding high income)', 'IDA & IBRD total', 'IBRD only',
                         'Europe & Central Asia', 'Sub-Saharan Africa (excluding high income)', 'Macao SAR China',
                         'Sub-Saharan Africa', 'Pre-demographic dividend', 'South Asia (IDA & IBRD)',
                         'Sub-Saharan Africa (IDA & IBRD countries)', 'Upper middle income',
                         'Fragile and conflict affected situations', 'Low income', 'Not classified']
        data = data[~data['country_name'].isin(drop_patterns)]

        # set index to country_name, datetime, and year so that we can use this information to add more columns correctly
        data = data.set_index(["country_name", "datetime", "year"])

        # if we are processing the first indicator, create the the dataframe with the current data
        if i == 0:
            all_world_bank_data = data
        # for all subsequent indicators, continue adding more columns to the dataframe
        else:
            all_world_bank_data = all_world_bank_data.join(data, how="outer")

    # reset the index for the table so the country_name, datetime, and year return to being columns
    all_world_bank_data = all_world_bank_data.reset_index()

    # add ISO3 codes to table, based on the World Bank country names
    all_world_bank_data.insert(0, "country_code", all_world_bank_data.apply(lambda row: add_iso(row["country_name"]), axis=1))

    # drop rows which don't have an ISO3 assigned
    all_world_bank_data = all_world_bank_data.loc[pd.notnull(all_world_bank_data["country_code"])]

    # add in RW specific country names and ISO codes
    all_world_bank_data["rw_country_name"] = all_world_bank_data.apply(lambda row: add_rw_name(row["country_code"]), axis=1)
    all_world_bank_data["rw_country_code"] = all_world_bank_data.apply(lambda row: add_rw_code(row["country_code"]), axis=1)

    # make sure all null values are set to None
    all_world_bank_data = all_world_bank_data.where((pd.notnull(all_world_bank_data)), None).reset_index(drop=True)

    return all_world_bank_data

def main():
    logging.info('STARTING WORLD BANK CARTO UPDATE')

    # create a new sub-directory within your specified dir called 'data'
    data_dir = 'data'
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    # process each Carto table for World Bank datasets one at a time
    for table_name, info in wb_rw_table.iterrows():
        # get the dataset name (table name without the '_edit' at the end of the table_name
        dataset_name = table_name[:-5]
        logging.info('Next table to update: {}'.format(dataset_name))

        '''
        Download data and save to your data directory
        '''
        # get a list of World Bank indicators that go into this table
        indicators = info['wb_indicators'].split(";")
        # create an empty list to store the raw data files associated with each indicator to go on S3
        raw_data_files = []
        # pull the CSVs for each indicator
        for indicator in indicators:
            # insert the url used to download the data from the source website
            url = f'http://api.worldbank.org/v2/en/indicator/{indicator}?downloadformat=csv'
            # download the data from the source
            raw_data_file = os.path.join(data_dir, f'{indicator}_DS2_en_csv_v2')
            urllib.request.urlretrieve(url, raw_data_file)
            # add the name of the raw data file to the list of raw data files to go on S3
            raw_data_files.append(raw_data_file)

        '''
        Process data
        '''
        # fetch and process World Bank data for this table
        all_world_bank_data = fetch_wb_data(table_name)
        # save processed dataset to csv
        processed_data_file = os.path.join(data_dir, dataset_name + '_edit.csv')
        all_world_bank_data.to_csv(processed_data_file, index=False)

        '''
        Upload processed data to Carto
        '''
        logging.info('Uploading processed data to Carto.')
        # check if table exists
        # if table does not exist, create it
        if not table_name in carto_table_names:
            logging.info(f'Table {table_name} does not exist, creating')
            # Change privacy of table on Carto
            # set up carto authentication using local variables for username (CARTO_WRI_RW_USER) and API key (CARTO_WRI_RW_KEY)
            auth_client = APIKeyAuthClient(api_key=os.getenv('CARTO_WRI_RW_KEY'),
                                           base_url="https://{user}.carto.com/".format(user=os.getenv('CARTO_WRI_RW_USER')))
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
            # column names and types for data table
            # column names should be lowercase
            # column types should be one of the following: geometry, text, numeric, timestamp
            CARTO_SCHEMA = OrderedDict([
                ('country_code', 'text'),
                ('country_name', 'text'),
                ('datetime', 'timestamp'),
                ('year', 'numeric')])
            # Go through each type of "value" in this table
            # Add data column, unit, and indicator code to CARTO_SCHEMA
            valnames = info['Carto Column'].split(";")
            for i in range(len(valnames)):
                # add the name of the column for the values to the Carto schema
                CARTO_SCHEMA.update({valnames[i]: 'numeric'})
                # add the unit column name and type for this value to the Carto schema
                CARTO_SCHEMA.update({'unit' + str(i + 1): 'text'})
                # add the World Bank Indicator Code column name and type for this value to the Carto schema
                CARTO_SCHEMA.update({'indicator_code' + str(i + 1): 'text'})
            # add the Resource Watch country name and country code columns to the Carto schema
            CARTO_SCHEMA.update({"rw_country_name": 'text'})
            CARTO_SCHEMA.update({"rw_country_code": 'text'})

            # delete all the rows
            cartosql.deleteRows(table_name, 'cartodb_id IS NOT NULL', user=CARTO_USER, key=CARTO_KEY)
            # note: we do not delete the entire table because this will cause the dataset visualization on Resource Watch
            # to disappear until we log into Carto and open the table again. If we simply delete all the rows, this
            # problem does not occur

            # insert all data rows for this table
            if len(all_world_bank_data):
                cartosql.blockInsertRows(table_name, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), all_world_bank_data.values.tolist(), user=CARTO_USER, key=CARTO_KEY)
                logging.info('Success! New rows have been added to Carto.')
            else:
                logging.info('No rows to add to Carto.')

        '''
        Upload original data and processed data to Amazon S3 storage
        '''
        logging.info('Uploading original data to S3.')
        # Copy the raw data into a zipped file to upload to S3
        raw_data_dir = os.path.join(data_dir, dataset_name + '.zip')
        with ZipFile(raw_data_dir, 'w') as zip:
            for raw_data_file in raw_data_files:
                zip.write(raw_data_file, os.path.basename(raw_data_file))

        # Upload raw data file to S3
        uploaded = upload_to_aws(raw_data_dir, 'wri-public-data', 'resourcewatch/' + os.path.basename(raw_data_dir))

        logging.info('Uploading processed data to S3.')
        # Copy the processed data into a zipped file to upload to S3
        processed_data_dir = os.path.join(data_dir, dataset_name + '_edit.zip')
        with ZipFile(processed_data_dir, 'w') as zip:
            zip.write(processed_data_file, os.path.basename(processed_data_file))

        # Upload processed data file to S3
        uploaded = upload_to_aws(processed_data_dir, 'wri-public-data', 'resourcewatch/' + os.path.basename(processed_data_dir))