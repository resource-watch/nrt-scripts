''  # !/usr/bin/env python3
if __name__ == '__main__':
    import logging
    import os
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

    CARTO_USER = os.getenv('CARTO_WRI_RW_USER')
    CARTO_KEY = os.getenv('CARTO_WRI_RW_KEY')

    wb_rw_table = pd.read_csv(
        'https://raw.githubusercontent.com/resource-watch/data-pre-processing/master/upload_worldbank_data//WB_RW_dataset_names_ids.csv').set_index(
        'Carto Table')

    # pull in sheet with World Bank name to iso3 conversions
    wb_name_to_iso3_conversion = pd.read_csv(
        'https://raw.githubusercontent.com/resource-watch/data-pre-processing/master/upload_worldbank_data/WB_name_to_ISO3.csv').set_index(
        'WB_name')

    tables_to_not_overwrite_column = ['ene_021a_renewable_energy_consumption',
                                      'soc_081_mortality_rate',
                                      'ene_004_renewable_energy_share_of_total_energy_consumption',
                                      'ene_029a_energy_intensity']
    # get list of all carto table names
    carto_table_names = cartosql.getTables(user=CARTO_USER, key=CARTO_KEY)


    def upload_to_aws(local_file, bucket, s3_file):
        s3 = boto3.client('s3', aws_access_key_id=os.getenv('aws_access_key_id'),
                          aws_secret_access_key=os.getenv('aws_secret_access_key'))
        try:
            s3.upload_file(local_file, bucket, s3_file)
            logging.info("Upload Successful")
            logging.info("http://{}.s3.amazonaws.com/{}".format(bucket, s3_file))
            return True
        except FileNotFoundError:
            logging.info("The file was not found")
            return False
        except NoCredentialsError:
            logging.info("Credentials not available")
            return False


    # Add ISO3 column
    def add_iso(name):
        try:
            return (wb_name_to_iso3_conversion.loc[name, "ISO"])
        except:
            return (np.nan)


    # pull in table of standard Resource Watch names
    sql_statement = 'SELECT iso_a3, name FROM wri_countries_a'
    country_html = requests.get(f'https://{CARTO_USER}.carto.com/api/v2/sql?q={sql_statement}')
    country_info = pd.DataFrame(country_html.json()['rows'])


    def add_rw_name(code):
        temp = country_info.loc[country_info['iso_a3'] == code]
        temp = temp['name'].tolist()
        if temp != []:
            return (temp[0])
        else:
            return (None)


    def add_rw_code(code):
        temp = country_info.loc[country_info['iso_a3'] == code]
        temp = temp['iso_a3'].tolist()
        if temp != []:
            return (temp[0])
        else:
            return (None)


    #
    def fetch_wb_data(table):
        # pull the WB indicators that are included in this table
        indicators = wb_rw_table.loc[table, 'wb_indicators'].split(";")
        # pull the list of column names used in the Carto table associated with each indicator
        value_names = wb_rw_table.loc[table, 'wb_columns'].split(";")
        #
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
            data.columns = ["Country", "Year", value_name]
            # add a units column
            data['unit' + str(i + 1)] = unit
            # add indicator code column
            data['indicator_code' + str(i + 1)] = indicator
            # Standardize time column for ISO time
            data["Time"] = data.apply(lambda x: datetime.date(int(x['Year']), 1, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                                      axis=1)

            # Only keep countries, not larger political bodies
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

            data = data[~data['Country'].isin(drop_patterns)]

            # Set index to Country, Year, and Time
            data = data.set_index(["Country", "Time", "Year"])
            if i == 0:
                # Start off the dataframe
                all_world_bank_data = data
            else:
                # Continue adding more columns to the dataframe
                all_world_bank_data = all_world_bank_data.join(data, how="outer")

            # Reset the index for the table
        all_world_bank_data = all_world_bank_data.reset_index()

        all_world_bank_data.insert(0, "ISO3", all_world_bank_data.apply(lambda row: add_iso(row["Country"]), axis=1))

        # Drop rows which don't have an ISO3 assigned
        all_world_bank_data = all_world_bank_data.loc[pd.notnull(all_world_bank_data["ISO3"])]

        # Add in RW specific countries and ISO codes
        all_world_bank_data["rw_country_name"] = all_world_bank_data.apply(lambda row: add_rw_name(row["ISO3"]), axis=1)
        all_world_bank_data["rw_country_code"] = all_world_bank_data.apply(lambda row: add_rw_code(row["ISO3"]), axis=1)

        # make sure all null values are set to None
        all_world_bank_data = all_world_bank_data.where((pd.notnull(all_world_bank_data)), None).reset_index(drop=True)
        return (all_world_bank_data)

#
for dataset_name, info in wb_rw_table.iterrows():
    dataset_name = dataset_name[:-5]
    logging.info('Next table to update: {}'.format(dataset_name))
    # create a new sub-directory within your specified dir called 'data'
    data_dir = 'data'
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    '''
    Download data and save to your data directory
    '''
    raw_data_files = []
    indicators = info['wb_indicators'].split(";")
    for indicator in indicators:
        url = f'http://api.worldbank.org/v2/en/indicator/{indicator}?downloadformat=csv'
        raw_data_file = os.path.join(data_dir, f'{indicator}_DS2_en_csv_v2')
        raw_data_files.append(raw_data_file)
        urllib.request.urlretrieve(url, raw_data_file)

    '''
    Process data
    '''
    # Creates dataset with all data columns
    all_world_bank_data = fetch_wb_data(dataset_name)
    # save processed dataset to csv
    processed_data_file = os.path.join(data_dir, dataset_name + '_edit.csv')
    all_world_bank_data.to_csv(processed_data_file, index=False)

    '''
    Upload processed data to Carto
    '''
    logging.info('Uploading processed data to Carto.')

    ### Check if table exists
    # if table does not exist, create it
    table_name = dataset_name+'_edit'
    if not table_name in carto_table_names:
        logging.info(f'Table {table_name} does not exist, creating')
        # Change privacy of table on Carto
        # set up carto authentication using local variables for username (CARTO_WRI_RW_USER) and API key (CARTO_WRI_RW_KEY)
        auth_client = APIKeyAuthClient(api_key=os.getenv('CARTO_WRI_RW_KEY'),
                                       base_url="https://{user}.carto.com/".format(user=os.getenv('CARTO_WRI_RW_USER')))
        # set up dataset manager with authentication
        dataset_manager = DatasetManager(auth_client)
        # upload dataset to carto
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
        # Add data column, unit, and indicator code to CARTO_SCHEMA, column_order, and dataset
        valnames = info['Carto Column'].split(";")
        for i in range(len(valnames)):
            CARTO_SCHEMA.update({valnames[i]: 'numeric'})
            # add the unit column name and type for this value  to the Carto Schema
            CARTO_SCHEMA.update({'unit' + str(i + 1): 'text'})
            # add the WB Indicator Code column name and type for this value to the Carto Schema
            CARTO_SCHEMA.update({'indicator_code' + str(i + 1): 'text'})
        # add the RW country name and country code columns to the table
        CARTO_SCHEMA.update({"rw_country_name": 'text'})
        CARTO_SCHEMA.update({"rw_country_code": 'text'})

        cartosql.deleteRows(table_name, 'cartodb_id IS NOT NULL', user=CARTO_USER, key=CARTO_KEY)

        # Insert new observations
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