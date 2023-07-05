import LMIPy as lmi
import os
import pandas as pd
import numpy as np
import requests
import cartoframes
import logging
import sys
import datetime
import time

logging.basicConfig(stream = sys.stderr, level = logging.INFO)

# pull in RW API key for updating and adding new layers
API_TOKEN = os.getenv('RW_API_KEY')

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_WRI_RW_USER')
CARTO_KEY = os.getenv('CARTO_WRI_RW_KEY')

# set up authentication for cartoframes module
auth = cartoframes.auth.Credentials(username = CARTO_USER, api_key = CARTO_KEY)

def get_layers(ds_id):
    '''
    Given a Resource Watch dataset's API ID, this function will return a list of all the layers associated with it
    INPUT   ds_id: Resource Watch API dataset ID (string)
    RETURN  layers: layers for the input dataset (list of dictionaries)
    '''
    # generate the API url for this dataset - this must include the layers
    apiUrl = 'http://api.resourcewatch.org/v1/dataset/{}?includes=layer'.format(ds_id)
    try_num = 1
    while try_num <= 3:
        try:
            # pull the dataset from the API
            r = requests.get(apiUrl)
            #get a list of all the layers
            layers = r.json()['data']['attributes']['layer']
            break
        except:
            logging.info('Fail to get layers info. Try again after 10 seconds.')
            time.sleep(10)
            try_num += 1
    # create an empty list to store the years from the layers
    return layers

def get_layer_years(ds_id):
    '''
    Given a Resource Watch dataset's API ID, this function will return a list of all the years associated with its layers
    INPUT   ds_id: Resource Watch API dataset ID (string)
    RETURN  years: years associated with layers for the input dataset (list of integers)
    '''
    # get all the layers for this dataset
    layers = get_layers(ds_id)
    # create an empty list to store the years from the layers
    years =[]
    # go through each layer and add its year to the list
    for layer in layers:
        # pull out first four characters of layer name (this is where year should be) and turn it into an integer
        year = int(layer['attributes']['name'][:4])
        if year not in years:
            years.append(year)
    return years

def get_carto_years(carto_table, data_col):
    '''
    Given a Carto table name and a column where we expect to have data, this function will return a list of all the
    years for which there is data in the table (as long as there are at least 10 data points for that year or the data
    is less than 10 years old)
    INPUT   carto_table: name of Carto table (string)
            data_col: name of column where we want to make sure we have data (string)
    RETURN  carto_years: years in table for which we have data (list of integers)
    '''

    # if there are multiple data columns to check
    if ';' in data_col:
        # turn columns into a list
        cols = data_col.split(';')
        # generate a WHERE statement to use in SQL query to remove rows where these columns are null
        where = ''
        for col in cols:
            where += col + ' IS NOT NULL AND '
        where = where[:-5]
    # if there is only one column to check
    else:
        # generate a WHERE statement to use in SQL query to remove rows where this column is null
        where = data_col +  ' IS NOT NULL'
    # query Carto table to get rows where there is data
    carto_df = cartoframes.read_carto(f' SELECT * from {carto_table} WHERE {where}', credentials=auth)

    # pull out a list of years from the 'year' column
    carto_years = [int(year) for year in np.unique(carto_df['year'])]
    # get count of occurrences of each year
    vc = carto_df['year'].value_counts()
    # pull out list of years to drop with fewer than 10 data points
    years_to_drop = vc[vc < 10].index
    # keep list of these years to drop that are more than 10 years old
    years_to_drop = [year for year in years_to_drop if year < datetime.datetime.utcnow().year - 10]
    # remove years with less that 10 countries of data, if it is more than 10 years old
    carto_years = [year for year in carto_years if year not in years_to_drop]
    # put these years in order from oldest to newest
    carto_years.sort()
    return carto_years

def duplicate_eia_layers(ds_id, update_years):
    '''
    Given a Resource Watch dataset's API ID and a list of years we want to add to it, this function will create new
    layers on Resource Watch for those years
    INPUT   ds_id: Resource Watch API dataset ID (string)
            update_years: list of years for which we want to add layers to this dataset (list of integers)
    '''

    # pull the dataset we want to update
    dataset = lmi.Dataset(ds_id)
    # pull out its first layer to use as a template to create new layers
    layer_to_clone = dataset.layers[0]

    # get attributes that might need to change:
    name = layer_to_clone.attributes['name']
    description = layer_to_clone.attributes['description']
    appConfig = layer_to_clone.attributes['layerConfig']
    sql = appConfig['body']['layers'][0]['options']['sql']
    order = str(appConfig['order'])
    timeLineLabel = appConfig['timelineLabel']
    interactionConfig = layer_to_clone.attributes['interactionConfig']

    # pull out the year from the example layer's name - we will use this to find all instances of the year within our
    # example layer so that we can replace it with the correct year in the new layers
    replace_string = name[:4]

    # replace year in example layer with {}
    name_convention = name.replace(replace_string, '{}')
    description_convention = description.replace(replace_string, '{}')
    sql_convention = sql.replace(replace_string, '{}')
    order_convention = order.replace(replace_string, '{}')
    timeLineLabel_convention = timeLineLabel.replace(replace_string, '{}')
    for i, dictionary in enumerate(interactionConfig.get('output')):
        for key, value in dictionary.items():
            if value != None:
                if replace_string in value:
                    interactionConfig.get('output')[i][key] = value.replace(replace_string, '{}')

    # go through each year we want to add a layer for
    for year in update_years:
        # generate the layer attributes with the correct year
        new_layer_name = name_convention.replace('{}', str(year))
        new_description = description_convention.replace('{}', str(year))
        new_sql = sql_convention.replace('{}', str(year))
        new_timeline_label = timeLineLabel_convention.replace('{}', str(year))
        new_order = int(order_convention.replace('{}', str(year)))

        # Clone the example layer to make a new layer
        clone_attributes = {
            'name': new_layer_name,
            'description': new_description
        }
        new_layer = layer_to_clone.clone(token=API_TOKEN, env='production', layer_params=clone_attributes,
                                         target_dataset_id=ds_id)

        # Replace layerConfig with new values
        appConfig = new_layer.attributes['layerConfig']
        appConfig['body']['layers'][0]['options']['sql'] = new_sql
        appConfig['order'] = new_order
        appConfig['timelineLabel'] = new_timeline_label
        payload = {
            'layerConfig': {
                **appConfig
            }
        }
        new_layer = new_layer.update(update_params=payload, token=API_TOKEN)

        # Replace interaction config with new values
        interactionConfig = new_layer.attributes['interactionConfig']
        for i, element in enumerate(interactionConfig['output']):
            if '{}' in element.get('property'):
                interactionConfig['output'][i]['property'] = interactionConfig['output'][i]['property'].replace(
                    '{}', str(year))
        payload = {
            'interactionConfig': {
                **interactionConfig
            }
        }
        new_layer = new_layer.update(update_params=payload, token=API_TOKEN)

        # Replace layer name and description
        payload = {
            'name': new_layer_name,
            'description': new_description
        }
        new_layer = new_layer.update(update_params=payload, token=API_TOKEN)

        logging.info(new_layer)
        logging.info('\n')

def update_rw_layer_year(ds_id, current_year, new_year):
    '''
    Given a Resource Watch dataset's API ID, the current year it is showing data for, and the year we want to change it
    to, this function will update all layers to show data for the new year
    INPUT   ds_id: Resource Watch API dataset ID (string)
            current_year: current year used in dataset layers (integer)
            new_year: year we want to change the layers to show data for (integer)
    '''
    # pull the dataset we want to update
    dataset = lmi.Dataset(ds_id)

    # go through and update each of the layers
    for layer in dataset.layers:
        # Replace layer config with new values
        appConfig = layer.attributes['layerConfig']
        new_sql = appConfig['body']['layers'][0]['options']['sql'].replace(str(current_year), str(new_year))
        appConfig['body']['layers'][0]['options']['sql'] = new_sql
        payload = {
            'layerConfig': {
                **appConfig
            }
        }
        layer = layer.update(update_params=payload, token=API_TOKEN)

        # Replace interaction config with new values
        interactionConfig = layer.attributes['interactionConfig']
        for i, element in enumerate(interactionConfig['output']):
            interactionConfig['output'][i]['property'] = interactionConfig['output'][i]['property'].replace(str(current_year), str(new_year))
        payload = {
            'interactionConfig': {
                **interactionConfig
            }
        }
        layer = layer.update(update_params=payload, token=API_TOKEN)

        # Replace layer name and description
        new_name = layer.attributes['name'].replace(str(current_year), str(new_year))
        new_description = layer.attributes['description'].replace(str(current_year), str(new_year))
        payload = {
            'name': new_name,
            'description': new_description
        }
        layer = layer.update(update_params=payload, token=API_TOKEN)
        logging.info(layer)

def update_default_layer(ds_id, default_year):
    '''
    Given a Resource Watch dataset's API ID and the year we want to set as the default layer, this function will 
    update the default layer on Resource Watch
    INPUT   ds_id: Resource Watch API dataset ID (string)
            default_year: year to be used as default layer on Resource Watch (integer)
    '''
    # pull the dataset we want to update
    dataset = lmi.Dataset(ds_id)
    for layer in dataset.layers:
        # check which year the current layer is for
        year = layer.attributes['name'][:4]
        # check if this is currently the default layer
        default = layer.attributes['default']
        # if it is the year we want to set as default, and it is not already set as default,
        # update the 'default' parameter to True
        if year == str(default_year) and default==False:
            payload = {
                'default': True}
            # update the layer on the API
            layer = layer.update(update_params=payload, token=API_TOKEN)
            print(f'default layer updated to {year}')
        # if this layer should no longer be the default layer, but it was previously,
        # make sure the 'default' parameter is False
        elif year != str(default_year) and default==True:
            payload = {
                'default': False}
            # update the layer on the API
            layer = layer.update(update_params=payload, token=API_TOKEN)
            print(f'{year} is no longer default layer')

def main():
    logging.info('STARTING EIA RW LAYER UPDATE')

    # read in csv containing information relating Carto tables to RW datasets
    url='https://raw.githubusercontent.com/resource-watch/nrt-scripts/master/upload_eia_data/EIA_RW_dataset_names_ids.csv'
    df = pd.read_csv(url)

    # go through each Resource Watch dataset and make sure it is up to date with the most recent data
    for i, row in df.iterrows():
        # if there is a dataset ID in the table
        if type(row['Dataset ID']) == str:
            # some rows contain more than one dataset
            ds_ids = row['Dataset ID'].split(';')
            for i in range(len(ds_ids)):
                # pull in relevant information about dataset
                ts = row['Time Slider']
                ds_id = ds_ids[i]
                carto_table = row['Carto Table']
                carto_col = row ['Carto Column']

                # get all the years that we have already made layers for on RW
                rw_years = get_layer_years(ds_id)

                # get all years available in Carto table (with more than 10 data points, or less than 10 yrs old)
                carto_years = get_carto_years(carto_table, carto_col)
                logging.info(f'dataset being checked for currency on RW: {ds_id}')

                # if this dataset is a time slider on RW,
                if ts=='Yes':
                    # find years that we need to make layers for (data available on Carto, but no layer on RW)
                    update_years = np.setdiff1d(carto_years, rw_years)
                    logging.info(f'layers for the following years are being added: {update_years}')
                    # make layers for missing years
                    duplicate_eia_layers(ds_id, update_years)
                    # get a list of all years with layers on Resource Watch (previous + new)
                    all_years = rw_years+list(update_years)
                    # pull the most recent year on Resource Watch
                    all_years.sort()
                    most_recent_year = all_years[-1]
                    # set this year as the default layer
                    update_default_layer(ds_id, most_recent_year)

                # if this dataset is not a time slider on RW
                else:
                    # pull the year of data being shown in the RW dataset's layers
                    rw_year = rw_years[0]
                    # get the most recent year of data available in the Carto table
                    latest_carto_year = carto_years[-1]
                    # if we don't have the latest years on RW, update the existing layers
                    if rw_year != latest_carto_year:
                        logging.info(f'layers being updated for new year: {latest_carto_year}')
                        # update layer on RW to be latest year of data available
                        update_rw_layer_year(ds_id, rw_year, latest_carto_year)

    logging.info('SUCCESS')
main()