import dotenv
#insert the location of your .env file here:
dotenv.load_dotenv('/home/eduardo/Documents/RW_github/cred/.env')
import webbrowser, sys, bs4
import pandas as pd
from datetime import date, timedelta, datetime
import numpy as np
import re
from urllib.parse import urljoin
import os
from os import listdir
from os.path import isfile, join
import requests
import logging
import sys
import time
from collections import OrderedDict
import cartosql
import json
from filecmp import cmp
import urllib
import flat_table

#Setting up logging
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = False

# name of data directory in Docker container
DATA_DIR = 'data'

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')
# asserting table structure rather than reading from input
# We will create two tables in this script, due to the different dissagregation levels.
CARTO_NODES_TABLE = 'loc_mx_ene_nodes'
CARTO_LOAD_ZONES_TABLE = 'loc_mx_ene_load_zones'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_NODES_SCHEMA = OrderedDict([
       ('the_geom', 'geometry'),
       ('uid', 'numeric'),
       ('node_id', 'text'),
       ('node_name','text'),
       ('system','text'),
       ('control_center','text'),
       ('load_zone','text'),
       ('state_code', 'text'),
       ('state','text'),
       ('municipality_code','text'),
       ('municipality','text'),
       ('longitude','numeric'),
       ('latitude','numeric')
    ])

CARTO_LOAD_ZONES_SCHEMA = OrderedDict([
       ('the_geom', 'geometry'),
       ('uid', 'numeric'),
       ('system', 'text'),
       ('load_zone','text'),
       ('state_code', 'text'),
       ('state','text'),
       ('municipality_code','text'),
       ('municipality','text'),
       ('longitude','numeric'),
       ('latitude','numeric')
    ])

UID_FIELD = 'uid'
TIME_FIELD = 'entry_date'
NODE_FIELD = 'node_id'
LOAD_FIELD = 'load_zone'

CARTO_USER = os.environ.get('CARTO_USER')
CARTO_KEY = os.environ.get('CARTO_KEY')

#DATASET_ID =
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
#This function fetches the coordinates for municipalities from the INEGI API.
def inegi_localidad(df): 
    links=[]
    frames= []
    for index,row in df.iterrows():        
        localidad_url= 'https://gaia.inegi.org.mx/wscatgeo/localidades/{}{}0001'.format(row['CLAVE DE ENTIDAD FEDERATIVA (INEGI)'],row['CLAVE DE MUNICIPIO (INEGI)'])
        links.append(localidad_url)
    for link in links:
        try:
            res = requests.get(link, timeout=None)
            res.raise_for_status()
            frames.append(res.json())
        except Exception as exc:
            print('There was a problem: %s' % (exc))
    response_frame = pd.json_normalize(frames)
    response_frame = flat_table.normalize(response_frame)
    response_frame = response_frame[['datos.longitud','datos.latitud','datos.cve_agem','datos.cve_agee']]
    response_frame['datos.longitud'] = response_frame['datos.longitud'].astype('float64')
    response_frame['datos.latitud'] = response_frame['datos.latitud'].astype('float64')
    response_frame['datos.cve_agem'] = response_frame['datos.cve_agem'].astype('str')
    response_frame['datos.cve_agee'] = response_frame['datos.cve_agee'].astype('str')
    response_frame = response_frame.drop_duplicates().reset_index(drop=True)
    return response_frame

#This two functions update the nodes files from the source, concatenate the data and then 
#merge the file with the coordinates provided by INEGI
def xlsx_search(href):
    return href and re.compile(".xlsx").search(href)
###
def download_nodes():
    try:
        res = requests.get('https://www.cenace.gob.mx/Paginas/SIM/NodosP.aspx');
        res.raise_for_status() 
        noStarchSoup = bs4.BeautifulSoup(res.content, 'html.parser');
        table = noStarchSoup.find_all(href=xlsx_search)
        table=noStarchSoup.select('a[href$=".xlsx"]')

        ficheros=os.listdir(os.getcwd())
        for file in table:
            file_name=file['href'][34:-5]+'.xlsx'
            if file_name in ficheros:
                print("File {} exists in current directory".format(file_name))
            else:
                print("File {} doesn't exists in current directory".format(file_name)) 
                link=urljoin('https://www.cenace.gob.mx',file['href'])
                try:
                    resp=requests.get(link)
                    resp.raise_for_status() 
                    output = open(file_name, 'wb')
                    output.write(resp.content)
                    output.close()
                except Exception as exc:
                    print('There was a problem: %s' % (exc))        
    except Exception as exc2:
            return print('There was a problem: %s' % (exc2))
    ficheros=[file for file in os.listdir(os.getcwd()) if file.endswith('.xlsx')]
    d3=pd.DataFrame()
    for f in ficheros:
        df=pd.read_excel(f,skiprows=1) 
        d3=pd.concat([d3,df])
    d3.drop_duplicates(subset=['CLAVE'], inplace=True)
    d3.sort_values('SISTEMA', inplace=True)
    d3 = d3[['CLAVE', 'NOMBRE','SISTEMA','CENTRO DE CONTROL REGIONAL','ZONA DE CARGA','CLAVE DE ENTIDAD FEDERATIVA (INEGI)','ENTIDAD FEDERATIVA (INEGI)','CLAVE DE MUNICIPIO (INEGI)','MUNICIPIO (INEGI)']]
    d3['CLAVE DE ENTIDAD FEDERATIVA (INEGI)'] = d3['CLAVE DE ENTIDAD FEDERATIVA (INEGI)'].astype(str).str.zfill(2)
    d3['CLAVE DE MUNICIPIO (INEGI)'] = d3['CLAVE DE MUNICIPIO (INEGI)'].astype(str).str.zfill(3)
    d3.reset_index(drop=True, inplace=True)
    merged_d3 = pd.merge(d3, inegi_localidad(d3), how = 'left', left_on = ['CLAVE DE ENTIDAD FEDERATIVA (INEGI)', 'CLAVE DE MUNICIPIO (INEGI)'], right_on = ['datos.cve_agee','datos.cve_agem'])
    merged_nodes = merged_d3[['CLAVE','NOMBRE','SISTEMA','CENTRO DE CONTROL REGIONAL','ZONA DE CARGA', 'CLAVE DE ENTIDAD FEDERATIVA (INEGI)','ENTIDAD FEDERATIVA (INEGI)','CLAVE DE MUNICIPIO (INEGI)','MUNICIPIO (INEGI)','datos.longitud','datos.latitud']]
    merged_nodes = merged_nodes.reset_index(drop=True)
    merged_nodes.columns = ['NODE_ID','NODE_NAME','SYSTEM','CONTROL_CENTER','LOAD_ZONE','STATE_CODE', 'STATE', 'MUNICIPALITY_CODE','MUNICIPALITY', 'LONGITUDE','LATITUDE']
    merged_nodes.columns= merged_nodes.columns.str.strip().str.lower()
    
    return merged_nodes

#This function download the load zones from cenace's api, then merge the file with inegi coordinates
def load_zone_download():
    ficheros=[file for file in os.listdir(os.getcwd()) if file.endswith('.xlsx')]
    d3=pd.DataFrame()
    for f in ficheros:
        df=pd.read_excel(f,skiprows=1) 
        d3=pd.concat([d3,df])
    d3.drop_duplicates(subset=['CLAVE'], inplace=True)
    d3.sort_values('SISTEMA', inplace=True)
    group_zone=d3[d3['ZONA DE CARGA']!= 'No Aplica'][['SISTEMA','ZONA DE CARGA', 'CLAVE DE ENTIDAD FEDERATIVA (INEGI)','ENTIDAD FEDERATIVA (INEGI)','CLAVE DE MUNICIPIO (INEGI)','MUNICIPIO (INEGI)']].drop_duplicates(subset='ZONA DE CARGA')
    group_zone['CLAVE DE ENTIDAD FEDERATIVA (INEGI)'] = group_zone['CLAVE DE ENTIDAD FEDERATIVA (INEGI)'].astype(str).str.zfill(2)
    group_zone['CLAVE DE MUNICIPIO (INEGI)'] = group_zone['CLAVE DE MUNICIPIO (INEGI)'].astype(str).str.zfill(3)
    group_zone.reset_index(drop=True, inplace=True)
    merged_d3 = pd.merge(group_zone, inegi_localidad(group_zone), how = 'left', left_on = ['CLAVE DE ENTIDAD FEDERATIVA (INEGI)', 'CLAVE DE MUNICIPIO (INEGI)'], right_on = ['datos.cve_agee','datos.cve_agem'])
    merged_zones = merged_d3[['SISTEMA','ZONA DE CARGA', 'CLAVE DE ENTIDAD FEDERATIVA (INEGI)','ENTIDAD FEDERATIVA (INEGI)','CLAVE DE MUNICIPIO (INEGI)','MUNICIPIO (INEGI)','datos.longitud','datos.latitud']]
    merged_zones = merged_zones.reset_index(drop=True)
    merged_zones.columns = ['SYSTEM','LOAD_ZONE','STATE_CODE', 'STATE', 'MUNICIPALITY_CODE','MUNICIPALITY', 'LONGITUDE','LATITUDE']
    merged_zones['LOAD_ZONE'] = merged_zones['LOAD_ZONE'].replace('-','')
    merged_zones = merged_zones.replace(to_replace="\s\s*",value = '',regex=True)
    merged_zones.columns= merged_zones.columns.str.strip().str.lower()
    
    return merged_zones

def get_nodes_zones(table,param_column):
    '''
    Fetch a list of the required parameter in carto table
    INPUT   table: name of table in Carto we want to get a list of existing information
    RETURN  list of entries for required parameter
    '''
    # get ids of nodes or name of load zones
    r = cartosql.getFields(param_column, table, f='csv', post=True, user=CARTO_USER, key=CARTO_KEY)
    # turn the response into a list of nodes ids or name of load zones
    lis_param= r.text.split('\r\n')[1:-1]
    # sort the parameters
    lis_param.sort()
    
    return lis_param
def upload_data(df,param_column,existing_ids,CARTO_TABLE,CARTO_SCHEMA):
    #Check if fetched entries are already in the carto table, and if so removes them.
    df = df[~df[param_column].isin(get_nodes_zones(CARTO_TABLE, param_column))]
    # create a 'uid' column to store the index of rows as unique ids
    df = df.reset_index(drop=True)
    df['uid'] = df.index + max(existing_ids, default=0)
    # create 'the_geom' column to store the geometry of the data points
    df['the_geom'] = [{'type': 'Point','coordinates': [x, y]} for (x, y) in zip(df['longitude'], df['latitude'])]
    #Turn empty spaces and other characters to null
    df = df.where(pd.notnull(df), None)
    # reorder the columns in the dataframe based on the keys from the dictionary "CARTO_SCHEMA"
    df = df[CARTO_SCHEMA.keys()]
    if len(df):
        # find the length of the data
        num_new = len(df)
        # create a list of new data
        data = df.values.tolist()
        # insert new data into the carto table
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), data, user=CARTO_USER, key=CARTO_KEY)

    return(num_new)

def main():
      
    # Check if table exists, create it if it does not
    logging.info('Checking if nodes table exists and getting existing IDs.')
    nodes_existing_ids = checkCreateTable(CARTO_NODES_TABLE, CARTO_NODES_SCHEMA, UID_FIELD, TIME_FIELD)
    # Fetch, process, and upload new data
    logging.info('Fetching nodes!')
    new_nodes = download_nodes()
    #Updating nodes table
    logging.info('Upload nodes!')
    num_new = upload_data(new_nodes,NODE_FIELD, nodes_existing_ids,CARTO_NODES_TABLE, CARTO_NODES_SCHEMA)
    logging.info('Previous rows: {},  New rows: {}'.format(len(nodes_existing_ids), num_new))
    
    #Repeating process for load zones table
    logging.info('Checking if load zones table exists and getting existing IDs.')
    load_zones_existing_ids = checkCreateTable(CARTO_LOAD_ZONES_TABLE, CARTO_LOAD_ZONES_SCHEMA, UID_FIELD, TIME_FIELD)
    #Fetch, process, and upload new data
    new_zones = load_zone_download()    
    #Updating nodes table
    logging.info('Upload load zones!')
    num_new = upload_data(new_zones,LOAD_FIELD, load_zones_existing_ids, CARTO_LOAD_ZONES_TABLE, CARTO_LOAD_ZONES_SCHEMA)
    logging.info('Previous rows: {},  New rows: {}'.format(len(load_zones_existing_ids), num_new))