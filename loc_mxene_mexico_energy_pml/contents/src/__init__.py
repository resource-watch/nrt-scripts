import sys, bs4
import requests
import pandas as pd
from shapely.geometry import Polygon, mapping
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
import numpy as np
import re
from urllib.parse import urljoin
import os
from filecmp import cmp
from os import listdir
from os.path import isfile, join
import urllib
import cartoframes
import logging
from collections import OrderedDict
import cartosql

#Setting up logging
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = True

# name of data directory in Docker container
DATA_DIR = 'data'

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')
# asserting table structure rather than reading from input
# We will create four tables for this dataset, due to the different dissagregation levels.
CARTO_NODES_DASH_PML_TABLE = 'dash_loc_mx_ene_nodes'
CARTO_LOAD_DASH_PML_TABLE = 'dash_loc_mx_ene_load'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_NODES_DASH_SCHEMA = OrderedDict([
       ('the_geom', 'geometry'),
       ('uid', 'numeric'),
       ('entry_date', 'timestamp'),
       ('node_id', 'text'),
       ('pml','numeric'),
       ('energy','numeric'),
       ('losses','numeric'),
       ('congestion','numeric'),
       ('node_name','text'),
       ('system','text'),
       ('control_center','text'),
       ('load_zone','text'),
       ('state_code', 'text'),
       ('state','text'),
       ('municipality_code', 'text'),
       ('municipality','text'),
       ('last_week_pct_change', 'numeric'),
       ('last_month_pct_change', 'numeric'),
       ('last_year_pct_change', 'numeric'),
       ('pml_label', 'text'),
       ('pml_syst_avg', 'text'),
       ('pml_syst_pct_change', 'text')
    ])

CARTO_LOAD_DASH_SCHEMA = OrderedDict([
       ('the_geom', 'geometry'),
       ('uid', 'numeric'),
       ('entry_date', 'timestamp'),
       ('load','numeric'),
       ('system', 'text'),
       ('load_zone','text'),
       ('state_code', 'text'),
       ('state','text'),
       ('municipality_code', 'text'),
       ('municipality','text'),
       ('load_syst_total', 'numeric'),
       ('load_syst_pct', 'numeric')
    ])
# how many rows can be stored in the Carto table before the oldest ones are deleted?
MAX_ROWS = 1000000

UID_FIELD = 'uid'
TIME_FIELD = 'entry_date'
NODE_FIELD = 'node_id'
LOAD_FIELD = 'load_zone'

#Getting dates that we will fetch
yesterday = date.today() - timedelta(days=1)
last_week = yesterday - timedelta(days=7)
last_month = yesterday - relativedelta(months=1)
last_year = yesterday - relativedelta(years=1)

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
    most_recent_date = datetime.strptime(dates[-1], '%Y-%m-%d %H:%M:%S')
    return most_recent_date

#This function builds the url's to make the requests to cenace's api.
def PMLs_URL(df, dia,dfin):
    url2=[]
    sistema=df['system'].unique()
    for syst in sistema:
        key = df[df['system']==syst]['node_id']
        nodos=[key[i:i + 20] for i in range(0, len(key), 20)]
        pml_url='https://ws01.cenace.gob.mx:8082/SWPML/SIM/{}/MDA'.format(syst) 
        for lst in nodos:   
            url2.append('/'.join([pml_url, ','.join(m for m in lst),dia,dfin,'XML']))
    return url2
    
#This function concatenates the content of the lists and melts the hours columns
def db_mkr(lis,db):
    tmp = pd.concat(lis)
    tmp.columns = ['FECHA','ID_NODO','H1','H2','H3','H4','H5','H6','H7','H8','H9','H10','H11','H12','H13','H14','H15','H16','H17','H18','H19','H20','H21','H22','H23','H24']
    db = pd.concat([db,tmp]).drop_duplicates(subset=['FECHA','ID_NODO'], keep='last')
    db = pd.melt(db, id_vars=['FECHA','ID_NODO'], value_vars=['H1','H2','H3','H4','H5','H6','H7','H8','H9','H10','H11','H12','H13','H14','H15','H16','H17','H18','H19','H20','H21','H22','H23','H24'], var_name = 'HOUR')
    db['value'] = db['value'].astype('float64')
    db = db.groupby(['ID_NODO','FECHA']).agg({'value': np.mean}).reset_index()    
    db['FECHA'] = pd.to_datetime(db['FECHA'], format="%Y-%m-%d")
    db = db.reset_index(drop=True)
    return db

#This function performs the call to cenace api and process data
def fetcher_nodes(yesterday, last_week, last_month, last_year):
    lis_pml = []
    lis_ene = []
    lis_per = []
    lis_cng = []
    auth=cartoframes.auth.Credentials(username=CARTO_USER, api_key=CARTO_KEY)
    df = cartoframes.read_carto('loc_mx_ene_nodes', credentials=auth)
    db_pml = pd.DataFrame(columns = ['FECHA','ID_NODO','H1','H2','H3','H4','H5','H6','H7','H8','H9','H10','H11','H12','H13','H14','H15','H16','H17','H18','H19','H20','H21','H22','H23','H24'])
    db_ene = pd.DataFrame(columns = ['FECHA','ID_NODO','H1','H2','H3','H4','H5','H6','H7','H8','H9','H10','H11','H12','H13','H14','H15','H16','H17','H18','H19','H20','H21','H22','H23','H24'])
    db_per = pd.DataFrame(columns = ['FECHA','ID_NODO','H1','H2','H3','H4','H5','H6','H7','H8','H9','H10','H11','H12','H13','H14','H15','H16','H17','H18','H19','H20','H21','H22','H23','H24'])
    db_cng = pd.DataFrame(columns = ['FECHA','ID_NODO','H1','H2','H3','H4','H5','H6','H7','H8','H9','H10','H11','H12','H13','H14','H15','H16','H17','H18','H19','H20','H21','H22','H23','H24'])
    #dinicio = date(2020,12,16) ##Use this function the first time to update the table
    #dfin = date.today() ###to current date
    #dinicio = (get_most_recent_date(CARTO_NODES_PML_TABLE) +timedelta(days=1)).date()
    #dfin =  dinicio + timedelta(days=6)
    print('Downloading information for dates {} - {} \n'.format(yesterday,yesterday))
    links=PMLs_URL(df,yesterday.strftime("%Y/%m/%d"),yesterday.strftime("%Y/%m/%d"))
    print('Downloading information for dates {} - {} \n'.format(last_week,last_week))
    links.extend(PMLs_URL(df,last_week.strftime("%Y/%m/%d"),last_week.strftime("%Y/%m/%d")))
    print('Downloading information for dates {} - {} \n'.format(last_month,last_month))
    links.extend(PMLs_URL(df,last_month.strftime("%Y/%m/%d"),last_month.strftime("%Y/%m/%d")))
    print('Downloading information for dates {} - {} \n'.format(last_year,last_year))
    links.extend(PMLs_URL(df,last_year.strftime("%Y/%m/%d"),last_year.strftime("%Y/%m/%d")))
            
    for link in links:
        try:
            res = requests.get(link, timeout=None)
            res.raise_for_status()  
            noStarchSoup = bs4.BeautifulSoup(res.text, 'xml')
            aux_noSoup = str(noStarchSoup).split('</nodo>')
            
            for nS in aux_noSoup:
                scrape_fecha=list(map(lambda x: x.getText(), bs4.BeautifulSoup(nS).select('fecha')))
                scrape_node = bs4.BeautifulSoup(nS).select('clv_nodo')
                scrape_hora = list(map(lambda x: float(x.getText()), bs4.BeautifulSoup(nS).select('hora')))
                scrape_pml = list(map(lambda x: float(x.getText()), bs4.BeautifulSoup(nS).select('pml')))
                scrape_pml_ene = list(map(lambda x: float(x.getText()), bs4.BeautifulSoup(nS).select('pml_ene')))
                scrape_pml_per = list(map(lambda x: float(x.getText()), bs4.BeautifulSoup(nS).select('pml_per')))
                scrape_pml_cng = list(map(lambda x: float(x.getText()), bs4.BeautifulSoup(nS).select('pml_cng')))
                
                if len(scrape_pml)>0:
                    for i in range(len(scrape_node)):
                        c_node = scrape_node[i].getText()
                        parsed_row = df.loc[df['node_id'].isin([c_node])]
                        nodo_id =tuple(parsed_row['node_id'])[0]                                        
                    
                        fecha=[]
                        [fecha.append(f) for f in scrape_fecha if f not in fecha]
                        to_db_pml, to_db_ene, to_db_per, to_db_cng={}, {}, {}, {}
                        
                        for f in fecha:
                            for i in range(1,25):
                                to_db_pml[f,nodo_id, i]=0
                                to_db_ene[f,nodo_id, i]=0
                                to_db_per[f,nodo_id, i]=0
                                to_db_cng[f,nodo_id, i]=0
                        
                            for index, f in enumerate(scrape_fecha):
                                to_db_pml[f, nodo_id, scrape_hora[index]]=scrape_pml[index] 
                                to_db_ene[f, nodo_id, scrape_hora[index]]=scrape_pml_ene[index] 
                                to_db_per[f, nodo_id, scrape_hora[index]]=scrape_pml_per[index] 
                                to_db_cng[f, nodo_id, scrape_hora[index]]=scrape_pml_cng[index]
                            
                            for index, f in enumerate(fecha):
                                dlis=[f,nodo_id]
                                [dlis.append(to_db_pml[f, nodo_id, i]) for i in range(1,25)]
                                if not len(db_pml[(db_pml['FECHA']==f) & (db_pml['ID_NODO']==nodo_id)]):
                                    row_frame = pd.DataFrame(dlis).T
                                    lis_pml.append(row_frame) 
                                else:
                                    print('Node already registered {} with this date {}'.format(nodo_id,f))        
                        
                                dlis=[f,nodo_id]
                                [dlis.append(to_db_ene[f, nodo_id, i]) for i in range(1,25)]
                                if not len(db_ene[(db_ene['FECHA']==f) & (db_ene['ID_NODO']==nodo_id)]):
                                    row_frame = pd.DataFrame(dlis).T
                                    lis_ene.append(row_frame)
                        
                                dlis=[f,nodo_id]
                                [dlis.append(to_db_per[f, nodo_id, i]) for i in range(1,25)]
                                if not len(db_per[(db_per['FECHA']==f) & (db_per['ID_NODO']==nodo_id)]):
                                    row_frame = pd.DataFrame(dlis).T
                                    lis_per.append(row_frame)
                        
                                dlis=[f,nodo_id]
                                [dlis.append(to_db_cng[f, nodo_id, i]) for i in range(1,25)]
                                if not len(db_cng[(db_cng['FECHA']==f) & (db_cng['ID_NODO']==nodo_id)]):
                                    row_frame = pd.DataFrame(dlis).T
                                    lis_cng.append(row_frame)
                                        
        except Exception as exc:
            print('There was a problem: %s' % (exc))
    #Running function to concatenate dataframes
    db_pml = db_mkr(lis_pml,db_pml)
    db_ene = db_mkr(lis_ene,db_ene)
    db_per = db_mkr(lis_per,db_per)
    db_cng = db_mkr(lis_cng,db_cng)
    
    #Merging pml, energy, losses and congestion tables with nodes table
    merged_tmp = pd.merge(db_pml, db_ene, left_on=['FECHA','ID_NODO'], right_on=['FECHA','ID_NODO'], how='outer')
    merged_tmp.rename(columns={'value_x':'pml', 'value_y':'energy'}, inplace=True)
    merged_tmp = pd.merge(merged_tmp, db_per, left_on=['FECHA','ID_NODO'], right_on=['FECHA','ID_NODO'], how='outer')
    merged_tmp.rename(columns={'value':'losses'}, inplace=True)
    merged_tmp = pd.merge(merged_tmp, db_cng, left_on=['FECHA','ID_NODO'], right_on=['FECHA','ID_NODO'], how='outer')
    merged_tmp.rename(columns={'value':'congestion'}, inplace=True)
    #####Merging pml with nodes table
    merged_nodes = pd.merge(merged_tmp, df, left_on =['ID_NODO'], right_on =['node_id'], how='right')    
    merged_nodes.drop('ID_NODO', axis=1, inplace=True)
    merged_nodes.rename(columns={'FECHA':'entry_date'}, inplace=True)    
    merged_nodes = merged_nodes.sort_values(['node_id', 'entry_date'], ascending=[True, True]).reset_index(drop=True)
    # Group on keys and call `pct_change` inside `apply`.
    merged_nodes['last_week_pct_change'] = merged_nodes.groupby('node_id', sort=False)['pml'].apply(lambda x: x.pct_change()).to_numpy()
    merged_nodes['last_month_pct_change'] = merged_nodes.groupby('node_id', sort=False)['pml'].apply(lambda x: x.pct_change(periods=2)).to_numpy()
    merged_nodes['last_year_pct_change'] = merged_nodes.groupby('node_id', sort=False)['pml'].apply(lambda x: x.pct_change(periods=3)).to_numpy()
    #Labeling PML ranges to simplify visualization on RW backoffice.
    criteria = [merged_nodes['pml'].between(0, 49), merged_nodes['pml'].between(50, 249), merged_nodes['pml'].between(250,499),merged_nodes['pml'].between(500,749),merged_nodes['pml'].between(750,999), merged_nodes['pml'].between(1000,1499),merged_nodes['pml'].between(1500,1999),merged_nodes['pml'].between(2000, 2499),merged_nodes['pml'].between(2500, 2999),merged_nodes['pml'].between(3000, 3999),merged_nodes['pml'].between(4000,4999),merged_nodes['pml'].between(5000, 5999), merged_nodes['pml'].between(6000, 20000)]
    values = ['<50','50-249','250-499','500-749','750-999','1000-1499','1500-1999','2000-2499','2500-2999','3000-3999','4000-4999','5000-5999','>6000']
    merged_nodes['pml_label'] = np.select(criteria, values, 0)
    merged_nodes['pml_label'] = merged_nodes['pml_label'].replace(['0',0],np.nan)
    #Calculating whole system average per date and adding pct_change against node pml value.
    system_avg = merged_nodes.groupby('entry_date').agg(pml_syst_avg=('pml', 'mean'))
    merged_nodes = pd.merge(merged_nodes, system_avg, on='entry_date', how='left')
    merged_nodes['pml_syst_pct_change'] = (merged_nodes.pml - merged_nodes.pml_syst_avg)/merged_nodes.pml_syst_avg * 100
    
    return merged_nodes

#The following functions perform the scraping process at the load zone dissagregation level

#This function builds the urls to perform the api calls to download load zones information
def load_zones_url(group_zona, dinicio,dfin):
    zones=group_zona['system'].unique()    
    AU_url='https://ws01.cenace.gob.mx:8082/SWCAEZC/SIM'
    group_zona['load_zone']=list(map(lambda x: '-'.join(x.split()), group_zona['load_zone'])) 
    url2=[] 
    for zone in zones:
        list_zonas=list(group_zona[group_zona['system']==zone]['load_zone'])
        split_zone=[list_zonas[i:i + 10] for i in range(0, len(list_zonas), 10)]
        for z in split_zone:
            url2.append('/'.join([AU_url, zone, 'MDA' , ','.join(m for m in z),dinicio,dfin,'XML']))
    return url2

#This function performs concatenation and cleaning of cenace information
def db_load_mkr(lis,db):
    tmp = pd.concat(lis)
    tmp.columns = ['FECHA','SISTEMA','ZONA_CARGA','H1','H2','H3','H4','H5','H6','H7','H8','H9','H10','H11','H12','H13','H14','H15','H16','H17','H18','H19','H20','H21','H22','H23','H24']
    db = pd.concat([db,tmp]).drop_duplicates(subset=['FECHA','SISTEMA','ZONA_CARGA'], keep='last')
    db = pd.melt(db, id_vars=['FECHA','SISTEMA','ZONA_CARGA'], value_vars=['H1','H2','H3','H4','H5','H6','H7','H8','H9','H10','H11','H12','H13','H14','H15','H16','H17','H18','H19','H20','H21','H22','H23','H24'], var_name = 'HOUR')
    db['value'] = db['value'].astype('float64')
    db = db.groupby(['ZONA_CARGA','SISTEMA','FECHA']).agg({'value': np.mean}).reset_index()    
    db['FECHA'] = pd.to_datetime(db['FECHA'], format="%Y-%m-%d")
    db = db.reset_index(drop=True)
    
    return db

#This function downloads information from CENACE API at the load zone level
def fetcher_load(yesterday, last_week, last_month, last_year):
    lis_ca = []
    auth=cartoframes.auth.Credentials(username=CARTO_USER, api_key=CARTO_KEY)
    df = cartoframes.read_carto('loc_mx_ene_load_zones', credentials=auth)
    db_ca=pd.DataFrame(columns = ['FECHA','SISTEMA','ZONA_CARGA','H1','H2','H3','H4','H5','H6','H7','H8','H9','H10','H11','H12','H13','H14','H15','H16','H17','H18','H19','H20','H21','H22','H23','H24'])
    #dinicio = date(2021,1,1) ##Use this function the first time to update the table
    #dfin = date(2021,1,7) ###to desired date
    #dinicio = (get_most_recent_date(CARTO_LOAD_PML_TABLE) +timedelta(days=1)).date()
    #dfin =  dinicio + timedelta(days=6)    
    print('Downloading information for dates {} - {} \n'.format(yesterday,yesterday))
    links=load_zones_url(df,yesterday.strftime("%Y/%m/%d"),yesterday.strftime("%Y/%m/%d"))
    print('Downloading information for dates {} - {} \n'.format(last_week,last_week))
    links.extend(load_zones_url(df,last_week.strftime("%Y/%m/%d"),last_week.strftime("%Y/%m/%d")))
    print('Downloading information for dates {} - {} \n'.format(last_month,last_month))
    links.extend(load_zones_url(df,last_month.strftime("%Y/%m/%d"),last_month.strftime("%Y/%m/%d")))
    print('Downloading information for dates {} - {} \n'.format(last_year,last_year))
    links.extend(load_zones_url(df,last_year.strftime("%Y/%m/%d"),last_year.strftime("%Y/%m/%d")))

    for link in links:
        try:
            res = requests.get(link, timeout=None)
            res.raise_for_status()     
            noStarchSoup = bs4.BeautifulSoup(res.text, 'html.parser')
            aux_noSoup = str(str(noStarchSoup).split('</Resultados>')).split('</Zona_Carga> ')
            split_zones=bs4.BeautifulSoup(aux_noSoup[0], features="lxml").select('zona_carga')  
            scrape_sys=noStarchSoup.select('sistema')[0].getText()
            for children in split_zones:
                fecha=children.select('fecha')
                if fecha:
                    scrape_fecha=list(map(lambda x: x.getText(), children.select('fecha')))
                    scrape_zona=children.select('zona_carga')[0].getText()
                    scrape_hora = list(map(lambda x: float(x.getText()), children.select('hora')))
                    scrape_CT = list(map(lambda x: float(x.getText()), children.select('total_cargas')))
                    fecha=[]
                    [fecha.append(f) for f in scrape_fecha if f not in fecha]
                    to_db_CDM, to_db_CIM, to_db_CT={}, {}, {}
                    parsed_row = df.loc[df['load_zone'].isin([scrape_zona])]
                    zona_id =tuple(parsed_row['load_zone'])[0]
                
                    for f in fecha:
                        for i in range(1,25):
                            to_db_CT[f,zona_id, i]=0
                                                        
                        for index, f in enumerate(scrape_fecha):
                            to_db_CT[f, zona_id, scrape_hora[index]]=scrape_CT[index] 
                        
                        for index, f in enumerate(fecha):
                            dlis=[f,scrape_sys,zona_id]
                            [dlis.append(to_db_CT[f, zona_id, i]) for i in range(1,25)]
                            if not len(db_ca[(db_ca['FECHA']==f) & (db_ca['ZONA_CARGA']==zona_id)]):
                                row_frame = pd.DataFrame(dlis).T
                                lis_ca.append(row_frame)
                            else:
                                print('Information of load zone {} with date {} already exists'.format(zona_id,f))
        except Exception as exc:
            print('There was a problem: %s' % (exc))
    db_ca = db_load_mkr(lis_ca,db_ca)
    merged_tmp = pd.merge(df, db_ca, left_on=['system','load_zone'], right_on=['SISTEMA','ZONA_CARGA'], how='right')
    merged_tmp.rename(columns={'value':'load'}, inplace=True)
    merged_tmp.drop(['SISTEMA', 'ZONA_CARGA'], axis=1, inplace=True)
    merged_zones = merged_tmp.reset_index(drop=True)
    merged_zones.rename(columns={'FECHA':'entry_date'}, inplace=True)
    #Calculating percentage of total load of the system
    system_total = merged_zones.groupby('entry_date').agg(load_syst_total=('load', 'sum'))
    merged_zones = pd.merge(merged_zones, system_total, on='entry_date', how='left')
    merged_zones['load_syst_pct'] = (merged_zones.load/merged_zones.load_syst_total) * 100
        
    return merged_zones

#This function uploads new data to carto 
def upload_data(df,existing_ids,CARTO_TABLE,CARTO_SCHEMA):
    #Droping unwanted columns after merging with nodes table
    df.drop(['cartodb_id', 'uid'], axis=1, inplace=True, errors='ignore')
    # create a 'uid' column to store the index of rows as unique ids
    df = df.reset_index(drop=True)
    df['uid'] = df.index + max([int(i) for i in existing_ids],default = 0)+1
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

    return num_new

def deleteExcessRows(table, max_rows, time_field):
    ''' 
    Delete rows to bring count down to max_rows
    INPUT   table: name of table in Carto from which we will delete excess rows (string)
            max_rows: maximum rows that can be stored in the Carto table (integer)
            time_field: column that stores datetime information (string) 
    RETURN  num_dropped: number of rows that have been dropped from the table (integer)
    ''' 
    # initialize number of rows that will be dropped as 0
    num_dropped = 0
    # get cartodb_ids from carto table sorted by date (new->old)
    r = cartosql.getFields('cartodb_id', table, order='{} desc'.format(time_field),
                           f='csv', user=CARTO_USER, key=CARTO_KEY)
    # turn response into a list of strings of the ids
    ids = r.text.split('\r\n')[1:-1]

    # if number of rows is greater than max_rows, delete excess rows
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[max_rows:], CARTO_USER, CARTO_KEY)
        # get the number of rows that have been dropped from the table
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))

    return(num_dropped)


def main():
    # Check if table exists, create it if it does not
    logging.info('Checking if nodes_pml table exists and getting existing IDs.')
    nodes_pml_existing_ids = checkCreateTable(CARTO_NODES_DASH_PML_TABLE, CARTO_NODES_DASH_SCHEMA, UID_FIELD, TIME_FIELD)
    # Fetch, process, and upload new data
    logging.info('Fetching nodes pml info from cenace api!')
    new_nodes_pml = fetcher_nodes(yesterday, last_week, last_month, last_year)
    #Updating carto table with pml information
    logging.info('Uploading pml info!')
    num_new = upload_data(new_nodes_pml, nodes_pml_existing_ids,CARTO_NODES_DASH_PML_TABLE, CARTO_NODES_DASH_SCHEMA)
    logging.info('Previous rows: {},  New rows: {}'.format(len(nodes_pml_existing_ids), num_new))
     # Delete data to get back to MAX_ROWS
    logging.info('Delete Nodes pml excess Rows!')
    num_deleted = deleteExcessRows(CARTO_NODES_PML_TABLE, MAX_ROWS, TIME_FIELD)
    logging.info('Success!')
    # Check if table exists, create it if it does not
    logging.info('Checking if zones_pml table exists and getting existing IDs.')
    zones_pml_existing_ids = checkCreateTable(CARTO_LOAD_DASH_PML_TABLE, CARTO_LOAD_DASH_SCHEMA, UID_FIELD, TIME_FIELD)
    # Fetch, process, and upload new data
    logging.info('Fetching zones pml info from cenace api!')
    new_zones_pml = fetcher_load(yesterday, last_week, last_month, last_year)
    #Updating carto table with pml information
    logging.info('Uploading zones pml info!')
    num_new = upload_data(new_zones_pml, zones_pml_existing_ids,CARTO_LOAD_DASH_PML_TABLE, CARTO_LOAD_DASH_SCHEMA)
    logging.info('Previous rows: {},  New rows: {}'.format(len(zones_pml_existing_ids), num_new))
    # Delete data to get back to MAX_ROWS
    logging.info('Delete load pml excess Rows!')
    num_deleted = deleteExcessRows(CARTO_LOAD_DASH_PML_TABLE, MAX_ROWS, TIME_FIELD)
    logging.info("SUCCESS")