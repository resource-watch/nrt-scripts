from __future__ import unicode_literals

import os
import logging
import sys
import urllib
import datetime
import fiona
from collections import OrderedDict
from shapely import geometry
import cartosql
import cartoframes
from zipfile import ZipFile
import requests
import geopandas as gpd
import glob

import warnings
warnings.filterwarnings("ignore")

# name of data directory in Docker container
DATA_DIR = 'data'

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'dis_015a_hurricane_tracks'

# column that stores datetime information
TIME_FIELD = 'ISO_TIME'

# url for cyclone track data
url_a = 'https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/'
url_b = 'v04r00/access/shapefile/IBTrACS.since1980.list.v04r00.lines.zip'
SOURCE_URL = url_a + url_b

# maximum number of attempts that will be made to download the data
MAX_TRIES = 5

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = 'b82eab85-0fee-4212-8a7e-ca0b28a16a2f'

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

def create_carto_schema(gdf):
    '''
    Function to create a dictionary of column names and data types 
    in order to the upload the data to Carto
    INPUT   gdf: geodataframe for tropical cyclone data (dataframe)
    RETURN  ouput: an ordered dictionary (dictionary of strings)
    '''
    # create an empty list to store column names
    list_cols = []
    # column names and types for data table
    # column types should be one of the following: geometry, text, numeric, timestamp
    for col in list(gdf):
        # if the column type is float64 or int64, assign the column type as numeric
        if (gdf[col].dtypes  == 'float64')| (gdf[col].dtypes  == 'int64'):
            list_cols.append((col, 'numeric'))
        # if the column type is geometry, assign the column type as geometry
        elif col  == 'geometry':
            list_cols.append(('the_geom', 'geometry'))
        # for all other columns assign them as text
        else:
            list_cols.append((col, 'text'))
    # create an ordered dictionary using the list
    output = OrderedDict(list_cols)
    
    return output

def match_carto(gdf):
    '''
    Function to format geomteries to be correctly interpreted by Carto
    INPUT   gdf: geodataframe for tropical cyclone track data (dataframe)
    RETURN  gdf: geodataframe with formatted geometries (dataframe)
    '''
    # create a list from the geometry column
    geoms = list(gdf['geometry'])
    
    # create an empty list to store the formatted geometries
    val_geoms = []
    
    # loop through each geometries to reformat them
    for item in geoms:
        # create an empty dictionary to store each formatted geometries
        new_dict = {}
        # loop through each item in the dictionary
        for key, value in item.items():
            # if we are processing coordinates
            if key == 'coordinates':
                # convert list of tuples to list of lists
                res = [list(ele) for val in value for ele in val]
                # add additional dimensions to the list to match Carto
                # add the formatted list to the dictionary
                new_dict['coordinates'] = [[res]]
            else:
                # change type of geometry from Polygon to MultiPolygon
                new_dict['type'] = 'MultiPolygon'
        # add the correctly formatted geometry to the list of formatted geometries        
        val_geoms.append(new_dict)
        
    # update the values in the geometry column of the dataframe with formatted values    
    gdf['geometry'] = val_geoms
    
    return gdf

def convert_geometry(geometries):
    '''
    Function to convert shapely geometries to geojsons
    INPUT   geometries: shapely geometries (list of shapely geometries)
    RETURN  output: geojsons (list of geojsons)
    '''
    # create an empty list to store converted geojsons
    output = []
    # loop through each geometries and convert them to geojson
    for geom in geometries:
        # add converted geojsons to a list
        output.append(geom.__geo_interface__)

    return output

def fetch_data():
    '''
    Download shapefile from source url and put that into a geopandas dataframe
    RETURN  gdf: geodataframe for tropical cyclone track data (dataframe)
            tmpfile: name under which the downloaded shapefile will be saved (string)
    '''
    # create a filename for the shapefile that will be downloaded
    tmpfile = '{}.zip'.format(os.path.join(DATA_DIR,'dis_015a_hurricane_tracks-shp'))
    logging.info('Fetching shapefile')

#    try:
#     logging.info('pull data from url and save to tmpfile')
    # pull data from url and save to tmpfile
    urllib.request.urlretrieve(SOURCE_URL, tmpfile)
#     logging.info('unzip source data')
    # unzip source data
    tmpfile_unzipped = tmpfile.split('.')[0]
    zip_ref = ZipFile(tmpfile, 'r')
    zip_ref.extractall(tmpfile_unzipped)
    zip_ref.close()
#     logging.info('load in the polygon shapefile')
    # load in the polygon shapefile
    shapefile = glob.glob(os.path.join(tmpfile_unzipped, '*.shp'))[0]
    logging.info("Finding column names from shapefile using fiona")
    keys = []
    with fiona.open(shapefile, 'r') as shp:
        for obs in shp:
            for key, value in obs.items():
                if key == 'properties':
                    for k, v in obs['properties'].items():
                        keys.append(k)
                elif key == 'geometry':
                    keys.append(key)
            break
    logging.info("Reading shapefile using fiona")
    rows = []
    with fiona.open(shapefile, 'r') as shp:
        for obs in shp:
            row = []
            for field in keys:
                if field == 'geometry':
                    row.append(obs[field])
                else:
                    row.append(obs['properties'][field])
        rows.append(row)
    logging.info("length of rows = ", len(rows))
    logging.info('gpd.read_file(shapefile)')
    gdf = gpd.read_file(shapefile)
    logging.info('Find the columns where each value is null')
    # Find the columns where each value is null
    empty_cols = [col for col in gdf.columns if gdf[col].isnull().all()]
    logging.info('Drop these columns from the dataframe')
    # Drop these columns from the dataframe
    gdf.drop(empty_cols, axis=1, inplace=True)
    logging.info('there were invalid geometries (self intersection) which was preventing')        
	# there were invalid geometries (self intersection) which was preventing
    # them to be correctly interpreted by Carto. So, the lines are buffered
    # by a very small distance to fix the issue (lines are converted to polygons)
    gdf['geometry'] = gdf.geometry.buffer(0.0001)
    logging.info('convert the geometries from shapely to geojson')
    # convert the geometries from shapely to geojson
    gdf['geometry'] = convert_geometry(gdf['geometry'])
    logging.info('format geomteries to be correctly interpreted by Carto')
    # format geomteries to be correctly interpreted by Carto
    formt_gdf = match_carto(gdf)
    
    return formt_gdf, tmpfile

#    except Exception as e:
#        logging.info(e)
#        logging.info("Error fetching data")

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
        logging.info('Try retrieving cyclone track data, try number = {}'.format(tries))
        try:
            # pull cyclone track data from source url and format the data into a geopandas dataframe
            # download the shapefile from source and get the name of the file
            gdf, tmpfile = fetch_data()
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
        # generate the schema for the table that will be uploaded to Carto
        # column names and types for data table, column names should be lowercase
        # column types should be one of the following: geometry, text, numeric, timestamp
        carto_schema = create_carto_schema(gdf)
        # check if the table doesn't already exist in Carto
        if not cartosql.tableExists(CARTO_TABLE, user=CARTO_USER, key=CARTO_KEY):
            logging.info('Table {} does not exist'.format(CARTO_TABLE))
            # if the table does not exist, create it with columns based on the schema input
            cartosql.createTable(CARTO_TABLE, carto_schema)
            # Send dataframe to Carto
            logging.info('Writing to Carto')
            cc = cartoframes.CartoContext(base_url="https://{user}.carto.com/".format(user=CARTO_USER),
                                          api_key=CARTO_KEY)
            cc.write(gdf, CARTO_TABLE, overwrite=True, privacy='link')
        else:
            # if the table already exists, delete all the rows
            cartosql.deleteRows(CARTO_TABLE, 'cartodb_id IS NOT NULL', user=CARTO_USER, key=CARTO_KEY)
            # Send the processed dataframe to Carto
            logging.info('Writing to Carto')
            cc = cartoframes.CartoContext(base_url="https://{user}.carto.com/".format(user=CARTO_USER),
                                          api_key=CARTO_KEY)
            cc.write(gdf, CARTO_TABLE, overwrite=True, privacy='link')

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

