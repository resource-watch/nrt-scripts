from __future__ import unicode_literals

import os
import sys
import urllib
import datetime
import logging
import subprocess
import eeUtil
import urllib.request
import requests
from bs4 import BeautifulSoup
import copy
import numpy as np
import ee
import time
from string import ascii_uppercase
import json

# url for historical air quality data
SOURCE_URL_HISTORICAL = 'https://portal.nccs.nasa.gov/datashare/gmao/geos-cf/v1/das/Y{year}/M{month}/D{day}/GEOS-CF.v01.rpl.chm_tavg_1hr_g1440x721_v1.{year}{month}{day}_{time}z.nc4'

# url for forecast air quality data
SOURCE_URL_FORECAST = 'https://portal.nccs.nasa.gov/datashare/gmao/geos-cf/v1/forecast/Y{start_year}/M{start_month}/D{start_day}/H12/GEOS-CF.v01.fcst.chm_tavg_1hr_g1440x721_v1.{start_year}{start_month}{start_day}_12z+{year}{month}{day}_{time}z.nc4'

# subdataset to be converted to tif
# should be of the format 'NETCDF:"filename.nc":variable'
SDS_NAME = 'NETCDF:"{fname}":{var}'

# list variables (as named in netcdf) that we want to pull
VARS = ['NO2', 'O3', 'PM25_RH35_GCC']

# define unit conversion factors for each compound
CONVERSION_FACTORS = {
    # mol/mol to ppb
    'NO2': 1e9, # mol/mol to ppb
    'O3': 1e9,
    # keep original units
    'PM25_RH35_GCC': 1, 
}

# define metrics to calculate for each compound
METRIC_BY_COMPOUND = {
    'NO2': 'daily_avg',
    'O3': 'daily_max',
    'PM25_RH35_GCC': 'daily_avg',
}

# nodata value for netcdf
NODATA_VALUE = 9.9999999E14

# name of data directory in Docker container
DATA_DIR = 'data'

# name of collection in GEE where we will upload the final data
COLLECTION = '/projects/resource-watch-gee/cit_002_gmao_air_quality'

# do you want to delete everything currently in the GEE collection when you run this script?
CLEAR_COLLECTION_FIRST = False

# do you want to delete local tif and netcdf files?
DELETE_LOCAL = True

#how many assets can be stored in the GEE collection before the oldest ones are deleted?
MAX_ASSETS = 100

# date format to use in GEE
DATE_FORMAT = '%Y-%m-%d'

# Resource Watch dataset API IDs
# Important! Before testing this script:
# Please change these IDs OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on different datasets on Resource Watch
DATASET_IDS = {
    'NO2':'ecce902d-a322-4d13-a3d6-e1a36fc5573e',
    'O3':'ebc079a1-51d8-4622-ba25-d8f3b4fcf8b3',
    'PM25_RH35_GCC':'645fe192-28db-4949-95b9-79d898f4226b',
}

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
    apiUrl = 'http://api.resourcewatch.org/v1/dataset/{0}'.format(dataset)
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
FUNCTIONS FOR RASTER DATASETS

The functions below must go in every near real-time script for a RASTER dataset.
Their format should not need to be changed.
'''

def getLastUpdate(dataset):
    '''
    Given a Resource Watch dataset's API ID,
    this function will get the current 'last update date' from the API
    and return it as a datetime
    INPUT   dataset: Resource Watch API dataset ID (string)
    RETURN  lastUpdateDT: current 'last update date' for the input dataset (datetime)
    '''
    # generate the API url for this dataset
    apiUrl = 'http://api.resourcewatch.org/v1/dataset/{}'.format(dataset)
    # pull the dataset from the API
    r = requests.get(apiUrl)
    # find the 'last update date'
    lastUpdateString=r.json()['data']['attributes']['dataLastUpdated']
    # split this date into two pieces at the seconds decimal so that the datetime module can read it:
    # ex: '2020-03-11T00:00:00.000Z' will become '2020-03-11T00:00:00' (nofrag) and '000Z' (frag)
    nofrag, frag = lastUpdateString.split('.')
    # generate a datetime object
    nofrag_dt = datetime.datetime.strptime(nofrag, "%Y-%m-%dT%H:%M:%S")
    # add back the microseconds to the datetime
    lastUpdateDT = nofrag_dt.replace(microsecond=int(frag[:-1])*1000)
    return lastUpdateDT

def getLayerIDs(dataset):
    '''
    Given a Resource Watch dataset's API ID,
    this function will return a list of all the layer IDs associated with it
    INPUT   dataset: Resource Watch API dataset ID (string)
    RETURN  layerIDs: Resource Watch API layer IDs for the input dataset (list of strings)
    '''
    # generate the API url for this dataset - this must include the layers
    apiUrl = 'http://api.resourcewatch.org/v1/dataset/{}?includes=layer'.format(dataset)
    # pull the dataset from the API
    r = requests.get(apiUrl)
    #get a list of all the layers
    layers = r.json()['data']['attributes']['layer']
    # create an empty list to store the layer IDs
    layerIDs =[]
    # go through each layer and add its ID to the list
    for layer in layers:
        # only add layers that have Resource Watch listed as its application
        if layer['attributes']['application']==['rw']:
            layerIDs.append(layer['id'])
    return layerIDs

def flushTileCache(layer_id):
    """
    Given the API ID for a GEE layer on Resource Watch,
    this function will clear the layer cache.
    If the cache is not cleared, when you view the dataset on Resource Watch, old and new tiles will be mixed together.
    INPUT   layer_id: Resource Watch API layer ID (string)
    """
    # generate the API url for this layer's cache
    apiUrl = 'http://api.resourcewatch.org/v1/layer/{}/expire-cache'.format(layer_id)
    # create headers to send with the request to clear the cache
    headers = {
    'Content-Type': 'application/json',
    'Authorization': os.getenv('apiToken')
    }

    # clear the cache for the layer
    # sometimetimes this fails, so we will try multiple times, if it does

    # specify that we are on the first try
    try_num=1
    while try_num<4:
        try:
            # try to delete the cache
            r = requests.delete(url = apiUrl, headers = headers, timeout=1000)
            # if we get a 200, the cache has been deleted
            # if we get a 504 (gateway timeout) - the tiles are still being deleted, but it worked
            if r.ok or r.status_code==504:
                logging.info('[Cache tiles deleted] for {}: status code {}'.format(layer_id, r.status_code))
                return r.status_code
            # if we don't get a 200 or 504:
            else:
                # if we are not on our last try, wait 60 seconds and try to clear the cache again
                if try_num < (tries-1):
                    logging.info('Cache failed to flush: status code {}'.format(r.status_code))
                    time.sleep(60)
                    logging.info('Trying again.')
                # if we are on our last try, log that the cache flush failed
                else:
                    logging.error('Cache failed to flush: status code {}'.format(r.status_code))
                    logging.error('Aborting.')
            try_num += 1
        except Exception as e:
            logging.error('Failed: {}'.format(e))

'''
FUNCTIONS FOR THIS DATASET

The functions below have been tailored to this specific dataset.
They should all be checked because their format likely will need to be changed.
'''

def getAssetName(date):
    '''
    get asset name
    INPUT   date: date in the format of the DATE_FORMAT variable (string)
    RETURN  GEE asset name for input date (string)
    '''
    return os.path.join(EE_COLLECTION, FILENAME.format(metric=METRIC_BY_COMPOUND[VAR], var=VAR, date=date))

def getTiffname(file, variable):
    '''
    generate names for tif files that we are going to create from netcdf
    INPUT   file: netcdf filename (string)
            variable: variable that we want to pull (string)
    RETURN  file name to save tif file created from netcdf (string)
    '''
    # get year, month, day and time from netcdf filename 
    year = file.split('/')[1][-18:-14]
    month = file.split('/')[1][-14:-12]
    day = file.split('/')[1][-12:-10]
    time = file.split('/')[1][-9:-5]

    name = os.path.join(DATA_DIR, FILENAME.format(metric=METRIC_BY_COMPOUND[VAR], var=variable, date=year+'-'+month+'-'+day +'_'+time))+'.tif'
    return name

def getDateTime(filename):
    '''
    get date from filename (last 10 characters of filename after removing extension)
    INPUT   filename: file name that ends in a date of the format YYYY-MM-DD (string)
    RETURN  date in the format YYYY-MM-DD (string)
    '''
    return os.path.splitext(os.path.basename(filename))[0][-10:]

def getDate_GEE(filename):
    '''
    get date from Google Earth Engine asset name (last 10 characters of filename after removing extension)
    INPUT   filename: asset name that ends in a date of the format YYYY-MM-DD (string)
    RETURN  date in the format YYYY-MM-DD (string)
    '''
    return os.path.splitext(os.path.basename(filename))[0][-10:]

def list_available_files(url, file_start=''):
    '''
    get the files available for a given day using a source url formatted with date
    INPUT   url: source url for the given day's data folder (string)
            file_start: a string that is present in the begining of every source netcdf filename for this data (string)
    RETURN  list of files available for the given url (list of strings)
    '''
    # open and read the url
    page = requests.get(url).text
    # use BeautifulSoup to read the content as a nested data structure
    soup = BeautifulSoup(page, 'html.parser')
    # Extract all the <a> tags within the html content to find the files available for download marked with these tags.
    # Get only the files that starts with a certain word present in the begining of every source netcdf filename
    return [node.get('href') for node in soup.find_all('a') if type(node.get('href'))==str and node.get('href').startswith(file_start)]

def getNewDatesHistorical(existing_dates):
    '''
    Get new dates we want to try to fetch historical data for
    INPUT   existing_dates: list of dates that we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
    RETURN  new_dates: list of new dates we want to try to get, in the format of the DATE_FORMAT variable (list of strings)
    '''
    #create empty list to store dates we should process
    new_dates = []

    # start with today's date and time
    date = datetime.datetime.utcnow()
    # generate date string in same format used in GEE collection
    date_str = datetime.datetime.strftime(date, DATE_FORMAT)

    # find date beyond which we don't want to go back since that will exceed the maximum allowable assets in GEE
    last_date = date - datetime.timedelta(days=MAX_ASSETS)

    # if the date string is not in our list of existing dates and don't go beyond max allowable dates:
    while (date_str not in existing_dates) and (date!=last_date):
        # general source url for the given dates data folder
        url = SOURCE_URL_HISTORICAL.split('/GEOS')[0].format(year=date.year, month='{:02d}'.format(date.month), day='{:02d}'.format(date.day))
        # get the list of files available for the given date
        files = list_available_files(url, file_start='GEOS-CF.v01.rpl.chm_tavg')
        # if the first 12 hourly files are available for a day, we can process this data - add it to the list
        # note: we are centering the averages about midnight each day, so we just need 12 hours from the most recent day and 12 hours from the previous day
        if len(files) >= 12:
            new_dates.append(date_str)
        # go back one more day
        date = date - datetime.timedelta(days=1)
        # generate new string in same format used in GEE collection
        date_str = datetime.datetime.strftime(date, DATE_FORMAT)
    #repeat until we reach something in our existing dates

    #reverse order so we pull oldest date first
    new_dates.reverse()
    return new_dates

def getNewDatesForecast(existing_dates):
    '''
    Get new dates we want to try to fetch forecasted data for
    INPUT   existing_dates: list of dates that we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
    RETURN  new_dates: list of new dates we want to try to get, in the format of the DATE_FORMAT variable (list of strings)
    '''
    if existing_dates:
        # get start date of last forecast
        first_date_str = existing_dates[0]
        # convert date string to datetime object
        existing_start_date = datetime.datetime.strptime(first_date_str, DATE_FORMAT)
    else:
        # if we don't have existing data, just choose an old date so that we keep checking back until that date
        # let's assume we will probably have a forecast in the last 30 days, so we will check back that far for
        # forecasts until we find one
        existing_start_date = datetime.datetime.utcnow() - datetime.timedelta(days=30)
    #create empty list to store dates we should process
    new_dates = []

    # start with today's date and time
    date = datetime.datetime.utcnow()
    # while the date is newer than the most recent forecast that we pulled:
    while date > existing_start_date:
        # general source url for this day's forecast data folder
        url = SOURCE_URL_FORECAST.split('/GEOS')[0].format(start_year=date.year, start_month='{:02d}'.format(date.month), start_day='{:02d}'.format(date.day))
        # check the files available for this day:
        files = list_available_files(url, file_start='GEOS-CF.v01.fcst.chm_tavg')
        # if all 120 files are available (5 days x 24 hours/day), we can process this data
        if len(files) == 120:
            #add the next five days forecast to the new dates
            for i in range(5):
                date = date + datetime.timedelta(days=1)
                # generate a string from the date
                date_str = datetime.datetime.strftime(date, DATE_FORMAT)
                new_dates.append(date_str)
            # once we have found the most recent forecast we can break from the while loop because we only want to process the most recent forecast
            break
        # if there was no forecast for this day, go back one more day
        date = date - datetime.timedelta(days=1)
    # repeat until we reach the forecast we already have

    return new_dates

def convert(files):
    '''
    Convert netcdf files to tifs
    INPUT   files: list of file names for netcdfs that have already been downloaded (list of strings)
    RETURN  tifs: list of file names for tifs that have been generated (list of strings)
    '''
    # create an empty list to store the names of tif files that we create
    tifs = []
    for f in files:
        logging.info('Converting {} to tiff'.format(f))
        # generate the subdatset name for current netcdf file for a particular variable
        sds_path = SDS_NAME.format(fname=f, var=VAR)
        # only one band available in each file, so we will pull band 1
        band = 1
        # generate a name to save the tif file we will translate the netcdf file into
        tif = getTiffname(file=f, variable=VAR)
        # tranlate the netcdf into a tif
        cmd = ['gdal_translate', '-b', str(band), '-q', '-a_nodata', str(NODATA_VALUE), '-a_srs', 'EPSG:4326', sds_path, tif]
        # add the new tif files to the list of tifs
        tifs.append(tif)

    return tifs

def fetch(new_dates, unformatted_source_url, period):
    '''
    Fetch files by datestamp
    INPUT   new_dates: list of dates we want to try to fetch, in the format YYYY-MM-DD (list of strings)
            unformatted_source_url: url for air quality data (string)
            period: time period for which we want to get the data, either historical or forecast (string)
    RETURN  files: list of file names for netcdfs that have been downloaded (list of strings)
            files_by_date: dictionary of file names along with the date for which they were downloaded (dictionary of strings)
    '''
    # make an empty list to store names of the files we downloaded
    files = []
    # create a list of hours to pull (24 hours per day, on the half-hour)
    # starts after noon on previous day through noon of current day
    hours = ['1230', '1330', '1430', '1530', '1630', '1730', '1830', '1930', '2030', '2130', '2230', '2330', '0030', '0130', '0230', '0330', '0430', '0530', '0630', '0730', '0830', '0930', '1030', '1130']
    # create an empty dictionary to store downloaded file names as value and corresponding dates as key 
    files_by_date = {}
    # Loop over all hours of the new dates, check if there is data available, and download netcdfs
    for date in new_dates:
        # make an empty list to store names of the files we downloaded
        # this list will be used to insert values to the "files_by_date" dictionary
        files_for_current_date = []
        # convert date string to datetime object and go back one day
        first_date = datetime.datetime.strptime(new_dates[0], DATE_FORMAT) - datetime.timedelta(days=1)
        # generate a string from the datetime object
        first_date = datetime.datetime.strftime(first_date, DATE_FORMAT)
        # loop through each hours we want to pull data for
        for hour in hours:
            # for the first half of the hours, get data from previous day
            if hours.index(hour) < 12:
                # convert date string to datetime object and go back one day
                prev_date = datetime.datetime.strptime(date, DATE_FORMAT) - datetime.timedelta(days=1)
                # generate a string from the datetime object
                fetching_date = datetime.datetime.strftime(prev_date, DATE_FORMAT)
            # for the second half, use the current day
            else:
                fetching_date = date
            # Set up the url of the filename to download historical data
            if period=='historical':
                url = unformatted_source_url.format(year=int(fetching_date[:4]), month='{:02d}'.format(int(fetching_date[5:7])), day='{:02d}'.format(int(fetching_date[8:])), time=hour)
            # Set up the url of the filename to download forecast data
            elif period=='forecast':
                url = unformatted_source_url.format(start_year=int(first_date[:4]), start_month='{:02d}'.format(int(first_date[5:7])), start_day='{:02d}'.format(int(first_date[8:])),year=int(fetching_date[:4]), month='{:02d}'.format(int(fetching_date[5:7])), day='{:02d}'.format(int(fetching_date[8:])), time=hour)
            # Create a file name to store the netcdf in after download
            f = DATA_DIR+'/'+url.split('/')[-1]
            # try to download the data
            tries = 0
            while tries <3:
                try:
                    logging.info('Retrieving {}'.format(f))
                    # download files from url and put in specified file location (f)
                    urllib.request.urlretrieve(url, f)
                    # if successful, add the file to the list of files we have downloaded
                    files.append(f)
                    files_for_current_date.append(f)
                    break
                # if unsuccessful, log that the file was not downloaded
                except Exception as e:
                    logging.info('Unable to retrieve data from {}'.format(url))
                    logging.info(e)
                    tries+=1
                    logging.info('try {}'.format(tries))
            if tries==3:
                logging.error('Unable to retrieve data from {}'.format(url))
                exit()

        # populate dictionary of file names along with the date for which they were downloaded        
        files_by_date[date]=files_for_current_date

    return files, files_by_date

def daily_avg(date, tifs_for_date):
    '''
    Calculate a daily average tif file from all the hourly tif files
    INPUT   date: list of dates we want to try to fetch, in the format YYYY-MM-DD (list of strings)
            tifs_for_date: list of file names for tifs that were created from downloaded netcdfs (list of strings)
    RETURN  result_tif: file name for tif file created after averaging all the input tifs (string)
    '''
    # create a list to store the tifs and variable names to be used in gdal_calc
    gdal_tif_list=[]
    # set up calc input for gdal_calc
    calc = '--calc="('
    # go through each hour in the day to be averaged
    for i in range(len(tifs_for_date)):
        # generate a letter variable for that tif to use in gdal_calc (A, B, C...)
        letter = ascii_uppercase[i]
        # add each letter to the list to be used in gdal_calc
        gdal_tif_list.append('-'+letter)
        # pull the tif name 
        tif = tifs_for_date[i]
        # add each tif name to the list to be used in gdal_calc
        gdal_tif_list.append('"'+tif+'"')
        # add the variable to the calc input for gdal_calc
        if i==0:
            # for first tif, it will be like: --calc="(A
            calc= calc +letter   
        else:
            # for second tif and onwards, keep adding each letter like: --calc="(A+B
            calc = calc+'+'+letter
    # calculate the number of tifs we are averaging 
    num_tifs = len(tifs_for_date)
    # finish creating calc input
    # since we are trying to find average, the algorithm is: (sum all tifs/number of tifs)*(conversion factor for corresponding variable)
    calc= calc + ')*{}/{}"'.format(CONVERSION_FACTORS[VAR], num_tifs)
    # generate a file name for the daily average tif
    result_tif = DATA_DIR+'/'+FILENAME.format(metric=METRIC_BY_COMPOUND[VAR], var=VAR, date=date)+'.tif'
    # create the gdal command to calculate the average by putting it all together
    cmd = ('gdal_calc.py {} --outfile="{}" {}').format(' '.join(gdal_tif_list), result_tif, calc)
    # using gdal from command line from inside python
    subprocess.check_output(cmd, shell=True)
    return result_tif

def daily_max(date, tifs_for_date):
    '''
    Calculate a daily maximum tif file from all the hourly tif files
    INPUT   date: list of dates we want to try to fetch, in the format YYYY-MM-DD (list of strings)
            tifs_for_date: list of file names for tifs that were created from downloaded netcdfs (list of strings)
    RETURN  result_tif: file name for tif file created after finding the max from all the input tifs (string)
    '''
    # create a list to store the tifs and variable names to be used in gdal_calc
    gdal_tif_list=[]

    # go through each hour in the day to find the maximum
    for i in range(len(tifs_for_date)):
        # generate a letter variable for that tif to use in gdal_calc
        letter = ascii_uppercase[i]
        # add each letter to the list of tifs to be used in gdal_calc
        gdal_tif_list.append('-'+letter)
        # pull the tif name
        tif = tifs_for_date[i]
        # add each tif name to the list to be used in gdal_calc
        gdal_tif_list.append('"'+tif+'"')
        #add the variable to the calc input for gdal_calc
        if i==0:
            calc= letter
        else:
            # set up calc input for gdal_calc to find the maximum from all tifs
            calc = 'maximum('+calc+','+letter+')'
    # finish creating calc input
    calc= '--calc="'+calc + '*{}"'.format(CONVERSION_FACTORS[VAR])
    #generate a file name for the daily maximum tif
    result_tif = DATA_DIR+'/'+FILENAME.format(metric=METRIC_BY_COMPOUND[VAR], var=VAR, date=date)+'.tif'
    # create the gdal command to calculate the maximum by putting it all together
    cmd = ('gdal_calc.py {} --outfile="{}" {}').format(' '.join(gdal_tif_list), result_tif, calc)
    # using gdal from command line from inside python
    subprocess.check_output(cmd, shell=True)
    return result_tif

def processNewData(all_files, files_by_date, period, assets_to_delete):
    '''
    Process and upload clean new data
    INPUT   all_files: list of file names for netcdfs that have been downloaded (list of strings)
            files_by_date: dictionary of netcdf file names along with the date for which they were downloaded (dictionary of strings)
            period: time period for which we want to process the data, either historical or forecast (string)
            assets_to_delete: list of old assets to delete (list of strings)
    RETURN  assets: list of file names for netcdfs that have been downloaded (list of strings)
    '''
    # if files is empty list do nothing, otherwise, process data
    if all_files: 
        # create an empty list to store the names of the tifs we generate
        tifs = []
        # create an empty list to store the names we want to use for the GEE assets
        assets=[]
        # create an empty list to store the list of dates from the averaged or maximum tifs
        dates = []
        # create an empty list to store the list of datetime objects from the averaged or maximum tifs
        datestamps = []
        # loop over each downloaded netcdf file
        for date, files in files_by_date.items():
            logging.info('Converting files')
            # Convert new files from netcdf to tif files
            hourly_tifs = convert(files)
            # take relevant metric (daily average or maximum) of hourly tif files for days we have pulled
            metric = METRIC_BY_COMPOUND[VAR]
            tif = globals()[metric](date, hourly_tifs)
            # add the averaged or maximum tif file to the list of files to upload to GEE
            tifs.append(tif)
            # Get a list of the names we want to use for the assets once we upload the files to GEE
            assets.append(getAssetName(date))
            # get new list of dates (in case order is different) from the averaged or maximum tifs
            dates.append(getDateTime(tif))
            # generate datetime objects for each data
            datestamps.append(datetime.datetime.strptime(date, DATE_FORMAT))
        # delete old assets (none for historical)
        for asset in assets_to_delete:
            ee.data.deleteAsset(asset)
            logging.info(f'Deleteing {asset}')

        logging.info('Uploading files:')
        for asset in assets:
            logging.info(os.path.split(asset)[1])
        # Upload new files (tifs) to GEE
        eeUtil.uploadAssets(tifs, assets, GS_FOLDER, datestamps, timeout=3000)
        return assets
    #if no new assets, return empty list
    else:
        return []

def checkCreateCollection(VARS):
    '''
    List assests in collection if it exists, else create new collection
    INPUT   VARS: list variables (as named in netcdf) that we want to pull (list of strings)
    RETURN  existing_dates_all_vars: list of dates that exist for all variables collections in GEE (list of strings)
            existing_dates_by_var: list of dates that exist for each individual variable collection in GEE (list of strings)
    '''
    # create a master list (not variable-specific) of which dates we already have data for
    existing_dates = []
    # create an empty list to store the dates that we currently have for each AQ variable
    # will be used in case the previous script run crashed before completing the data upload for every variable.
    existing_dates_by_var = []
    # loop through each variables that we want to pull
    for VAR in VARS:
        # For one of the variables, get the date of the most recent data set
        # All variables come from the same file
        # If we have one for a particular data, we should have them all
        collection = EE_COLLECTION_GEN.format(metric=METRIC_BY_COMPOUND[VAR], var=VAR)

        # Check if folder to store GEE collections exists. If not, create it.
        # we will make one collection per variable, all stored in the parent folder for the dataset
        parent_folder = PARENT_FOLDER.format(metric=METRIC_BY_COMPOUND[VAR], var=VAR)
        if not eeUtil.exists(parent_folder):
            logging.info('{} does not exist, creating'.format(parent_folder))
            eeUtil.createFolder(parent_folder)

        # If the GEE collection for a particular variable exists, get a list of existing assets
        if eeUtil.exists(collection):
            existing_assets = eeUtil.ls(collection)
            # get a list of the dates from these existing assets
            dates = [getDate_GEE(a) for a in existing_assets]
            # append this list of dates to our list of dates by variable
            existing_dates_by_var.append(dates)

            # for each of the dates that we have for this variable, append the date to the master list
            # list of which dates we already have data for (if it isn't already in the list)
            for date in dates:
                if date not in existing_dates:
                    existing_dates.append(date)
        #If the GEE collection does not exist, append an empty list to our list of dates by variable
        else:
            existing_dates_by_var.append([])
            # create a collection for this variable
            logging.info('{} does not exist, creating'.format(collection))
            eeUtil.createFolder(collection, True)

    '''
     We want make sure all variables correctly uploaded the data on the last run. To do this, we will
     check that we have the correct number of appearances of the data in our GEE collection. If we do
     not, we will want to re-upload this date's data.
    '''
    # Create a copy of the master list of dates that will store the dates that were properly uploaded for all variables.
    existing_dates_all_vars = copy.copy(existing_dates)
    for date in existing_dates:
        #check how many times each date appears in our list of dates by variable
        date_count = sum(x.count(date) for x in existing_dates_by_var)
        # If this count is less than the number of variables we have, one of the variables did not finish
        # upload for this date, and we need to re-upload this file.
        if date_count < len(VARS):
            #remove this from the list of existing dates for all variables
            existing_dates_all_vars.remove(date)
    return existing_dates_all_vars, existing_dates_by_var

def deleteExcessAssets(all_assets, max_assets):
    '''
    Delete oldest assets, if more than specified in max_assets variable
    INPUT   all_assets: list of all the assets currently in the GEE collection (list of strings)
            max_assets: maximum number of assets allowed in the collection (int)
    '''
    # if we have more assets than allowed,
    if len(all_assets) > max_assets:
        # sort the list of dates so that the oldest is first
        all_assets.sort()
        logging.info('Deleting excess assets.')
        # go through each assets, starting with the oldest, and delete until we only have the max number of assets left
        for asset in all_assets[:-max_assets]:
            eeUtil.removeAsset(EE_COLLECTION +'/'+ asset)

def get_most_recent_date(all_assets):
    '''
    Get most recent data we have assets for
    INPUT   all_assets: list of all the assets currently in the GEE collection (list of strings)
    RETURN  most_recent_date: most recent date in GEE collection (datetime)
    '''
    # sort these dates oldest to newest
    all_assets.sort()
    # get the most recent date (last in the list) and turn it into a datetime
    most_recent_date = datetime.datetime.strptime(all_assets[-1][-10:], DATE_FORMAT)
    return most_recent_date

# ATTENTION AMELIA AMELIA AMELIA ATTENTION !!!
# NOT SURE IF WE NEED THIS FUNCTION, COULDN'T FIND ANY PLACE WHERE IT'S USED
def get_forecast_run_date(all_assets):
    all_assets.sort()
    most_recent_date = datetime.datetime.strptime(all_assets[0][-10:], DATE_FORMAT)
    return most_recent_date

def clearCollection():
    '''
    Clear the GEE collection
    '''
    logging.info('Clearing collections.')
    for var_num in range(len(VARS)):
        var = VARS[var_num]
        collection = EE_COLLECTION_GEN.format(metric=METRIC_BY_COMPOUND[var], var=var)
        if eeUtil.exists(collection):
            if collection[0] == '/':
                collection = collection[1:]
            a = ee.ImageCollection(collection)
            collection_size = a.size().getInfo()
            if collection_size > 0:
                list = a.toList(collection_size)
                for item in list.getInfo():
                    ee.data.deleteAsset(item['id'])

def listAllCollections():
    '''
    Get list of old assets to delete (all currently in collection)
    RETURN  all_assets: list of old assets to delete (list of strings)
    '''
    all_assets = []
    collection = EE_COLLECTION_GEN.format(metric=METRIC_BY_COMPOUND[VAR], var=VAR)
    if eeUtil.exists(collection):
        if collection[0] == '/':
            collection = collection[1:]
        a = ee.ImageCollection(collection)
        collection_size = a.size().getInfo()
        if collection_size > 0:
            list = a.toList(collection_size)
            for item in list.getInfo():
                all_assets.append(item['id'])
    return all_assets

def initialize_ee():
    '''
    Initialize eeUtil and ee modules
    '''
    # get GEE credentials from env file 
    GEE_JSON = os.environ.get("GEE_JSON")
    _CREDENTIAL_FILE = 'credentials.json'
    GEE_SERVICE_ACCOUNT = os.environ.get("GEE_SERVICE_ACCOUNT")
    with open(_CREDENTIAL_FILE, 'w') as f:
        f.write(GEE_JSON)
    auth = ee.ServiceAccountCredentials(GEE_SERVICE_ACCOUNT, _CREDENTIAL_FILE)
    ee.Initialize(auth)

def create_headers():
    '''
    Create headers when we overwrite layers on API
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
    rw_api_url = 'https://api.resourcewatch.org/v1/dataset/{}/layer'.format(dataset_id)
    # request data
    r = requests.get(rw_api_url)
    # convert response into json and make dictionary of layers
    layer_dict = json.loads(r.content.decode('utf-8'))['data']
    return layer_dict

def update_layer(layer, new_date):
    '''
    Update layers in Resource Watch back office.
    INPUT   layer: layer that will be updated (string)
            new_date: date of most recent asset added for the input layer (string)
    '''
    # get name of asset - drop first / in string or asset won't be pulled into RW
    asset = getAssetName(new_date)[1:]

    # get previous date being used from
    old_date = getDate_GEE(layer['attributes']['layerConfig']['assetId'])
    # convert to datetime
    old_date_dt = datetime.datetime.strptime(old_date, DATE_FORMAT)
    # change to layer name text of date
    old_date_text = old_date_dt.strftime("%B %-d, %Y")

    # get text for new date
    new_date_dt = datetime.datetime.strptime(new_date, DATE_FORMAT)
    new_date_text = new_date_dt.strftime("%B %-d, %Y")

    # replace date in layer's title with new date
    layer['attributes']['name'] = layer['attributes']['name'].replace(old_date_text, new_date_text)

    # replace the asset id in the layer def with new asset id
    layer['attributes']['layerConfig']['assetId'] = asset

    # replace the asset id in the interaction config with new asset id
    old_asset = getAssetName(old_date)[1:]
    layer['attributes']['interactionConfig']['config']['url'] = layer['attributes']['interactionConfig']['config']['url'].replace(old_asset,asset)

    # send patch to API to replace layers
    # generate url to patch layer
    rw_api_url_layer = "https://api.resourcewatch.org/v1/dataset/{dataset_id}/layer/{layer_id}".format(
        dataset_id=layer['attributes']['dataset'], layer_id=layer['id'])
    # create payload with new title and layer configuration
    payload = {
        'application': ['rw'],
        'layerConfig': layer['attributes']['layerConfig'],
        'name': layer['attributes']['name'],
        'interactionConfig': layer['attributes']['interactionConfig']
    }
    # patch API with updates
    r = requests.request('PATCH', rw_api_url_layer, data=json.dumps(payload), headers=create_headers())
    # check response
    if r.ok:
        logging.info('Layer replaced: {}'.format(layer['id']))
    else:
        logging.error('Error replacing layer: {} ({})'.format(layer['id'], r.status_code))

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    #create global variables that will be used in many functions
    global VAR
    global EE_COLLECTION
    global EE_COLLECTION_GEN
    global PARENT_FOLDER
    global FILENAME
    global GS_FOLDER

    # Initialize eeUtil and ee modules
    eeUtil.initJson()
    initialize_ee()

    '''
    Process Historical Data
    '''
    logging.info('Starting Historical Data Processing')
    # generate name for dataset's parent folder on GEE which will be used to store
    # several collections - one collection per variable
    PARENT_FOLDER = COLLECTION+'_historical_{metric}'
    # generate generic string that can be formatted to name each variable's GEE collection
    EE_COLLECTION_GEN = PARENT_FOLDER + '/{var}'
    # generate generic string that can be formatted to name each variable's asset name
    FILENAME = PARENT_FOLDER.split('/')[-1]+'_{var}_{date}'

    # Clear collection in GEE if desired
    if CLEAR_COLLECTION_FIRST:
        clearCollection()

    # Check if collection exists. If not, create it.
    # Return a list of dates that exist for all variables collections in GEE (existing_dates),
    # as well as a list of which dates exist for each individual variable (existing_dates_by_var).
    # The latter will be used in case the previous script run crashed before completing the data upload for every variable.
    logging.info('Getting existing dates.')
    existing_dates, existing_dates_by_var = checkCreateCollection(VARS)

    # Get a list of the dates that are available, minus the ones we have already uploaded correctly for all variables.
    logging.info('Getting new dates to pull.')
    new_dates_historical = getNewDatesHistorical(existing_dates)

    # Fetch new files
    logging.info('Fetching files for {}'.format(new_dates_historical))
    # Download files and get list of locations of netcdfs in docker container
    files, files_by_date = fetch(new_dates_historical, SOURCE_URL_HISTORICAL, period='historical')
    for var_num in range(len(VARS)):
        logging.info('Processing {}'.format(VARS[var_num]))
        # get variable name
        VAR = VARS[var_num]
        # specify GEE collection name
        EE_COLLECTION=EE_COLLECTION_GEN.format(metric=METRIC_BY_COMPOUND[VAR], var=VAR)
        # specify Google Cloud Storage folder name
        GS_FOLDER=COLLECTION[1:]+'_'+VAR

        # Get list of old assets to delete (none for historical)
        assets_to_delete = []

        # Process new data files
        new_assets_historical = processNewData(files, files_by_date, period='historical', assets_to_delete=assets_to_delete)

        # get list of all dates we now have data for by combining existing dates with new dates
        all_dates = existing_dates_by_var[var_num] + new_dates_historical
        # get list of existing assets in current variable's GEE collection
        existing_assets = eeUtil.ls(EE_COLLECTION)
        # make list of all assets by combining existing assets with new assets
        all_assets_historical = np.sort(np.unique(existing_assets + [os.path.split(asset)[1] for asset in new_assets_historical]))

        logging.info('Existing assets for {}: {}, new: {}, max: {}'.format(
            VAR, len(all_dates), len(new_dates_historical), MAX_ASSETS))
        # Delete extra assets, past our maximum number allowed that we have set
        deleteExcessAssets(all_assets_historical, MAX_ASSETS)
        logging.info('SUCCESS for {}'.format(VAR))

        # Delete local tif files because we will run out of space
        if DELETE_LOCAL:
            try:
                files_available = os.listdir(DATA_DIR)
                for f in files_available:
                    if f.endswith(".tif"):
                        logging.info('Removing {}'.format(f))
                        os.remove(DATA_DIR + '/' + f)
            except NameError:
                logging.info('No local tiff files to clean.')

    # Delete local netcdf files
    if DELETE_LOCAL:
        try:
            for f in os.listdir(DATA_DIR):
                logging.info('Removing {}'.format(f))
                os.remove(DATA_DIR+'/'+f)
        except NameError:
            logging.info('No local files to clean.')
    '''
    Process Forecast Data
    '''
    logging.info('Starting Forecast Data Processing')
    # generate name for dataset's parent folder on GEE which will be used to store
    # several collections - one collection per variable
    PARENT_FOLDER = COLLECTION+'_forecast_{metric}'
    # generate generic string that can be formatted to name each variable's GEE collection
    EE_COLLECTION_GEN = PARENT_FOLDER + '/{var}'
    # generate generic string that can be formatted to name each variable's asset name
    FILENAME = PARENT_FOLDER.split('/')[-1]+'_{var}_{date}'

    # Clear collection in GEE if desired
    if CLEAR_COLLECTION_FIRST:
        clearCollection()

    # Check if collection exists. If not, create it.
    # Return a list of dates that exist for all variables collections in GEE (existing_dates),
    # as well as a list of which dates exist for each individual variable (existing_dates_by_var).
    # The latter will be used in case the previous script run crashed before completing the data upload for every variable.
    logging.info('Getting existing dates.')
    existing_dates, existing_dates_by_var = checkCreateCollection(VARS)

    # Get a list of the dates that are available, minus the ones we have already uploaded correctly for all variables.
    logging.info('Getting new dates to pull.')
    new_dates_forecast = getNewDatesForecast(existing_dates)

    # Fetch new files
    logging.info('Fetching files for {}'.format(new_dates_forecast))
    # Download files and get list of locations of netcdfs in docker container
    files, files_by_date = fetch(new_dates_forecast, SOURCE_URL_FORECAST, period='forecast')

    # Check if there are new forecast data available
    if new_dates_forecast:
        logging.info('New forecast available')

    # go through each air quality variables
    for var_num in range(len(VARS)):
        logging.info('Processing {}'.format(VARS[var_num]))
        # get variable name
        VAR = VARS[var_num]
        # specify GEE collection name
        EE_COLLECTION=EE_COLLECTION_GEN.format(metric=METRIC_BY_COMPOUND[VAR], var=VAR)
        # specify Google Cloud Storage folder name
        GS_FOLDER=COLLECTION[1:]+'_'+VAR

        # Get list of old assets to delete (all currently in collection)
        assets_to_delete = listAllCollections()

        # Process new data files
        new_assets_forecast = processNewData(files, files_by_date, period='forecast', assets_to_delete=assets_to_delete)

        # get list of existing assets in current variable's GEE collection
        existing_assets = eeUtil.ls(EE_COLLECTION)
        # make list of all assets by combining existing assets with new assets
        all_assets_forecast = np.sort(np.unique(existing_assets + [os.path.split(asset)[1] for asset in new_assets_forecast]))

        logging.info('New assets for {}: {}, max: {}'.format(
            VAR, len(new_dates_forecast), MAX_ASSETS))
        logging.info('SUCCESS for {}'.format(VAR))
        # Delete local tif files because we will run out of space
        if DELETE_LOCAL:
            try:
                files_available = os.listdir(DATA_DIR)
                for f in files_available:
                    if f.endswith(".tif"):
                        logging.info('Removing {}'.format(f))
                        os.remove(DATA_DIR + '/' + f)
            except NameError:
                logging.info('No local tiff files to clean.')

    # Delete local netcdf files
    if DELETE_LOCAL:
        try:
            for f in os.listdir(DATA_DIR):
                logging.info('Removing {}'.format(f))
                os.remove(DATA_DIR+'/'+f)
        except NameError:
            logging.info('No local files to clean.')



    '''
    Update layers in Resource Watch back office.
    '''
    if new_dates_historical and new_dates_forecast:
        logging.info('Updating Resource Watch Layers')
        for VAR, ds_id in DATASET_IDS.items():
            logging.info('Updating {}'.format(VAR))
            #pull dictionary of current layers from API
            layer_dict = pull_layers_from_API(ds_id)
            #go through each layer, pull the definition and update
            for layer in layer_dict:
                #check which point on the timeline this is
                order = layer['attributes']['layerConfig']['order']

                #if this is the first point on the timeline, we want to replace it the most recent historical data
                if order==0:
                    # generate name for dataset's parent folder on GEE which will be used to store
                    # several collections - one collection per variable
                    PARENT_FOLDER = COLLECTION + '_historical_{metric}'
                    # generate generic string that can be formatted to name each variable's GEE collection
                    EE_COLLECTION_GEN = PARENT_FOLDER + '/{var}'
                    # generate generic string that can be formatted to name each variable's asset name
                    FILENAME = PARENT_FOLDER.split('/')[-1] + '_{var}_{date}'
                    # specify GEE collection name
                    EE_COLLECTION = EE_COLLECTION_GEN.format(metric=METRIC_BY_COMPOUND[VAR], var=VAR)

                    #get date of most recent asset added
                    date = new_dates_historical[-1]

                    #replace layer asset and title date with new
                    update_layer(layer, date)


                # otherwise, we want to replace it with the appropriate forecast data
                else:
                    # generate name for dataset's parent folder on GEE which will be used to store
                    # several collections - one collection per variable
                    PARENT_FOLDER = COLLECTION + '_forecast_{metric}'
                    # generate generic string that can be formatted to name each variable's GEE collection
                    EE_COLLECTION_GEN = PARENT_FOLDER + '/{var}'
                    # generate generic string that can be formatted to name each variable's asset name
                    FILENAME = PARENT_FOLDER.split('/')[-1] + '_{var}_{date}'
                    # specify GEE collection name
                    EE_COLLECTION = EE_COLLECTION_GEN.format(metric=METRIC_BY_COMPOUND[VAR], var=VAR)

                    #forecast layers start at order 1, and we will want this point on the timeline to be the first forecast asset
                    # order 4 will be the second asset, and so on
                    #get date of appropriate asset
                    date = new_dates_forecast[order-1]

                    #replace layer asset and title date with new
                    update_layer(layer, date)
    elif not new_dates_historical and not new_dates_forecast:
        logging.info('Layers do not need to be updated.')
    else:
        if not new_dates_historical:
            logging.error('Historical data was not updated, but forecast was.')
        if not new_dates_forecast:
            logging.error('Forecast data was not updated, but historical was.')
    '''
    Update Last Update Date and flush tile cache on RW
    '''
    # generate name for dataset's parent folder on GEE - we will set date based on 'historical' data
    PARENT_FOLDER = COLLECTION + '_historical_{metric}'
    # generate generic string that can be formatted to name each variable's GEE collection
    EE_COLLECTION_GEN = PARENT_FOLDER + '/{var}'
    for var_num in range(len(VARS)):
        VAR = VARS[var_num]
        EE_COLLECTION = EE_COLLECTION_GEN.format(metric=METRIC_BY_COMPOUND[VAR], var=VAR)
        existing_assets = eeUtil.ls(EE_COLLECTION)
        try:
            # Get most recent date to use as last update date
            # to show most recent date in collection, instead of start date for forecast run
            # use get_most_recent_date(new_assets) function instead
            most_recent_date = get_most_recent_date(existing_assets)
            logging.info(most_recent_date)
            current_date = getLastUpdate(DATASET_IDS[VAR])

            if current_date != most_recent_date: #comment for testing
                logging.info('Updating last update date and flushing cache.')
                # Update data set's last update date on Resource Watch
                lastUpdateDate(DATASET_IDS[VAR], most_recent_date)
                # get layer ids and flush tile cache for each
                layer_ids = getLayerIDs(DATASET_IDS[VAR])
                for layer_id in layer_ids:
                    flushTileCache(layer_id)
        except KeyError:
            continue

    # Delete local netcdf and tif files
    if DELETE_LOCAL:
        try:
            for f in os.listdir(DATA_DIR):
                logging.info('Removing {}'.format(f))
                os.remove(DATA_DIR+'/'+f)
        except NameError:
            logging.info('No local files to clean.')

    logging.info('SUCCESS')
