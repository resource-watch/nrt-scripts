from __future__ import unicode_literals

import os
import sys
import datetime
import logging
import subprocess
import eeUtil
import requests
import time
import urllib
import urllib.request
import gdal
import numpy as np
from collections import OrderedDict 
import json 

'''
************************************ Useful Info About Source Data **********************************************************
# this dataset requires acquiring several separate netcdf files
# file path examples:
# baa:  ftp://ftp.star.nesdis.noaa.gov/pub/sod/mecb/crw/data/5km/v3.1/nc/v1.0/daily/baa-max-7d/2020/ct5km_baa-max-7d_v3.1_20200623.nc
# hs:   ftp://ftp.star.nesdis.noaa.gov/pub/sod/mecb/crw/data/5km/v3.1/nc/v1.0/daily/hs/2020/ct5km_hs_v3.1_20200622.nc
# dhw:  ftp://ftp.star.nesdis.noaa.gov/pub/sod/mecb/crw/data/5km/v3.1/nc/v1.0/daily/dhw/2020/ct5km_dhw_v3.1_20200622.nc
# ssta: ftp://ftp.star.nesdis.noaa.gov/pub/sod/mecb/crw/data/5km/v3.1/nc/v1.0/daily/ssta/2020/ct5km_ssta_v3.1_20200622.nc
# sst:  ftp://ftp.star.nesdis.noaa.gov/pub/sod/mecb/crw/data/coraltemp/v1.0/nc/2020/coraltemp_v1.0_20200622.nc
# sstt: ftp://ftp.star.nesdis.noaa.gov/pub/sod/mecb/crw/data/5km/v3.1/nc/v1.0/daily/sst-trend-7d/2020/ct5km_sst-trend-7d_v3.1_20200622.nc

# baa original: -5=No Data (land); 0=No Stress; 1=Watch; 2=Warning; 3=Alert Level 1; 4=Alert Level 2
#     valid range [0,4]
#     fill value -5
#     scalefactor = 1
# hs original: from low negatives up to about 10(C). scale bar from 0-5. nodata=nodata(land)
#     valid range [-1500,1500]
#     fill value -32768
#     scalefactor = 0.0099999998
# dhw original
#     valid range [0,10000]
#     fill value -32768
#     scalefactor = 0.0099999998
# sst anomaly
#     valid range [-1500,1500]
#     fill value -32768
#     scalefactor = 0.0099999998
# sst
#     valid range [-200,5000]
#     fill value -32768
#     scale_factor=0.01
# sst trend
#     valid range [-1500,1500]
#     fill value -32768
#     scale_factor=0.0099999998

***********************************************************************************************
'''

# note that all netcdfs contain a "mask" subdataset, but it is the same for each so does not need to be extracted
# create an ordered dictionary to store information about all the netcdf files that we want to fetch and process
DATA_DICT = OrderedDict()
DATA_DICT['bleaching_alert_area_7d'] = {
        'url_template': 'ftp://ftp.star.nesdis.noaa.gov/pub/sod/mecb/crw/data/5km/v3.1/nc/v1.0/daily/baa-max-7d/{}/ct5km_baa-max-7d_v3.1_{}.nc',
        'sds': [
            'bleaching_alert_area',
        ],
        'original_nodata': 251,
        'missing_data': [
            -32768,
        ],
        'pyramiding_policy': 'MEAN',
    }
DATA_DICT['hotspots'] = {
        'url_template': 'ftp://ftp.star.nesdis.noaa.gov/pub/sod/mecb/crw/data/5km/v3.1/nc/v1.0/daily/hs/{}/ct5km_hs_v3.1_{}.nc',
        'sds': [
            'hotspot',
        ],
        'original_nodata': -32768,
        'missing_data': [
            -32768,
        ],
        'pyramiding_policy': 'MEAN',
        'scale_factor': 0.0099999998,
    }
DATA_DICT['degree_heating_week'] = {
        'url_template': 'ftp://ftp.star.nesdis.noaa.gov/pub/sod/mecb/crw/data/5km/v3.1/nc/v1.0/daily/dhw/{}/ct5km_dhw_v3.1_{}.nc',
        'sds': [
            'degree_heating_week',
        ],
        'original_nodata': -32768,
        'missing_data': [
            -32768,
        ],
        'pyramiding_policy': 'MEAN',
        'scale_factor': 0.0099999998,
    }
DATA_DICT['sea_surface_temperature_anomaly'] = {
        'url_template': 'ftp://ftp.star.nesdis.noaa.gov/pub/sod/mecb/crw/data/5km/v3.1/nc/v1.0/daily/ssta/{}/ct5km_ssta_v3.1_{}.nc',
        'sds': [
            'sea_surface_temperature_anomaly',
        ],
        'original_nodata': -32768,
        'missing_data': [
            -32768,
        ],
        'pyramiding_policy': 'MEAN',
        'scale_factor': 0.0099999998,
    }
DATA_DICT['sea_surface_temperature'] = {
        'url_template': 'ftp://ftp.star.nesdis.noaa.gov/pub/sod/mecb/crw/data/coraltemp/v1.0/nc/{}/coraltemp_v1.0_{}.nc',
        'sds': [
            'analysed_sst',
        ],
        'original_nodata': -32768,
        'missing_data': [
            -32768,
        ],
        'pyramiding_policy': 'MEAN',
        'scale_factor': 0.01,
    }
DATA_DICT['sea_surface_temperature_trend_7d'] = {
        'url_template': 'ftp://ftp.star.nesdis.noaa.gov/pub/sod/mecb/crw/data/5km/v3.1/nc/v1.0/daily/sst-trend-7d/{}/ct5km_sst-trend-7d_v3.1_{}.nc',
        'sds': [
            'trend',
        ],
        'original_nodata': -32768,
        'missing_data': [
            -32768,
        ],
        'pyramiding_policy': 'MEAN',
        'scale_factor': 0.0099999998,
    }

# filename format for GEE
FILENAME = 'ocn_007_coral_bleaching_monitoring_{date}'

# name of data directory in Docker container
DATA_DIR = os.path.join(os.getcwd(),'data')

# name of collection in GEE where we will upload the final data
EE_COLLECTION = '/projects/resource-watch-gee/ocn_007_coral_bleaching_monitoring'

# name of folder to store data in Google Cloud Storage
GS_FOLDER = EE_COLLECTION[1:]

# do you want to delete everything currently in the GEE collection when you run this script?
CLEAR_COLLECTION_FIRST = False

# how many assets can be stored in the GEE collection before the oldest ones are deleted?
MAX_ASSETS = 16

# format of date used in both source and GEE
DATE_FORMAT = '%Y%m%d'

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = '574f0b71-8363-4e3a-978f-2b1ce58c1c33'

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
    # get a list of all the layers
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
    tries=4
    while try_num<tries:
        try:
            # try to delete the cache
            logging.info(headers)
            logging.info(apiUrl)
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
def getFilename(date):
     '''
     generate filename to save final merged file as 
     INPUT   date: date in the format of the DATE_FORMAT variable (string)
     RETURN  file name to save merged tif file (string)
     '''
     return (os.path.join(DATA_DIR, FILENAME.format(date=date)) + '_merged.tif' )

def getAssetName(date):
     '''
     get asset name
     INPUT   date: date in the format of the DATE_FORMAT variable (string)
     RETURN  GEE asset name for input date (string)
     '''
     return os.path.join(EE_COLLECTION, FILENAME.format(date=date))

def getDate(filename):
     '''
     get date from asset name (last 8 characters of filename after removing extension)
     INPUT   filename: file name that ends in a date of the format YYYYMMDD (string)
     RETURN  existing_dates: dates in the format YYYYMMDD (string)
     '''
     existing_dates = os.path.splitext(os.path.basename(filename))[0][-8:]

     return existing_dates

def find_latest_date():
    '''
    Fetch the latest date for which coral bleach monitoring data is available
    RETURN  latest_available_date: latest date available for download from source website (string)
    '''   
    # get one of the source url from DATA_DICT and split it to get the parent directory where 
    # inidividual folder for each year is present
    url = DATA_DICT.get('sea_surface_temperature_trend_7d')['url_template'].split('{')[0]
    # try to open and fetch data from the url
    try:
        # open the url
        response = urllib.request.urlopen(url)
        # read the opened url
        content = response.read()
        # use string manipulation to get all the available links in source url
        links = [url + line.split()[-1] for line in content.decode().splitlines()]
        # split the folder names on '/' and get the last elements after each split to retrieve years
        # also get the last element from the collection of years to get the latest year
        latest_year = ([s.split('/')[-1] for s in links])[-1]
        # generate a sub url to fetch data from the latest year folder
        sub_url = url + latest_year + '/'
        # open the sub url
        response = urllib.request.urlopen(sub_url)
        # read the opened sub url
        content = response.read()
        # use string manipulation to get all the available links in sub url 
        sub_link = [sub_url + line.split()[-1] for line in content.decode().splitlines()]
        # use string manipulation to separate out the dates from the links
        available_dates = ([s.split('.nc')[0].split('_')[-1] for s in sub_link])
        # last element in available_dates list is the latest date for which 'sea_surface_temperature_trend_7d' 
        # data is available; we also want to make sure other data sources also have data for this date

        # set success to False initially
        success = False
        # initialize tries count as 0
        tries = 0
        # start with latest date and then go backwards if data is not available for every source    
        idx = -1
        max_tries = 5
        # try to get the data from the url for max_tries 
        while tries < max_tries and success == False:
            logging.info('Checking availibility of data in every sources, try number = {}'.format(tries))
            try:
              # pull data from source url for every item in the global dictionary
              url_check = [urllib.request.urlopen(val['url_template'].format(latest_year, available_dates[idx])) 
                            for key, val in DATA_DICT.items()]
              # if data is available in every source for the date in the iteration, set it as latest available date  
              latest_available_date = available_dates[idx]
              # set success as True after retrieving the data to break out of this loop
              success = True
            # if unsuccessful, log error and try again for an older date  
            except Exception as inst:
              logging.info(inst)
              logging.info("Error fetching data, trying again for an older date")
              # increase the count of tries
              tries = tries + 1
              # change index to use one step older date in next iteration
              idx = idx - 1
              # if we reach maximum try, break out 
              if tries == max_tries:
                logging.error("Error fetching data, and max tries reached. See source for last data update.")
        # if we suceessfully collected data from the url
        if success == True:
            # construct complete urls for the latest available date and add it as a new key in the parent dictionary 
            for key, val in DATA_DICT.items():
                val['url'] = val['url_template'].format(latest_year, latest_available_date)
            
            return latest_available_date

    except Exception as e:
      # if unsuccessful, log that no data were found from the source url
      logging.debug('No data found from url {})'.format(url))
      logging.debug(e)
      return ()


def fetch():
     '''
     Fetch latest netcdef files by using the url from the global dictionary
     '''
     logging.info('Downloading raw data')
     # go through each item in the parent dictionary
     for key, val in DATA_DICT.items():
        # get the url from the key 'url'
        url = val['url']
        # create a path under which to save the downloaded file
        raw_data_file = os.path.join(DATA_DIR,os.path.basename(url))
        try:
            # try to download the data
            urllib.request.urlretrieve(url, raw_data_file)
            # if successful, add the file to a new key in the parent dictionary
            val['raw_data_file'] = raw_data_file
            logging.debug('('+key+')'+'Raw data file path: ' + raw_data_file)
        except Exception as e:
            # if unsuccessful, log an error that the file was not downloaded
            logging.error('Unable to retrieve data from {}'.format(url))
            logging.debug(e)

def convert_netcdf(nc, subdatasets):
    '''
    Convert netcdf files to geotifs
    INPUT   nc: file name of netcdf to convert (string)
            subdatasets: subdataset names to extract to individual geotiffs (list of strings)
    RETURN  tifs: file names of generated geotiffs (list of strings)
    '''
    # create an empty list to store the names of the tifs we generate from this netcdf file
    tifs = []
    # go through each variables to process in this netcdf file
    for sds in subdatasets:
        # extract subdataset by name
        # should be of the format 'NETCDF:"filename.nc":variable'
        sds_path = f'NETCDF:"{nc}":{sds}'
        # generate a name to save the tif file we will translate the netcdf file's subdataset into
        sds_tif = '{}_{}.tif'.format(os.path.splitext(nc)[0], sds_path.split(':')[-1])
        # translate the netcdf file's subdataset into a tif
        cmd = ['gdal_translate','-q', '-a_srs', 'EPSG:4326', sds_path, sds_tif]
        completed_process = subprocess.run(cmd, shell=False)
        logging.debug(str(completed_process))
        if completed_process.returncode!=0:
            raise Exception('NetCDF conversion using gdal_translate failed! Command: '+str(cmd))
        # add the new subdataset tif files to the list of tifs generated from this netcdf file
        tifs.append(sds_tif)
    return tifs

def scale_geotiff(tif, scaledtif=None, scale_factor=None, nodata=None, gdal_type=gdal.GDT_Float32):
    '''
    Apply scale factor to geotiff, writing the result to a new geotiff file. 
    Raster values and linked metadata are changed; all other metadata are preserved.
    This function's complexity comes from metadata preservation, and is written with an eye
    towards typical NetCDF metadata contents and structure. If these elements are not relevant,
    then gdal_edit.py or gdal_calc.py may be a simpler solution.
    INPUT   tif: file name of single-band geotiff to be scaled (string)
            scaledtif: file name of output raster; if None, input file name is appended (string)
            scale_factor: scale factor to be applied; if None, value is drawn from metadata (numeric)
            nodata: value to indicate no data in output raster; if None, original value is used (numeric)
            gdal_type: GDAL numeric type of the output raster (gdalconst(int))
    RETURN scaledtif: filename of scaled output raster (string)
    '''
    # open the tif file using gdal
    geotiff = gdal.Open(tif, gdal.gdalconst.GA_ReadOnly)
    # verify that the geotiff has exactly one band
    assert (geotiff.RasterCount == 1)
    # Read the raster band as separate variable
    band = geotiff.GetRasterBand(1)
    
    # read in raster band as a numpy array
    raster = np.array(band.ReadAsArray())    
    
    # retrieve nodata/fill from band metadata
    # identify nodata entries in raster
    nodata_mask = (raster == band.GetNoDataValue())
    
    # retrieve scale factor from band metadata
    band_metadata = band.GetMetadata()
    band_scale_keys = [key for key, val in band_metadata.items() if 'scale' in key.lower()]
    assert (len(band_scale_keys)<=1)
    band_fill_keys = [key for key, val in band_metadata.items() if 'fill' in key.lower()]
    assert (len(band_fill_keys)<=1)
    assert (float(band_metadata[band_fill_keys[0]])==band.GetNoDataValue())
    if scale_factor is None:
        scale_factor = float(band_metadata[band_scale_keys[0]])
    
    # apply scale factor to raster
    logging.debug(f'Applying scale factor of {scale_factor} to raster of GeoTiff {os.path.basename(tif)}')
    new_raster = raster * scale_factor
    
    # apply nodata fill as desired
    if nodata is None:
        nodata = band.GetNoDataValue()
    new_raster[nodata_mask] = nodata
    
    # update band metadata
    new_band_metadata = band_metadata.copy()
    if len(band_scale_keys) > 0:
        new_band_metadata[band_scale_keys[0]] = str(1)
    if len(band_fill_keys) > 0:
        new_band_metadata[band_fill_keys[0]] = str(nodata)
    if 'valid_max' in new_band_metadata:
        new_band_metadata['valid_max'] = str(float(band_metadata['valid_max']) * scale_factor)
    if 'valid_min' in new_band_metadata:
        new_band_metadata['valid_min'] = str(float(band_metadata['valid_min']) * scale_factor)
    
    # update geotiff metadata
    ds_metadata = geotiff.GetMetadata()
    ds_scale_keys = [key for key, val in ds_metadata.items() if 'scale' in key.lower()]
    assert (len(ds_scale_keys)<=1)
    metadata_band_prefix = ds_scale_keys[0].split('#')[0]
    ds_fill_keys = [key for key, val in ds_metadata.items() if 'fill' in key.lower()]
    assert (len(ds_fill_keys)<=1)
    ds_valid_min_keys = [key for key, val in ds_metadata.items() if metadata_band_prefix.lower()+'#'+'valid_min' in key.lower()]
    assert (len(ds_valid_min_keys)<=1)
    ds_valid_max_keys = [key for key, val in ds_metadata.items() if metadata_band_prefix.lower()+'#'+'valid_max' in key.lower()]
    assert (len(ds_valid_max_keys)<=1)
    
    new_ds_metadata = ds_metadata.copy()
    if len(ds_scale_keys) > 0:
        new_ds_metadata[ds_scale_keys[0]] = str(1)
    if len(ds_fill_keys) > 0:
        new_ds_metadata[ds_fill_keys[0]] = str(nodata)
    if len(ds_valid_min_keys) > 0:
        new_ds_metadata[ds_valid_min_keys[0]] = str(float(ds_metadata[ds_valid_min_keys[0]]) * scale_factor)
    if len(ds_valid_max_keys) > 0:
        new_ds_metadata[ds_valid_max_keys[0]] = str(float(ds_metadata[ds_valid_max_keys[0]]) * scale_factor)
    
    # create output dataset
    # get output file name
    if scaledtif is None:
        dotindex = tif.rindex('.')
        scaledtif = tif[:dotindex] + '_scaled' + tif[dotindex:]
    [cols, rows] = raster.shape
    driver = gdal.GetDriverByName("GTiff")
    outds = driver.Create(scaledtif, rows, cols, 1, gdal_type)
    outds.SetGeoTransform(geotiff.GetGeoTransform())
    outds.SetProjection(geotiff.GetProjection())
    outds.GetRasterBand(1).WriteArray(new_raster)
    outds.GetRasterBand(1).SetMetadata(new_band_metadata)
    outds.GetRasterBand(1).SetNoDataValue(nodata)
    outds.SetMetadata(new_ds_metadata)
    outds.FlushCache()
    outds = None
    band = None
    geotiff = None
    
    return scaledtif 

def assign_nodata(inp_tif):
    '''
    Assign Nodata value to geotifs
    INPUT   inp_tif: file name of tif to translate (string)
    RETURN  trs_tif2: file name of generated geotiff (string)
    '''

    # generate a name to save the tif file we will translate the input file into
    trs_tif = inp_tif + '_scaled1.tif'
    # assign the no data value of 251
    cmd = ['gdal_translate','-q', '-a_nodata', '251', inp_tif, trs_tif]
    subprocess.run(cmd, shell=False)
    # assign the no data value of 5
    trs_tif2 = inp_tif + '_scaled.tif'
    cmd = ['gdal_translate','-q', '-a_nodata', '-5', trs_tif, trs_tif2]
    subprocess.run(cmd, shell=False)

    return trs_tif2

def processNewData(existing_dates):
    '''
    fetch, process, upload, and clean new data
    INPUT   existing_dates: list of dates we already have in GEE (list of strings)
    RETURN  asset: file name for asset that have been uploaded to GEE (string)
    '''

    # Get latest available date that is availble on the source
    available_date = find_latest_date()
    logging.debug('Latest available date: {}'.format(available_date))

    # if we don't have this date and time step already in GEE
    if available_date not in existing_dates:
        # fetch files for the latest date
        logging.info('Fetching files')        
        fetch()
        # convert netcdfs to tifs and store the tif filenames to a new key in the parent dictionary
        logging.info('Extracting relevant GeoTIFFs from source NetCDFs, and modifying nodata values in the resulting GeoTIFFs where appropriate')

        merge_list = []
        global_nodata = -32768
        # windows-necessary variable
        # calc_path = os.path.abspath(os.path.join(os.getenv('GDAL_DIR'),'gdal_calc.py'))

        for key, val in DATA_DICT.items():
            nc = val['raw_data_file'] # originally raw_data_file
            sds = val['sds'][0]
            local_nodata = val['original_nodata']

            # should be of the format 'NETCDF:"filename.nc":variable'
            sds_path = f'NETCDF:"{nc}":{sds}'
            # generate a name to save the tif file we will translate the netcdf file's subdataset into
            sds_tif = '{}_{}.tif'.format(os.path.splitext(nc)[0], sds_path.split(':')[-1])
    
            #cmd = f'gdal_translate -q -a_srs EPSG:4326 -a_nodata {local_nodata} -ot Float32 -unscale {sds_path} {sds_tif} '
            cmd = ['gdal_translate','-q', '-a_srs', 'EPSG:4326', '-a_nodata' , str(local_nodata), '-ot', 'Float32', '-unscale', sds_path, sds_tif]
            completed_process = subprocess.run(cmd, shell=False)
            logging.debug(str(completed_process))

            if local_nodata != global_nodata:
                sds_tif_edited = sds_tif.split('.tif')[0]+'_edited.tif'
                # windows style:
                # cmd = f'"{sys.executable}" "{calc_path}" -A {sds_tif} --outfile={sds_tif_edited} --NoDataValue=-32768 --calc="(A!=251)*A+(A==251)*-32768"'
                #cmd = f'gdal_calc.py -A {sds_tif} --outfile={sds_tif_edited} --NoDataValue=-32768 --calc="(A!=251)*A+(A==251)*-32768"'
                cmd = ['gdal_calc.py', '-A', sds_tif, '--outfile', sds_tif_edited, '--NoDataValue' , str(-32768), '--calc', "(A!=251)*A+(A==251)*-32768"]
                completed_process = subprocess.run(cmd, shell=False)
                logging.debug(str(completed_process))
                sds_tif = sds_tif_edited

            val['tifs'] = [sds_tif]
            merge_list.append(sds_tif)

        logging.info(os.listdir(DATA_DIR))
        merge_list_str = ' '.join(merge_list)
        # windows-necessary variable
        # merge_path = os.path.abspath(os.path.join(os.getenv('GDAL_DIR'),'gdal_merge.py'))
        merged_vrt = 'merged.vrt'

        logging.info('Merging masked, single-band GeoTIFFs into single, multiband VRT')

        #cmd = f'gdalbuildvrt -separate {merged_vrt} {merge_list_str}'
        logging.info(merge_list_str)
        cmd = ['gdalbuildvrt', '-separate', merged_vrt]
        cmd.extend(merge_list)
        completed_process = subprocess.run(cmd, shell=False)
        logging.debug(completed_process)

        logging.info('Converting multiband VRT into multiband GeoTIFF')

        # generate a name to save the tif file that will be produced by merging all the individual tifs   
        merged_tif = getFilename(available_date) 

        #cmd = f'gdal_translate -of GTiff {merged_vrt} {merged_tif}'
        cmd = ['gdal_translate', '-of', 'GTiff', merged_vrt, merged_tif]
        completed_process = subprocess.run(cmd, shell=False)
        logging.info(completed_process)

        logging.info('Uploading files')
        # Generate a name we want to use for the asset once we upload the file to GEE
        asset = [getAssetName(available_date)]
        # Get a datetime from the date we are uploading
        datestamp = [datetime.datetime.strptime(available_date, DATE_FORMAT)]
        # Upload new file (tif) to GEE
        eeUtil.uploadAssets([merged_tif], asset, GS_FOLDER, dates=datestamp, timeout=3000)

        return asset
    else:
        logging.info('Data already up to date')
        # if no new assets, return empty list
        return []

def checkCreateCollection(collection):
    '''
    List assests in collection if it exists, else create new collection
    INPUT   collection: GEE collection to check or create (string)
    RETURN  list of assets in collection (list of strings)
    '''
    # if collection exists, return list of assets in collection
    if eeUtil.exists(collection):
        return eeUtil.ls(collection)
    # if collection does not exist, create it and return an empty list (because no assets are in the collection)
    else:
        logging.info('{} does not exist, creating'.format(collection))
        eeUtil.createFolder(collection, imageCollection=True, public=True)
        return []

def deleteExcessAssets(dates, max_assets):
    '''
    Delete oldest assets, if more than specified in max_assets variable
    INPUT   dates: dates for all the assets currently in the GEE collection; dates should be in the format specified
                    in DATE_FORMAT variable (list of strings)
            max_assets: maximum number of assets allowed in the collection (int)
    '''
    # sort the list of dates so that the oldest is first
    dates.sort()
    # if we have more dates of data than allowed,
    if len(dates) > max_assets:
        # go through each date, starting with the oldest, and delete until we only have the max number of assets left
        for date in dates[:-max_assets]:
            eeUtil.removeAsset(getAssetName(date))

def get_most_recent_date(collection):
    '''
    Get most recent date we have assets for
    INPUT   collection: GEE collection to check dates for (string)
    RETURN  most_recent_date: most recent date in GEE collection (datetime)
    '''
    # get list of assets in collection
    existing_assets = checkCreateCollection(collection)
    # get a list of strings of dates in the collection
    existing_dates = [getDate(a) for a in existing_assets]
    # sort these dates oldest to newest
    existing_dates.sort()
    # get the most recent date (last in the list) and turn it into a datetime
    most_recent_date = datetime.datetime.strptime(existing_dates[-1], DATE_FORMAT)

    return most_recent_date

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

def create_headers():
    '''
    Create headers to perform authorized actions on API

    '''
    return {
        'Content-Type': "application/json",
        'Authorization': "{}".format(os.getenv('apiToken')),
    }

def update_layer(layer, new_date):
    '''
    Update layers in Resource Watch back office.
    INPUT  layer: layer that will be updated (string)
           new_date: date of asset to be shown in this layer (datetime)
    '''
    
    # get previous date being used from
    old_date = datetime.datetime.strptime(getDate(layer['attributes']['layerConfig']['assetId']), DATE_FORMAT)
    # convert old datetimes to string
    old_date_text = old_date.strftime("%B %d, %Y")

    # convert new datetimes to string
    new_date_text = new_date.strftime("%B %d, %Y")

    # replace date in layer's title with new date range
    layer['attributes']['name'] = layer['attributes']['name'].replace(old_date_text, new_date_text)

    # store the current asset id used in the layer 
    old_asset = layer['attributes']['layerConfig']['assetId']
    # find the asset id of the latest image 
    new_asset = getAssetName(new_date.strftime(DATE_FORMAT))[1:]
    # replace the asset id in the layer def with new asset id
    layer['attributes']['layerConfig']['assetId'] = new_asset

    # replace the asset id in the interaction config with new asset id
    layer['attributes']['interactionConfig']['config']['url'] = layer['attributes']['interactionConfig']['config']['url'].replace(old_asset,new_asset)

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
    # if we get a 200, the layers have been replaced
    # if we get a 504 (gateway timeout) - the layers are still being replaced, but it worked
    if r.ok or r.status_code==504:
        logging.info('Layer replaced: {}'.format(layer['id']))
    else:
        logging.error('Error replacing layer: {} ({})'.format(layer['id'], r.status_code))

def updateResourceWatch():
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date', flushing the tile cache, and updating any dates on layers
    '''
    # Get the most recent date from the data in the GEE collection
    most_recent_date = get_most_recent_date(EE_COLLECTION)
    # Get the current 'last update date' from the dataset on Resource Watch
    current_date = getLastUpdate(DATASET_ID)
    
    # pull dictionary of current layers from API
    layer_dict = pull_layers_from_API(DATASET_ID)
    # go through each layer, pull the definition and update
    for layer in layer_dict:
        # update layer name, asset id, and interaction configuration 
        update_layer(layer, most_recent_date)
        
    # If the most recent date from the GEE collection does not match the 'last update date' on the RW API, update it
    if current_date != most_recent_date:
        logging.info('Updating last update date and flushing cache.')
        # Update dataset's last update date on Resource Watch
        lastUpdateDate(DATASET_ID, most_recent_date)
        # get layer ids and flush tile cache for each
        layer_ids = getLayerIDs(DATASET_ID)
        for layer_id in layer_ids:
            flushTileCache(layer_id)
            
    
    

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Initialize eeUtil
    eeUtil.initJson()

    # clear the GEE collection, if specified above
    if CLEAR_COLLECTION_FIRST:
        if eeUtil.exists(EE_COLLECTION):
            eeUtil.removeAsset(EE_COLLECTION, recursive=True)

    # Check if collection exists, create it if it does not
    # If it exists return the list of assets currently in the collection
    existing_assets = checkCreateCollection(EE_COLLECTION)
    # Get a list of the dates of data we already have in the collection
    existing_dates = [getDate(a) for a in existing_assets]

    # Fetch, process, and upload the new data
    os.chdir(DATA_DIR)
    new_asset = processNewData(existing_dates)
    # Get the dates of the new data we have added
    new_dates = [getDate(a) for a in new_asset]

    logging.info('Previous assets: {}, new: {}, max: {}'.format(
          len(existing_dates), len(new_dates), MAX_ASSETS))

    # Delete excess assets
    deleteExcessAssets(existing_dates+new_dates, MAX_ASSETS)

    # Update Resource Watch
    updateResourceWatch()

    logging.info('SUCCESS')
