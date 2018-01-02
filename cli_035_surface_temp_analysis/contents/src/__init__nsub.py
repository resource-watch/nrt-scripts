# Libraries to fetch data
from urllib.request import urlopen
import shutil
from contextlib import closing
import gzip

# Libraries to handle data
from netCDF4 import Dataset

# Library to interact with OS
import os

# Libraries to debug
import logging
import sys
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

# Utilities
import cloud
import misc
import process

# Script options
PROCESS_FULL_HISTORY = False
PROCESS_PARTIAL_HISTORY = False
PARTIAL_HISTORY_LENGTH = 120

###
## Procedure for obtaining the netcdf file, and processing it to tifs
###

def download_full_nc_history(tmpNcFolder):
    """
    Inputs: location to store nc file temporary
    Outputs: Newest available gistemp250 file name, along with time_start and time_end for the entire collection
    """
    remote_path = 'https://data.giss.nasa.gov/pub/gistemp/'
    ncFile_zipped = 'gistemp250.nc.gz'
    ncFile_name = tmpNcFolder + ncFile_zipped[:-3]

    local_path = os.getcwd()

    logging.info(remote_path)
    logging.info(ncFile_zipped)
    logging.info(ncFile_name)

    #Download the file .nc
    with closing(urlopen(remote_path + ncFile_zipped)) as r:
        with gzip.open(r, "rb") as unzipped:
            with open(ncFile_name, 'wb') as f:
                shutil.copyfileobj(unzipped, f)
    
    logging.info('Downloaded full nc history')
    
    # NEED TO READ TIME_START FROM THE DATA... is in metadata?
    #time_start = fix_datetime_UTC("")
    
    #today = datetime.datetime.now()
    #time_end = fix_datetime_UTC(today)
    nc = Dataset(ncFile_name)
    return (nc)

###
## Execution
### 

def main():
    
    logging.info('starting')
    
    # Create a temporary folder structure to store data
    tmpDataFolder = "tmpData"
    try:
        misc.cleanUp(tmpDataFolder)
        os.mkdir(tmpDataFolder)
    except:
        os.mkdir(tmpDataFolder)
    logging.info("Clean folder created")
    
    tmpNcFolder = tmpDataFolder + "/ncFiles/"
    tmpTifFolder = tmpDataFolder + "/tifFiles/"
    os.mkdir(tmpNcFolder)
    os.mkdir(tmpTifFolder)
    
    # Returns the entire history of GISTEMP in a netCDF file
    nc = download_full_nc_history(tmpNcFolder)
    time_var_name = 'time'
    data_var_name = 'tempanomaly'
    nodata_val = str(nc[data_var_name].getncattr("_FillValue"))
    band_names = "surface_temp_anomalies"
    
    # Populate the tmpTifFolder will all files to process
    tifFileName_stub = "cli_035_surface_temp_analysis_"
    if PROCESS_FULL_HISTORY:
        process.process_full_history_to_tifs(nc, time_var_name, data_var_name, 
                                     tmpTifFolder, tifFileName_stub)
    elif PROCESS_PARTIAL_HISTORY:
        process.process_partial_history_to_tifs(nc, time_var_name, data_var_name, 
                                   tmpTifFolder, tifFileName_stub, PARTIAL_HISTORY_LENGTH)
    else:
        process.process_most_recent_to_tif(nc, time_var_name, data_var_name, 
                                   tmpTifFolder, tifFileName_stub)
    
    # Process all files in the tmpTifFolder onto the cloud
    cloud_props = {
        "imageCollection": "cli_035_surface_temp_analysis",
        "gs_bucket": "resource-watch-public"
    }
    asset_props = {
        "nodata_val":nodata_val,
        "band_names":band_names
    }
    process.process_tif_files_to_cloud(tmpTifFolder, cloud_props, asset_props)
    
    # Clean up before exit
    misc.cleanUp(tmpDataFolder)

    logging.info('container process finished, container cleaned')
    
main()