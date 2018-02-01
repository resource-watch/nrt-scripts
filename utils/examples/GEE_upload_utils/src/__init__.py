import numpy as np
import sys
import os
from ftplib import FTP
from netCDF4 import Dataset
import rasterio
import tinys3
from .geeUploadsUtils import *


np.set_printoptions(threshold='nan')

def dataDownload(): 
    remote_ftp = 'ftp.cdc.noaa.gov'
    last_file = 'air.mon.anom.nc'
    local_path = os.getcwd()
    filename='/Datasets/noaaglobaltemp/'+last_file
    
    # connect to ftp
    with FTP(remote_ftp) as ftp:
        ftp.login()
        #Download the file .nc
        with open(last_file, 'wb') as f:
            ftp.retrbinary("RETR " + filename, f.write)
    
    return last_file

def netcdf2tif(dst,outFile):
    nc = Dataset(dst)
    data = nc['air'][1,:,:]
            
    data[data < -40] = -99
    data[data > 40] = -99
    
    # Return lat info
    south_lat = -88.75
    north_lat = 88.75

    # Return lon info
    west_lon = -177.5
    east_lon = 177.5
    # Transformation function
    transform = rasterio.transform.from_bounds(west_lon, south_lat, east_lon, north_lat, data.shape[1], data.shape[0])
    # Profile
    profile = {
        'driver':'GTiff', 
        'height':data.shape[0], 
        'width':data.shape[1], 
        'count':1, 
        'dtype':np.float64, 
        'crs':'EPSG:4326', 
        'transform':transform, 
        'compress':'lzw', 
        'nodata':-99
    }
    with rasterio.open(outFile, 'w', **profile) as dst:
        dst.write(data.astype(profile['dtype']), 1)

    return { 'sources':[os.getcwd()+'/'+outFile],
    'gcsBucket':os.getenv('GCS_BUCKET'),
    'collectionAsset':'users/test-api/testcollection',
    'assetName':outFile.split('.')[0],
    'bandNames':[{'id': 'temp'}],
    'pyramidingPolicy':'MODE',
    'properties':{
        'system:time_start': '1994-11-05T13:15:30',#----------------- this is mandatory as gee will use it for date filtering it could be i n iso format or in epoch format
        'my_imageProperties':'to add to the collection'
        }   
    }


#def s3Upload(outFile):
#    # Push to Amazon S3 instance
#    conn = tinys3.Connection(os.getenv('S3_ACCESS_KEY'),os.getenv('S3_SECRET_KEY'),tls=True)
#    f = open(outFile,'rb')
#    conn.upload(outFile,f,os.getenv('BUCKET'))

# Execution
outFile ='air_temo_anomalies.tif'
print('starting')
file = dataDownload()
print('downloaded')
configFile = netcdf2tif(file,outFile)
print('converted')
#s3Upload(outFile)
assetManagement(configFile).execute()
print('finish')