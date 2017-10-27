from __future__ import print_function, division
import wget
import os
import sys
import threading
import datetime
from netCDF4 import Dataset
import numpy as np
import rasterio
import tinys3
from rasterio.transform import from_origin

# Download last dataset (2 days in the past)

def dataDownload(): 
    today = datetime.date.today() - datetime.timedelta(days=2)
    url='ftp://ftp.star.nesdis.noaa.gov/pub/sod/mecb/crw/data/5km/nc/baa_max_comp_7day/2017/baa_max_r07d_b05kmnn_'+ str(today.year) +"%02d" % (today.month)+"%02d" % (today.day)+'.nc'
    filename = wget.download(url)
    print('data download finish')
    return filename


# Convert nc to geotiff


def netcdf2tif(dst,outFile):
    nc = Dataset(dst)
    data = nc['CRW_BAA_max7d'][0,:,:].squeeze()
    # Return lat info
    south_lat = nc.geospatial_lat_min - nc.geospatial_lat_resolution/2  # Change pos.to edges of pxls (not center)
    north_lat = nc.geospatial_lat_max + nc.geospatial_lat_resolution/2

    # Return lon info
    west_lon = nc.geospatial_lon_min - nc.geospatial_lon_resolution/2
    east_lon = nc.geospatial_lon_max + nc.geospatial_lon_resolution/2
    # Transformation function
    transform = rasterio.transform.from_bounds(west_lon, south_lat, east_lon, north_lat, data.shape[1], data.shape[0])
    # Profile
    profile = {
        'driver':'GTiff', 
        'height':data.shape[0], 
        'width':data.shape[1], 
        'count':1, 
        'dtype':np.int16, 
        'crs':'EPSG:4326', 
        'transform':transform, 
        'compress':'lzw', 
        'nodata':nc['CRW_BAA_max7d']._FillValue
    }
    with rasterio.open(outFile, 'w', **profile) as dst:
        dst.write(data.astype(profile['dtype']), 1)
    print('transformation finish') 


# S3 upload
def s3Upload(outFile):
    conn = tinys3.Connection(os.getenv('S3_ACCESS_KEY'),os.getenv('S3_SECRET_KEY'), tls=True, default_bucket=os.getenv('BUCKET'), endpoint="s3.amazonaws.com")
    # So we could skip the bucket parameter on every request
    response = conn.upload(key=outFile, local_file=open(outFile,'rb'), public=True, close=True)
    if response.status_code==200:
        print('SUCCESS')
    else:
        print('UPLOAD PROCESS FAILURE STATUS CODE:' + str(response.status_code))
        print(response.content)


# Execution
outFile ='CoralReefHotspots.tif'
file = dataDownload()
netcdf2tif(file,outFile)
s3Upload(outFile)

