from __future__ import print_function, division
import wget
import os
import sys
import threading
import datetime
from netCDF4 import Dataset
import numpy as np
import rasterio
import boto3
from rasterio.transform import from_origin

class bcolors:
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    ENDC = '\033[0m'
    UNDERLINE = '\033[4m'

class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()
    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()

# Download last dataset (2 days in the past)

def dataDownload(): 
    today = datetime.date.today()
    url='ftp://ftp.star.nesdis.noaa.gov/pub/sod/mecb/crw/data/5km/nc/baa_max_comp_7day/2017/baa_max_r07d_b05kmnn_'+ str(today.year) +"%02d" % (today.month)+"%02d" % (today.day-2)+'.nc'
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
    # Get the service client
    s3 = boto3.client('s3', aws_access_key_id=os.getenv('S3_ACCESS_KEY'), aws_secret_access_key=os.getenv('S3_SECRET_KEY'))
    print('s3 connection open; proceeded to upload data...') 
    # Upload a file-like object to bucket-name at key-name
    try:
        with open(outFile, "rb") as f:
            response = s3.upload_fileobj(f, os.getenv('BUCKET'), outFile, ExtraArgs={'ACL': 'public-read'}, Callback=ProgressPercentage(outFile))
            print(response)
        
    except Exception as error:
        print(error)
    
    #s3._endpoint.http_session.close()

    # conn = tinys3.Connection(os.getenv('S3_ACCESS_KEY'),os.getenv('S3_SECRET_KEY'), tls=True, default_bucket=os.getenv('BUCKET'), endpoint="s3.amazonaws.com")
    # # So we could skip the bucket parameter on every request
    # print('r')
    # print(conn)
    # response = conn.upload(key=outFile, local_file=open(outFile,'rb'), public=True, close=True)
    # print('rffffff')

    # if response.status_code==200:
    #     print('SUCCESS')
    # else:
    #     print('UPLOAD PROCESS FAILURE STATUS CODE:' + str(response.status_code))
    #     print(response.content)


# Execution
outFile ='CoralReefHotspots.tif'
file = dataDownload()
netcdf2tif(file,outFile)
s3Upload(outFile)

