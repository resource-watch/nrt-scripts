import pandas as pd
import numpy as np
from os.path import basename, dirname, exists
import os
import rasterio
import glob
import ftplib
import urllib2
import gzip
import shutil
from contextlib import closing
from netCDF4 import Dataset

mypath = "/Users/vizzuality/Documents/Vizzuality/RW/planet_pulse/nrt-scripts/"
new_dir = str(mypath)+'chirps'
!mkdir $new_dir


listing = []
response = urllib2.urlopen('ftp://chg-ftpout.geog.ucsb.edu/pub/org/chg/products/CHIRPS-2.0/global_daily/tifs/p05/2017/')
for line in response:
    listing.append(line.rstrip())

s2=pd.DataFrame(listing)
s3=s2[0].str.split()
s4=s3[len(s3)-1]
last_file = s4[8]

uncompressed = os.path.splitext(last_file)[0]

with closing(urllib2.urlopen('ftp://chg-ftpout.geog.ucsb.edu/pub/org/chg/products/CHIRPS-2.0/global_daily/tifs/p05/2017/'+str(last_file))) as r:
    with open(str(last_file), 'wb') as f:
        shutil.copyfileobj(r, f)

!gzip -d $last_file > $new_dir #uncompress my file raster
!gdalinfo $uncompressed 

src = rasterio.open(str(mypath)+str(uncompressed))

array = src.read(1)

with rasterio.open(str(mypath)+str(uncompressed)) as src:
    npixels = src.width * src.height
    for i in src.indexes:
        band = src.read(i)
        print(i, band.min(), band.max(), band.sum()/npixels)

CM_IN_FOOT = 30.48

with rasterio.drivers():
    with rasterio.open(str(mypath)+str(uncompressed)) as src:
        kwargs = src.meta
        kwargs.update(
            driver='GTiff',
            dtype=rasterio.float64,  #rasterio.int16, rasterio.int32, rasterio.uint8,rasterio.uint16, rasterio.uint32, rasterio.float32, rasterio.float64
            count=1,
            compress='lzw',
            nodata=0,
            bigtiff='YES' # Output will be larger than 4GB
        )

        windows = src.block_windows(1)

        with rasterio.open(str(mypath)+str(uncompressed),'w',**kwargs) as dst:
            for idx, window in windows:
                src_data = src.read(1, window=window)

                # Source nodata value is a very small negative number
                # Converting in to zero for the output raster
                np.putmask(src_data, src_data < 0, 0)

                dst_data = (src_data * CM_IN_FOOT).astype(rasterio.float64)
                dst.write_band(1, dst_data, window=window)

!gdalinfo $uncompressed

import tinys3
conn = tinys3.Connection('S3_ACCESS_KEY','S3_SECRET_KEY',tls=True)
f = open('./'+str(uncompressed),'rb')
conn.upload('./'+str(uncompressed),f,'my_bucket')

# Execution
outFile ='chirps-v2.0.2017.03.31.tif'
file = dataDownload()
netcdf2tif(file,outFile)
s3Upload(outFile)