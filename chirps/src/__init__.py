

'''
THIS IS BEING PRESERVED, BUT NOT IMPLEMENTED AT THIS POINT
RW WILL USE THE CHIRPS DATA ON GEE FOR NOW
'''

import os
import glob
import gzip
import shutil
import rasterio
import numpy as np
import pandas as pd
from os import listdir
from netCDF4 import Dataset
from contextlib import closing
from os.path import isfile, join
from os.path import basename, dirname, exists

source = os.getcwd()
destination = source

listing = []
response = urllib2.urlopen('ftp://chg-ftpout.geog.ucsb.edu/pub/org/chg/products/CHIRPS-2.0/global_daily/tifs/p05/2017/')
for line in response:
    listing.append(line.rstrip())


s2=pd.DataFrame(listing)
s3=s2[0].str.split()
s4=s3[len(s3)-1]
last_file = s4[8]
print 'The last file (compress) is: ',last_file

uncompressed = os.path.splitext(last_file)[0]

print 'The last file UNCOMPRESSED is: ',uncompressed
print

with closing(urllib2.urlopen('ftp://chg-ftpout.geog.ucsb.edu/pub/org/chg/products/CHIRPS-2.0/global_daily/tifs/p05/2017/'+last_file)) as r:
    with open(str(last_file), 'wb') as f:
        shutil.copyfileobj(r, f)

#listing files in my directory
onlyfiles = [f for f in listdir(source) if isfile(join(source, f))]
print onlyfiles

#uncompress file

archives = [x for x in os.listdir(source) if '.gz' in x]

for archive in archives:
    archive = os.path.join(source, archive)
    dest = os.path.join(destination, os.path.splitext(archive)[0])

    with gzip.open(archive, "rb") as zip:
        with open(dest, "w") as out:
            for line in zip:
                out.write(line)

uncompressed = os.path.splitext(last_file)[0]

with rasterio.open(uncompressed) as src:
    npixels = src.width * src.height
    for i in src.indexes:
        band = src.read(i)
        print(i, band.min(), band.max(), band.sum()/npixels)

CM_IN_FOOT = 30.48

with rasterio.open(source+'/'+uncompressed) as src:
    kwargs = src.meta
    kwargs.update(
        driver='GTiff',
        dtype=rasterio.float64,  #rasterio.int16, rasterio.int32, rasterio.uint8,rasterio.uint16, rasterio.uint32, rasterio.float32, rasterio.float64
        count=1,
        compress='lzw',
        nodata=0,
        bigtiff='YES'
    )

    windows = src.block_windows(1)

    with rasterio.open((source+'/'+uncompressed),'w',**kwargs) as dst:
        for idx, window in windows:
            src_data = src.read(1, window=window)

            # Source nodata value is a very small negative number
            # Converting in to zero for the output raster
            np.putmask(src_data, src_data < 0, 0)

            dst_data = (src_data * CM_IN_FOOT).astype(rasterio.float64)
            dst.write_band(1, dst_data, window=window)

import tinys3

conn = tinys3.Connection(os.getenv('S3_ACCESS_KEY'),os.getenv('S3_SECRET_KEY'),tls=True)

f = open((source+'/'+uncompressed),'rb')
conn.upload((uncompressed),f,os.getenv('BUCKET'))

# Execution
outFile ='chirps-v2.0.2017.03.31.tif'
file = dataDownload()
netcdf2tif(file,outFile)
s3Upload(outFile)
