from __future__ import print_function, division
import os
from datetime import date, timedelta
import numpy as np
import rasterio
import tinys3
import pydap
from pydap.client import open_url
from pydap.cas.urs import setup_session

# Download last dataset (2 days in the past)

def dataConnection(url): 
    session = setup_session(os.getenv('EARTHDATA_USER'), os.getenv('EARTHDATA_KEY'), check_url=url)
    filename = open_url(url, session=session)
    print('Data connected')
    return filename


# Convert nc to geotiff


def earthdata2tif(dataset,variable,outFile):
    metadata = dataset.attributes['HDF5_GLOBAL']
    Wdata = dataset[variable][0][:][:]
    # Return basic info to be used in the profile
    rows, columns = Wdata[0].shape
    flipped_array = np.flipud(Wdata[0])
    # Return lat - lon info
    north = metadata['SOUTH_WEST_CORNER_LAT'] + (rows * metadata['DY'])
    west = metadata['SOUTH_WEST_CORNER_LON']
    # Transformation function
    trans = rasterio.transform.from_origin(west, north, metadata['DX'], metadata['DY'])
    # Profile
    profile = {'driver': 'GTiff', 
           'dtype': 'float64',
           'compress': 'lzw',
           'nodata': metadata['missing_value'],
           'width': columns,
           'height': rows,
           'count': 1,
           'crs':'EPSG:4326',
           'transform': trans,
           'tiled': False}
    # tif creation
    with rasterio.open(outFile, 'w', **profile) as dst:
        dst.write(flipped_array.astype(profile['dtype']), 1)
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
def main():
    #variables
    outFile ={'SoilMoi0_10cm_inst':'landWaterContent.tif',
              'RootMoist_inst':'rootMoist.tif',
              'Rainf_f_tavg':'rainF.tif'}
    # start point
    today = date.today()
    lastDate = today - timedelta(days=(today.day))
    url = 'https://hydro1.gesdisc.eosdis.nasa.gov/opendap/hyrax/GLDAS/GLDAS_NOAH025_M.2.1/'+ lastDate.strftime("%Y") +'/GLDAS_NOAH025_M.A' + lastDate.strftime("%Y%m") + '.021.nc4' 
    print(url)
    try:
        file = dataConnection(url)
        dataKeys = list(set(file.keys()).intersection(outFile.keys()))
        if len(dataKeys)!=0:
            if len(dataKeys)<len(outFile.keys()):
                print(list(set(outFile.keys()).difference(dataKeys)))
            for indicator in dataKeys:
                earthdata2tif(file,indicator,outFile[indicator])
                s3Upload(outFile[indicator])
        else:
            print('beee')
    except IOError:
        print('cannot open')


main()

