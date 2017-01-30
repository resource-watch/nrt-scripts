from __future__ import print_function, division
import wget
import os.path
import pandas as pd
from netCDF4 import Dataset
import numpy as np
import rasterio
import tinys3
from rasterio.transform import from_origin


bcolors = {'OKBLUE': '\033[94m',
           'OKGREEN': '\033[92m',
           'WARNING': '\033[93m',
           'ENDC': '\033[0m',
           'UNDERLINE': '\033[4m'}


def time_and_Z_index(time_index):
    """There is a diffrence between the order of the time and Z arrays. The
    time data is organised from oldest--> newest (therefore nc['time'][0]) gives
    the first time step, and nc['time'][-1] gives the last. The data (Z) array
    nc['spei'] is the opposite, the newest data is first, and oldest last. This
    function returns index positions, so a user can use negative counting and
    assume oldest --> newest, and request e.g.: second-from-last position:
    time_index, Z_index = time_and_Z_index(-2)
    """
    if time_index < 0:
        # people want the newest Z data
        data_slice = np.abs(time_index) - 1
    if time_index >= 0:
        # People want the oldest data...
        data_slice = len(nc['time']) - time_index
    return time_index, data_slice


def do_work(time_slice, month_size, write_name):
    """Main function to download a specific netcdf file to the local folder, and extract a slice.
    time_slice = signed integer indicating slice of array to extract
    e.g. where (-1 = last time-step).
    month_size = two digit string of time-binning in drought index (e.g. '01')
    write_name = string, name of tif file to create and upload to S3
    """
    var = 'spei'
    time_index, z_index = time_and_Z_index(time_slice)
    download_url = "http://notos.eead.csic.es/spei/nc/spei" + month_size + ".nc"
    fname = download_url.split('/')[-1]
    if not os.path.isfile(fname):
        print("File {0} not found in local directory".format(fname))
        wget.download(download_url)
    nc = Dataset(fname)   # Connect to the downloaded dataset
    Z = nc.variables[var][z_index, :, :].squeeze()  # Extract time-step of array
    missing = Z.data == nc['spei']._FillValue  # Identify missing values
    Z.data[missing] = -99       # Replace them with a smaller, distinct value
    # print(nc.variables)  # Show metadata
    startdate = nc.variables['time'].units.split()[-2]
    days_since_start = int(nc.variables['time'][time_index])
    enddate = pd.to_datetime(startdate) + pd.DateOffset(days=days_since_start)
    # print(" \r Extracting data for: {0}".format(enddate.date()))
    # Return lat info
    south_lat = nc['lat'][-1] + 0.25  # Change pos.to edges of pxls (not center)
    north_lat = nc['lat'][0] - 0.25
    num_lats = len(nc['lat'])
    # Return lon info
    west_lon = nc['lon'][0] - 0.25
    east_lon = nc['lon'][-1] + 0.25
    num_lons = len(nc['lon'])
    # Rasterio needs to transform the data
    x = np.linspace(west_lon, east_lon, num_lons)
    y = np.linspace(south_lat, north_lat, num_lats)
    X, Y = np.meshgrid(x, y)
    transform = rasterio.transform.from_bounds(west_lon, south_lat, east_lon,
                                               north_lat, Z.shape[1], Z.shape[0])
    # Create new file object, save the array to it, and close the conn.
    new_dataset = rasterio.open(write_name, 'w',
                                driver='GTiff',
                                height=Z.shape[0],
                                width=Z.shape[1],
                                count=1,
                                dtype=Z.dtype,
                                crs='EPSG:4326',
                                transform=transform,
                                compress='lzw',
                                nodata=-99.,
                                bigtiff='NO')
    new_dataset.write(Z, 1)
    new_dataset.close()
    # Push to Amazon S3 instance
    conn = tinys3.Connection(os.getenv('S3_ACCESS_KEY'),
                             os.getenv('S3_SECRET_KEY'), tls=True)
    # So we could skip the bucket parameter on every request
    f = open(write_name, 'rb')
    response = conn.upload(write_name, f, os.getenv('BUCKET'))
    if response.status_code == 200:
        print('\r ' + bcolors['OKGREEN'] + 'SUCCESS' + bcolors['ENDC'])
    else:
        print(bcolors['WARNING'] + 'UPLOAD PROCESS FAILURE STATUS CODE: ' +
              str(response.status_code) + bcolors['ENDC'])
        print('\r ' + str(response.content))
    return


time_slice = -1
month_sizes = ['01', '06']
write_names = ['speiShortTerm.tif', 'speiMidTerm.tif']

for write_name, month_size in zip(write_names, month_sizes):
    do_work(time_slice=time_slice, month_size=month_size, write_name=write_name)
