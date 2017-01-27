
import argparse
import wget
import os.path
import pandas as pd
from netCDF4 import Dataset
import numpy as np
import rasterio
import tinys3
from rasterio.transform import from_origin

class bcolors:
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    ENDC = '\033[0m'
    UNDERLINE = '\033[4m'
# --- CREATE A COMMAND LINE PARSER ---
parser = argparse.ArgumentParser(description='Downloads recent SPDI data (~250m'
                                 'b), then extracts a specified timeslice, '
                                 'saves it as a geotiff, and uploads it to S3.')
parser.add_argument('month_bin', type=int,
                    help='an integer from 1 to 48, specifying the monthly'
                    'binning desired in the SPDI index. (Lower = higher'
                    'frequency drought info.)')
parser.add_argument('--time_slice', default=-1,
                    help='Specify the time step to save from the array, using '
                    'negative counting, e.g. so that -2 means the second-from-'
                    'last time-step. By default the last time-step is '
                    'returned.')

args = parser.parse_args()
if args.month_bin < 1 or args.month_bin > 48:
    raise ValueError("Month_bin argument must be between 1 to 48")


#  --- SET ARGUMENTS BASED ON COMMAND LINE INPUTS ---
time_slice = args.time_slice   # newest--> oldest. e.g. -1 = newest time
month_size = "{0:02d}".format(args.month_bin)  # month bin property for url
var = 'spei'


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

time_index, z_index = time_and_Z_index(time_slice)

# --- DOWNLOAD DATA IF NOT IN LOCAL FOLDER ---
download_url = "http://notos.eead.csic.es/spei/nc/spei" + month_size + ".nc"
fname = download_url.split('/')[-1]
if not os.path.isfile(fname):
    print("File {0} not found in local directory".format(fname))
    # DOWNLOAD THE FILE Now with wget...
    wget.download(download_url)
else:
    pass
    # print("File {0} found".format(fname))

nc = Dataset(fname)   # Connect to the downloaded dataset
Z = nc.variables[var][z_index, :, :].squeeze()  # Extract time-step of array

missing = Z.data == nc['spei']._FillValue  # Identify missing values
Z.data[missing] = -99       # Replace them with a smaller, distinct value

# N.b. If you wanted, you can preview the array with matplotlib imshow below
# plt.imshow(np.flipud(Z))
# plt.show()

# print(nc.variables)  # Show metadata
startdate = nc.variables['time'].units.split()[-2]
days_since_start = int(nc.variables['time'][time_index])
enddate = pd.to_datetime(startdate) + pd.DateOffset(days=days_since_start)
# print(" \r Extracting data for: {0}".format(enddate.date()))

# Return lat info
south_lat = nc['lat'][-1] + 0.25  # Change pos. to edges of pixels (not center)
north_lat = nc['lat'][0] - 0.25
num_lats = len(nc['lat'])
# print(north_lat, south_lat, num_lats)

# Return lon info
west_lon = nc['lon'][0] - 0.25
east_lon = nc['lon'][-1] + 0.25
num_lons = len(nc['lon'])
# print(west_lon, east_lon, num_lons)

# Rasterio needs to transform the data
x = np.linspace(west_lon, east_lon, num_lons)
y = np.linspace(south_lat, north_lat, num_lats)
X, Y = np.meshgrid(x, y)

transform = rasterio.transform.from_bounds(west_lon, south_lat, east_lon,
                                           north_lat, Z.shape[1], Z.shape[0])

# Create new dataset file object, save the array to it, and close the connection
write_name = "spei.tif"
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

# Add code here to push to S3 instance
conn = tinys3.Connection(os.getenv('S3_ACCESS_KEY'),os.getenv('S3_SECRET_KEY'), tls=True)

# So we could skip the bucket parameter on every request

f = open(write_name,'rb')
response = conn.upload(write_name,f,os.getenv('BUCKET'))

if response.status_code==200:
    print('\r '+bcolors.OKGREEN+'SUCCESS'+bcolors.ENDC)
else:
    print(bcolors.WARNING+'UPLOAD PROCESS FAILURE STATUS CODE: ' + str(response.status_code)+bcolors.ENDC)
    print('\r '+str(response.content)


