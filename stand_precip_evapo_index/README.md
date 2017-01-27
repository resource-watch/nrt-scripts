## Standardised Precipitation and Evapotranspiration index

The `spei_nc2tif.py` script downloads a NetCDF file from a server to the local folder, subsets a time slice, and uses it to create a geotif file in the local folder.

Users execute the program with the command-line python interface as demonstrated below, passing a required argument (`monthly_bin`, explained below). An optional keyword argument `time_slice` can also be given if a user wants to change which month the map relates to (by default the latest time-step is returned).

The data is a [Standardised precipitation and evapotranspiration index](http://sac.csic.es/spei/index.html). This is a multi-scalar drought index, (a measure of water deficit).
Negative values indicate drought conditions, while positive values indicate flood
conditions. Note that there are multiple versions of these data, with a different monthly binning, which relates to the timescale of the drought/flooding effects (and consequently the functional impact of the water deficit). I.e. A low monthly
binning will show high-frequency water deficit conditions that impact more the soil water content, or headwater discharge of river areas, medium time scales are related to reservoir storages and discharge in the medium course of the rivers, and long time-scales are related to variations in groundwater storage.

The monthly binning is therefore critical, and must be explicitly requested
by users of the script as an argument. A new (250mb) file will be downloaded for
each monthly binning requested. However, if a user wants to make multiple maps
from the same monthly binning, these data are downloaded only once, and this file
can then be used repeatedly.

The specific source of the netcdf is [http://notos.eead.csic.es/spei](http://notos.eead.csic.es/spei). These data are
updated monthly. Each time we wish to extract the most recent update to these
data we must download the entire dataset from the server (approximately 250mb).

The (automated) naming convention of the created tif file indicates the origin data, and the date that the data relates to: e.g. if 1 month binned data is requested
for the latest month, a file such as `spei01_2016-12-01.tif` will be created.


First install the Python requirements:

``
pip install -r requirements.txt
``

Then run the Python script, providing the monthly_bin argument desired:

E.g. To extract the most recent 1-month SPEI map:

``
python spei_nc2tif.py 1
``

To extract the most-recent, 6-month binned SPEI map.

``
python  spei_nc2tif.py 6
``

Or, to extract the 6-month binned SPEI map, from 2 months ago:

``
python spei_nc2tif.py 6 --time_slice=-2
``
