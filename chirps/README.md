## Standardised Precipitation and Evapotranspiration index

The `spei_nc2tif.py` script downloads a NetCDF file from a server to the local folder, subsets a time slice, and uses it to create a geotif file in the local folder.


The data is a [Standardised precipitation and evapotranspiration index](http://sac.csic.es/spei/index.html). This is a multi-scalar drought index, (a measure of water deficit).
Negative values indicate drought conditions, while positive values indicate flood
conditions. Note that there are multiple versions of these data, with a different monthly binning, which relates to the timescale of the drought/flooding effects (and consequently the functional impact of the water deficit). I.e. A low monthly
binning will show high-frequency water deficit conditions that impact more the soil water content, or headwater discharge of river areas, medium time scales are related to reservoir storages and discharge in the medium course of the rivers, and long time-scales are related to variations in groundwater storage.

The monthly binning is  critical. A new (250mb) file will be downloaded for
each monthly binning requested.

The specific source of the netcdf is [http://notos.eead.csic.es/spei](http://notos.eead.csic.es/spei). These data are
updated monthly. Each time we wish to extract the most recent update to these
data we must download the entire dataset from the server (approximately 250mb).
