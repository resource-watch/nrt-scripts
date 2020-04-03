## Combined Sea Surface and Air Temperature Anomalies Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [GISS Surface Temperature Analysis (GISTEMP) dataset](https://www.esrl.noaa.gov/psd/data/gridded/data.gistemp.html) for [display on Resource Watch](https://resourcewatch.org/data/explore/cli_035_Surface-Temperature-Anomalies).

This dataset was provided by the source as a netcdf file. The data shown on Resource Watch can be found in the 'spei' variable of the netcdf file. This variable was converted to a tif file so that it could be uploaded to Google Earth Engine.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cli_035_surface_temp_analysis/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
