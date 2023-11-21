## Standardised Precipitation-Evapotranspiration Index Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Standardized Precipitation Evapotranspiration Index dataset](http://spei.csic.es/home.html) for [display on Resource Watch](https://resourcewatch.org/data/explore/cli039nrt-SPEI_replacement).

This dataset was provided by the source as a netcdf file. The data shown on Resource Watch can be found in the 'spei' variable of the netcdf file. This variable was converted to a tif file so that it could be uploaded to Google Earth Engine.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cli_039_spei/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run weekly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
