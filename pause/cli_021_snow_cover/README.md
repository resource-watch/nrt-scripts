## Snow Cover Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [MODIS/Terra Snow Cover Monthly L3 Global 0.05 Degree Climate Modeling Grid Version 6 dataset](https://nsidc.org/data/mod10cm) for [display on Resource Watch](https://resourcewatch.org/data/explore/cli021nrt_snow_cover_temp).

This dataset was provided by the source as a hdf file. The data shown on Resource Watch can be found in the 'MOD_CMG_Snow_5km:Snow_Cover_Monthly_CMG' variable of the hdf file. This variable was converted to a tif file so that it could be uploaded to Google Earth Engine.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cli_021_snow_cover/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run first nine days of every month. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Liz Saccoccia](https://www.wri.org/profile/liz-saccoccia), and is currently maintained by [Liz Saccoccia](https://www.wri.org/profile/liz-saccoccia).
