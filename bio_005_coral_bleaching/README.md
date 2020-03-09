## Coral Bleaching Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [National Oceanic and Atmospheric Administration (NOAA) Coral Reef Watch Bleaching Alerts dataset](https://coralreefwatch.noaa.gov/satellite/bleaching5km/index.php) for [display on Resource Watch](https://resourcewatch.org/data/explore/bio005-Coral-Reef-Bleaching-Alerts).

This dataset was provided by the source as a netcdf file. The data shown on Resource Watch can be found in the 'bleaching_alert_area' variable of the netcdf file. This variable was converted to a tif file so that it could be uploaded to Google Earth Engine.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/bio_005_coral_bleaching/contents/src/__init__.py) for more details on this processing.

You can view the processed Coral Bleaching dataset [on Resource Watch](https://resourcewatch.org/data/explore/bio005-Coral-Reef-Bleaching-Alerts).

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Francis Gassert](https://www.wri.org/profile/francis-gassert), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).