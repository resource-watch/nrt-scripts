## Vegetation Health Index Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [AVHRR Vegetation Health Product - Vegetation Health Index dataset](https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/index.php) for [display on Resource Watch](https://resourcewatch.org/data/explore/foo024nrt-Vegetation-Health-Index_replacement_4).

{Describe how the original data came from the source.}

{Describe the steps used to process the data, e.g., "convert variable X from the original netcdf file to a tif to upload to Google Earth Engine."}

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/foo_024_051_054_vegetation_health_products/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run weekly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
