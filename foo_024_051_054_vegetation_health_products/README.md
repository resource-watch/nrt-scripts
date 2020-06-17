## Vegetation Health Index/Vegetation Condition Index Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [AVHRR Vegetation Health Product - Vegetation Health Index dataset](https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/index.php) for display on Resource Watch as the following datasets:
* [Vegetation Health Index](https://resourcewatch.org/data/explore/c12446ce-174f-4ffb-b2f7-77ecb0116aba).
* [Vegetation Condition Index](https://resourcewatch.org/data/explore/2447d765-dc04-4e4a-aeaa-904760e94991).

This dataset is provided by the source as netcdf files, with one file for each week. Inside each netcdf file, the vegetation health index data can be found in the 'VHI' variable of the netcdf file and the vegetation condition index data in 'VCI'.

Data for each of these netcdf variables is converted to tif files and uploaded to Google Earth Engine.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/foo_024_051_054_vegetation_health_products/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run weekly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](https://www.wri.org/profile/nathan-suberi), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
