## Tsunamis Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [NGDC/WDS Global Historical Tsunami Database dataset](https://ngdc.noaa.gov/hazard/tsu_db.shtml) for [display on Resource Watch](https://resourcewatch.org/data/explore/dis009nrt-Tsunamis).

{Describe how the original data came from the source.}

{Describe the steps used to process the data, e.g., "convert variable X from the original netcdf file to a tif to upload to Google Earth Engine."}

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/dis_009_tsunamis/contents/src/__init__.py) for more details on this processing.

You can view the processed Tsunamis dataset [on Resource Watch](https://resourcewatch.org/data/explore/dis009nrt-Tsunamis).

**Schedule**

This script is run every two days. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
