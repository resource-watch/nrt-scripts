## Smoke Plumes (North America) Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Hazard Mapping System (HMS) Fire and Smoke Analysis dataset](https://www.ospo.noaa.gov/Products/land/hms.html) for [display on Resource Watch](https://resourcewatch.org/data/explore/US-Smoke-Plumes_1).

{Describe how the original data came from the source.}

{Describe the steps used to process the data, e.g., "convert variable X from the original netcdf file to a tif to upload to Google Earth Engine."}

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cli_037_smoke_plumes_US/contents/src/__init__.py) for more details on this processing.

You can view the processed Smoke Plumes (North America) dataset [on Resource Watch](https://resourcewatch.org/data/explore/US-Smoke-Plumes_1).

**Schedule**

This script is run twice daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
