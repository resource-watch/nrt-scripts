## Landslide Hazard Alerts Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Global Landslide Hazard Assessment for Situational Awareness Alerts dataset](https://pmm.nasa.gov/applications/global-landslide-model) for [display on Resource Watch](https://resourcewatch.org/data/explore/dis012nrt-Landslide-Hazard-Alerts).

{Describe how the original data came from the source.}

{Describe the steps used to process the data, e.g., "convert variable X from the original netcdf file to a tif to upload to Google Earth Engine."}

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/dis_012_landslide_hazard_alerts/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run every three hours. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
