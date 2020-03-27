## Organized Violence Events Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [UCDP Georeferenced Event Dataset (GED) Global version 18.1 (2017) dataset](https://ucdp.uu.se/downloads/ged/ged181.pdf) for [display on Resource Watch](https://resourcewatch.org/data/explore/Organized-Violence-Events_1).

{Describe how the original data came from the source.}

{Describe the steps used to process the data, e.g., "convert variable X from the original netcdf file to a tif to upload to Google Earth Engine."}

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/soc_048_organized_violence_events/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run monthly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
