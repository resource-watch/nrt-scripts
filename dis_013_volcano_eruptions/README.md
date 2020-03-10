## Volcano Eruptions Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Volcanoes of the World Database - Eruptions dataset](http://volcano.si.edu/search_eruption.cfm) for [display on Resource Watch](https://resourcewatch.org/data/explore/dis013-Volcanoes-CSV).

{Describe how the original data came from the source.}

{Describe the steps used to process the data, e.g., "convert variable X from the original netcdf file to a tif to upload to Google Earth Engine."}

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/dis_013_volcano_eruptions/contents/src/__init__.py) for more details on this processing.

You can view the processed Volcano Eruptions dataset [on Resource Watch](https://resourcewatch.org/data/explore/dis013-Volcanoes-CSV).

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
