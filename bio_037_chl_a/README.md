## Chlorophyll Concentration Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [MODIS Aqua Monthly Global Chlorophyll-a Near Surface Concentration](https://oceancolor.gsfc.nasa.gov/atbd/chlor_a/) for [display on Resource Watch](https://resourcewatch.org/data/explore/bio037-Chlorophyll-a-2).

{Describe how the original data came from the source.}

{Describe the steps used to process the data, e.g., "convert variable X from the original netcdf file to a tif to upload to Google Earth Engine."}

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/bio_037_chl_a/contents/src/__init__.py) for more details on this processing.

You can view the processed Chlorophyll Concentration dataset [on Resource Watch](https://resourcewatch.org/data/explore/bio037-Chlorophyll-a-2).

**Schedule**

This script is run once every month. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Kristine Lister](https://www.wri.org/profile/kristine-lister), and is currently maintained by [Kristine Lister](https://www.wri.org/profile/kristine-lister).
