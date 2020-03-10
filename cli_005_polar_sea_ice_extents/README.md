## Arctic/Antarctic Sea Ice Extent Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Sea Ice Index, Version 3 dataset](http://nsidc.org/data/g02135) to display monthly sea ice extents in the
* [Arctic](https://resourcewatch.org/data/explore/cli_005b_Arctic-Sea-Ice)
* [Antarctic](https://resourcewatch.org/data/explore/cli_005a_Antarctic-Sea-Ice)

on Resource Watch.

{Describe how the original data came from the source.}

{Describe the steps used to process the data, e.g., "convert variable X from the original netcdf file to a tif to upload to Google Earth Engine."}

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cli_005_polar_sea_ice_extents/contents/src/__init__.py) for more details on this processing.

You can view the processed 
* Arctic Sea Ice Extent dataset [on Resource Watch](https://resourcewatch.org/data/explore/cli_005b_Arctic-Sea-Ice).
* Antarctic Sea Ice Extent dataset [on Resource Watch](https://resourcewatch.org/data/explore/cli_005a_Antarctic-Sea-Ice).

**Schedule**

This script is run weekly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
