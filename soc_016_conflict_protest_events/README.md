## Conflict and Protest Events Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Armed Conflict Location & Event Data dataset](https://www.acleddata.com/data/) for [display on Resource Watch](https://resourcewatch.org/data/explore/soc_016-African-and-Asian-Conflict-and-Protest-Events).

{Describe how the original data came from the source.}

{Describe the steps used to process the data, e.g., "convert variable X from the original netcdf file to a tif to upload to Google Earth Engine."}

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/soc_016_conflict_protest_events/contents/src/__init__.py) for more details on this processing.

You can view the processed Conflict and Protest Events dataset [on Resource Watch](https://resourcewatch.org/data/explore/soc_016-African-and-Asian-Conflict-and-Protest-Events).

**Schedule**

This script is run weekly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Francis Gassert](https://www.wri.org/profile/francis-gassert), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
