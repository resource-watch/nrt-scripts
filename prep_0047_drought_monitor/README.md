## U.S. Drought Monitor Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [United States Drought Monitor dataset](https://droughtmonitor.unl.edu/Data/GISData.aspx) for [display on PREPdata](https://prepdata.org/dataset/PREP_0047-US-Drought-Monitor).

This dataset was provided by the source as zipped shapefiles. From the zipped file, shpafiles were collected and processed for upload. The shapefiles were read as GeoJSONs using Fiona library. The GeoJSONs was transformed into a table so that it could be uploaded to Carto.

The data shown on PREPdata can be found in the 'DM' variable of the 'properties' feature of the GeoJSON.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/prep_0047_drought_monitor/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
