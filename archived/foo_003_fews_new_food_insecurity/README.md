## Current and Projected Food Insecurity Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Famine Early Warning Systems Network (FEWS NET) Food Security Classification dataset](http://www.fews.net/fews-data/333) for [display on Resource Watch](https://resourcewatch.org/data/explore/foo003nrt-Food-Insecurity_replacement).

This dataset was provided by the source as zipped shapefiles. From the zipped file, shpafiles that contained the words 'CS', 'ML1' and 'ML2' were collected and processed for upload. The shapefiles were read as GeoJSONs using Fiona library. The GeoJSONs was transformed into a table so that it could be uploaded to Carto. In order to transform the data from the GeoJSON to the data table used by Resource Watch, the following changes were made:
- The geometry features from the GeoJSON were used to create the geometry shown on Resource Watch. The geometries were simplified using Shapely library before upload.
- A unique ID for each event was created based on the date, region, time period and feature index in retrieved GeoJSON. This was stored in a new column called '_uid'.

The data shown on Resource Watch can be found in the ifc_type variable of the 'properties' feature of the GeoJSON.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/foo_003_fews_new_food_insecurity/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

This script has been archived since national data has been updated more frequently than regional data.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
