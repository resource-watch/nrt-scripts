## Smoke Plumes (North America) Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Hazard Mapping System (HMS) Fire and Smoke Analysis dataset](https://www.ospo.noaa.gov/Products/land/hms.html) for [display on Resource Watch](https://resourcewatch.org/data/explore/US-Smoke-Plumes_1).

This dataset was provided by the source as zipped shapefiles. Different urls were used to access these zip files based on the date of the smoke plume events. The shapefiles were read as GeoJSONs using Fiona library. The GeoJSONs was transformed into a table so that it could be uploaded to Carto. In order to transform the data from the GeoJSON to the data table used by Resource Watch, the following changes were made:
- The geometry features from the GeoJSON were used to create the geometry shown on Resource Watch. 
- A unique ID for each event was created based on the date and feature index in retrieved GeoJSON. This was stored in a new column called '_UID'.
- Start and end dates were obtained from the 'Start' and 'End' variables of the 'properties' feature of the GeoJSON and were reformatted to get the duration of the events.

The data shown on Resource Watch can be found in the 'Density' variable of the 'properties' feature of the GeoJSON.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cli_037_smoke_plumes_US/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run twice daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
