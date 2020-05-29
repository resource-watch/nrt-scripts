## Landslide Hazard Alerts Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Global Landslide Hazard Assessment for Situational Awareness Alerts dataset](https://pmm.nasa.gov/applications/global-landslide-model) for [display on Resource Watch](https://resourcewatch.org/data/explore/dis012nrt-Landslide-Hazard-Alerts).

This dataset was provided by the source as a GeoJSON file. This GeoJSON was transformed into a table so that it could be uploaded to Carto. In order to transform the data from the GeoJSON to the data table used by Resource Watch, the following changes were made:
- Datetime was obtained from the 'date' variable which was within the 'properties' variable of 'items' feature of the GeoJSON.
- A unique ID for each event was created based on the date and the index of the date in GeoJSON. This was stored in a new column called '_UID'.
- The value for all other fields were obtained using a number of steps. First, the fifth element of the 'action' variable in the 'items' feature of the GeoJSON was accessed, from this the 'url' variable in the first position of the 'using' variable was used to open up a new GeoJSON containing information about each landslide hazard Alert. 
- The latitude and longitude in the GeoJSON's geometry were used to create the geometry shown on Resource Watch.

The data shown on Resource Watch can be found in the 'nowcast' variable from the 'properties' feature of the GeoJSON.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/dis_012_landslide_hazard_alerts/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run every three hours. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
