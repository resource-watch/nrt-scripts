## Flood Events Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Global Active Archive of Large Flood Events dataset](http://floodobservatory.colorado.edu/) for [display on Resource Watch](https://resourcewatch.org/data/explore/Current-Floods).

This dataset was provided by the source as tab-delimited file and zipped shapefile. The tab-delimited file was processed to get the point data for flood events and the zipped shapefile was processed to get the area of the flood events. Both files were read as GeoJSONs using Fiona library. The GeoJSONs were transformed into table and then uploaded to Carto as separate tables.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/wat_040_flood_events/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Francis Gassert](https://www.wri.org/profile/francis-gassert), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
