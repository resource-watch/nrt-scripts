## Earthquakes Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Earthquake Catalog dataset](https://earthquake.usgs.gov/earthquakes/) for [display on Resource Watch](https://resourcewatch.org/data/explore/dis001nrt-Significant-Earthquakes-over-the-past-30-Days).

This dataset was provided by the source as a GeoJSON file. This GeoJSON was transformed into a table so that it could be uploaded to Carto. In order to transform the data from the GeoJSON to the data table used by Resource Watch, the following changes were made:
- The latitude and longitude in the GeoJSON's geometry were used to create the geometry shown on Resource Watch.
- The depth portion of the GeoJSON's geometry was separated into its own column in the Carto table.
- The data was provided by the source in the units of milliseconds since the Unix Epoch (the 'time' variable in the 'properties' feature). This was converted to a datetime object.
- A unique ID for each event was created based on the latitude, longitude, depth, and time. This was stored in a new column called 'uid'.

The data shown on Resource Watch can be found in the 'mag' variable of the 'properties' feature of the GeoJSON.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/dis_001_significant_earthquakes/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run hourly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
