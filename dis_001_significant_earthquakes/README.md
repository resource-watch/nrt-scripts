## Earthquakes Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Earthquake Catalog dataset](https://earthquake.usgs.gov/earthquakes/) for [display on Resource Watch](https://resourcewatch.org/data/explore/dis001nrt-Significant-Earthquakes-over-the-past-30-Days).

This dataset was provided by the source as a geojson file. Minimum significance of the earthquake was set to 0 to include all earthquake events. The data shown on Resource Watch can be found in the 'mag' variable of the 'properties' feature of the geojson. Datetime was retrieved from the 'time' variable in the 'properties' feature and was divided by 1000. The resulting table was uploaded to Carto.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/dis_001_significant_earthquakes/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run hourly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
