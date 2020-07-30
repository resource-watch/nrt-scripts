## Tropical Cyclones Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [International Best Track Archive for Climate Stewardship (IBTrACS) Project, Version 4 dataset](https://www.ncdc.noaa.gov/ibtracs/) for [display on Resource Watch](https://resourcewatch.org/data/explore/dis015a-Hurricane-Tracks).

This dataset was provided by the source as zipped shapefiles. The shapefiles were read as a dataframe using the GeoPandas library. Columns that were completely empty were removed from the dataframe. There were invalid geometries (self-intersection) in the source data, which prevented them from being correctly interpreted by Carto. So, the line geometries representing cyclone tracks were buffered by a very small distance to fix the issue. The resulting dataframe was then uploaded to Carto.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/dis_015a_tropical_cyclones/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run bi-weekly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid), and is currently maintained by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid).
