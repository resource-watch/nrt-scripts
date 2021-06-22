## Conflict and Protest Events Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Armed Conflict Location & Event Data dataset](https://www.acleddata.com/data/) for [display on Resource Watch](https://resourcewatch.org/data/explore/soc_016-African-and-Asian-Conflict-and-Protest-Events).

This dataset was provided by the source as a JSON file. This JSON was transformed into a geopandas dataframe whose geometry was created from the 'latitude' and 'longitude' variables from the 'data' feature of the JSON. The dataframe was spatially joined to the admin-2 boundaries from [GADM](https://gadm.org/index.html) to calculate the number of events in each admin-2 region.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/soc_016_conflict_protest_events/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run weekly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Francis Gassert](https://www.wri.org/profile/francis-gassert), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
