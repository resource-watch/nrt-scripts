## Volcano Eruptions Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Volcanoes of the World Database - Eruptions dataset](http://volcano.si.edu/search_eruption.cfm) for [display on Resource Watch](https://resourcewatch.org/data/explore/dis013-Volcanoes-CSV).

This dataset was provided by the source as a json file. The data shown on Resource Watch can be found in the '{volcano.si.edu}Smithsonian_VOTW_Holocene_Eruptions' feature of the json file. This feature was queried to get the values of each variable shown on Resource Watch by using the column names in our Carto table. The only exception was the geometry column. The geometry of the volcano eruptions were obtained from the '{volcano.si.edu}GeoLocation' variable of '{volcano.si.edu}Smithsonian_VOTW_Holocene_Eruptions' feature. The resulting table was uploaded to Carto.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/dis_013_volcano_eruptions/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
