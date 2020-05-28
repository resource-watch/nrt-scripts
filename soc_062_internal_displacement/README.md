## Annual Internal Displacement from Natural Disasters Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Raw Incident Data dataset](https://incidentnews.noaa.gov/) for [display on Resource Watch](https://resourcewatch.org/data/explore/soc062c-Internal-Displacement-NRT).

This dataset was provided by the source as a json file. The data shown on Resource Watch can be found in the 'displacement_type' column of the json file. Datetime was constructed by retrieving year from the 'year' feature and month, day from the 'displacement_date' feature of the json. The resulting table was then uploaded to Carto.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/soc_062_internal_displacement/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
