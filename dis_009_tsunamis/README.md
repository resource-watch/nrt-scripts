## Tsunamis Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [NGDC/WDS Global Historical Tsunami Database dataset](https://ngdc.noaa.gov/hazard/tsu_db.shtml) for [display on Resource Watch](https://resourcewatch.org/data/explore/dis009nrt-Tsunamis).

This dataset was provided by the source as a text file. A pandas dataframe was created using the contents of the text file. All the empty entries were replaced with NaN. The LATITUDE and LONGITUDE columns were used to construct a geometry field. The resulting table was then uploaded to Carto.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/dis_009_tsunamis/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run every two days. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
