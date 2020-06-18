## Sea Level Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Sea Level dataset](https://climate.nasa.gov/vital-signs/sea-level/) for [display on Resource Watch](https://resourcewatch.org/data/explore/f655d9b2-ea32-4753-9556-182fc6d3156b).

This dataset was provided by the source as a text file. Decimal dates included in the file were converted to datetime objects, and the resulting table was uploaded to Carto.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cli_040_mean_sea_level_rise/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid), and is currently maintained by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid).
