## Land Ice: Greenland Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Greenland Mass Variation dataset](https://climate.nasa.gov/vital-signs/ice-sheets/) for [display on Resource Watch](https://resourcewatch.org/data/explore/095eee4a-ff4e-4c58-9110-85a9e42ed6f5).

This dataset was provided by the source as a text file. The data shown on Resource Watch can be found in the 'Greenland mass (Gigatonnes)' column of the text file. Decimal dates included in the file were converted to datetime objects, and the resulting table was uploaded to Carto.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cli_042_greenland_ice/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](https://www.wri.org/profile/nathan-suberi), and is currently maintained by [Weiqi Zhou](https://wri.org.cn/en/profile/weiqi-zhou).
