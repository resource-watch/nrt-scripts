## Land Ice: Antarctica Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Antarctica Mass Variation dataset](https://climate.nasa.gov/vital-signs/ice-sheets/) for [display on Resource Watch](https://resourcewatch.org/data/explore/0570f6d0-b34b-4bb3-bd93-46644a078996).

This dataset was provided by the source as a text file. The data shown on Resource Watch can be found in the 'Antarctic mass (Gigatonnes)' column of the text file. Decimal dates included in the file were converted to datetime objects, and the resulting table was uploaded to Carto.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cli_041_antarctica_ice/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid), and is currently maintained by [Weiqi Zhou](https://wri.org.cn/en/profile/weiqi-zhou).
