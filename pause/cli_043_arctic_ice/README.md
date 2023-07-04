## Minimum Annual Arctic Sea Ice Extent Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Arctic Sea Ice Minimum dataset](https://climate.nasa.gov/vital-signs/arctic-sea-ice/) for [display on Resource Watch](https://resourcewatch.org/data/explore/782b2e43-f492-4cea-a195-6635148a3c1b).

This dataset was provided by the source as an excel file. This excel file was transformed into a table so that it could be uploaded to Carto. In order to transform the data from the excel file to the data table used by Resource Watch, the following changes were made:
- The units on the sea ice extent and area columns were converted from millions of square kilometers to square kilometers.
- The 'date' column was constructed using the 'year' and 'month' columns. The day was assumed to be the first of the month when constructing this date.

The data shown on Resource Watch can be found in the 'extent' column of the excel file.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cli_043_arctic_ice/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run monthly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid), and is currently maintained by [Weiqi Zhou](https://www.wri.org/profile/weiqi-zhou).
