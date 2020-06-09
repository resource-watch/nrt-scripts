## Minimum Annual Arctic Sea Ice Extent Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Arctic Sea Ice Minimum dataset](https://climate.nasa.gov/vital-signs/ice-sheets/) for [display on Resource Watch](https://resourcewatch.org/data/explore/782b2e43-f492-4cea-a195-6635148a3c1b).

This dataset was provided by the source as a text file. This text file was transformed into a table so that it could be uploaded to Carto. In order to transform the data from the text file to the data table used by Resource Watch, the following changes were made:
- Sea ice extent and area were obtained from the 'extent' and 'area' columns respectively. The unit for these variables were converted from million square kilometer to square kilometer. 
- Year and month of each records were gathered from the columns 'year' and 'mo'. Datetime was constructed using year, month and first day of the month as day.
- Date of each record was used as unique ID.

The data shown on Resource Watch can be found in the 'extent' column of the text file. 

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cli_043_arctic_ice/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run monthly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid), and is currently maintained by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid).
