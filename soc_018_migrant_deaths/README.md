## Migrant Deaths Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Missing Migrants - Tracking Deaths Along Migratory Routes dataset](https://missingmigrants.iom.int/) for [display on Resource Watch](https://resourcewatch.org/data/explore/Missing-Migrants).

This dataset was provided by the source as an annual Excel file (one Excel file for each year of data). In order to transform the data from the original Excel file to the data table used by Resource Watch, the following changes were made:
- The Excel file was converted to CSV file locally.
- The spaces in the column names were replaced with underscores to match the column names in Carto table.
- The latitude and longitude from the column 'Coordinates' were used to create the geometry shown on Resource Watch.
- The datetime of each event was obtained from the column 'Incident_Date'.
- A unique ID for each event was created based on the 'URL' column of the CSV. This was stored in a new column called 'uid'.

The data shown on Resource Watch can be found in the 'Number Dead' column of the csv file. 

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/soc_018_migrant_deaths/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Weiqi Zhou](https://wri.org.cn/en/profile/weiqi-zhou).