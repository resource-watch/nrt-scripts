## Migrant Deaths Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Missing Migrants - Tracking Deaths Along Migratory Routes dataset](https://missingmigrants.iom.int/) for [display on Resource Watch](https://resourcewatch.org/data/explore/Missing-Migrants).

This dataset was provided by the source as a csv file. The data shown on Resource Watch can be found in the 'Number Dead' column of the csv file. The spaces in the column names were replaced with underscores to match the column names in Carto table. 
The resulting table was then uploaded to Carto.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/soc_018_migrant_deaths/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
