## Oil and Chemical Spill Incidents Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Raw Incident Data dataset](https://incidentnews.noaa.gov/) for [display on Resource Watch](https://resourcewatch.org/data/explore/US-Oil-and-Chemical-Spills).

This dataset was provided by the source as a CSV file. In order to transform the data from the CSV to the data table used by Resource Watch, the following changes were made:
- The latitude and longitude from the columns 'lat', 'lon' were used to create the geometry shown on Resource Watch.
- A unique ID for each event was created based on the 'id' column of the CSV. This was stored in a new column called 'uid'.
- The source CSV has some entries with breaks in the last column, which are interpreted as a new row while processing through csv Python library. These breaks were identified and removed.

The data shown on Resource Watch can be found in the 'threat' column of the csv file. 

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/ene_008_oil_chemical_spill_incidents/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
