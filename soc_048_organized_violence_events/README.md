## Organized Violence Events Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [UCDP Georeferenced Event Dataset (GED) Global version 18.1 (2017) dataset](https://ucdp.uu.se/downloads/ged/ged181.pdf) for [display on Resource Watch](https://resourcewatch.org/data/explore/Organized-Violence-Events_1).

This dataset was provided by the source as a JSON file. This JSON was transformed into a table so that it could be uploaded to Carto. In order to transform the data from the JSON to the data table used by Resource Watch, the following changes were made:
- The latitude and longitude variables from the 'Result' feature of the JSON were used to create the geometry shown on Resource Watch.
- Date of the events were obtained from the 'date_start' variable of the 'Result' feature of the JSON. 
- A unique ID for each event was created based on the 'id' variable of the 'Result' feature of the JSON. This was stored in a new column called 'uid'.

The data shown on Resource Watch can be found in the 'best' variable of the 'Result' feature of the JSON.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/soc_048_organized_violence_events/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run monthly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
