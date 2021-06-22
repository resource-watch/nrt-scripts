## Conflict and Protest Events Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Armed Conflict Location & Event Data dataset](https://www.acleddata.com/data/) for [display on Resource Watch](https://resourcewatch.org/data/explore/soc_016-African-and-Asian-Conflict-and-Protest-Events).

This dataset was provided by the source as a JSON file. This JSON was transformed into a table so that it could be uploaded to Carto. In order to transform the data from the JSON to the data table used by Resource Watch, the following changes were made:
- The 'latitude' and 'longitude' variables from the 'data' feature of the JSON were used to create the geometry shown on Resource Watch.
- Date of the events were obtained from the 'event_date' feature of the JSON.
- A unique ID for each event was created using the 'data_id' variable of the 'data' feature of the JSON. This was stored in a new column called 'data_id'.

The data shown on Resource Watch can be found in the 'event_type' variable of the 'data' feature of the JSON.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/soc_016_conflict_protest_events/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run weekly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

This script has been archived since we are no longer allowed by the data provider to display raw data on Resource Watch.

###### Note: This script was originally written by [Francis Gassert](https://www.wri.org/profile/francis-gassert), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
