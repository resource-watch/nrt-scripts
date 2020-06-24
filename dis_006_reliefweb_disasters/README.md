## Current Disaster Events Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [ReliefWeb Disasters dataset](https://reliefweb.int/disasters) for [display on Resource Watch](https://resourcewatch.org/data/explore/dis006-ReliefWeb-Disasaters).

This dataset was provided by the source as a JSON file. This JSON was transformed into a table so that it could be uploaded to Carto. In order to transform the data from the JSON to the data table used by Resource Watch, the following changes were made:
- A unique ID for each event was created using primary event id and country id. This was stored in a new column called 'uid'.
- Along with the regular table, an additional interaction table was created to display multiple ongoing disaster events in a country.
- The information shown on Resource Watch about current disaster events in each country was created using all the event names and the urls to each event description.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/dis_006_reliefweb_disasters/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
