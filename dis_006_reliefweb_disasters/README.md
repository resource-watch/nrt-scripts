## Current Disaster Events Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [ReliefWeb Disasters dataset](https://reliefweb.int/disasters) for [display on Resource Watch](https://resourcewatch.org/data/explore/dis006-ReliefWeb-Disasaters).

This dataset was provided by the source as a JSON file. This JSON was transformed into a table so that it could be uploaded to Carto. In order to transform the data from the JSON to the data table used by Resource Watch, the following changes were made:
- A unique ID for each event was created using primary event id and country id. This was stored in a new column called 'uid'.
- The 'lat' and 'lon' fields were used to construct the point geometry shown on Resource Watch.
- Along with the regular table, an additional 'interaction' table was created. This table was created because, at any given time, a country may be experiencing multiple disaster events. The 'interaction' on the Resource Watch map is only able to display one row of data for a given geometry. In order to display information about multiple ongoing disaster events in one country, information from each event had to be combined into a single row of data. All current event names and links to more information about each event were combined into a single field for each country, which is what is shown on the Resource Watch map.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/dis_006_reliefweb_disasters/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
