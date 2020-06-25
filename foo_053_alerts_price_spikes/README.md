## Food Price Spikes Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Alert for Price Spikes (ALPS) dataset](https://documents.wfp.org/stellent/groups/public/documents/manual_guide_proced/wfp264186.pdf?_ga=2.155059965.418661181.1556120721-1045976685.1553722904) for [display on Resource Watch](https://resourcewatch.org/data/explore/foo053-Food-Price-Spikes).

This dataset was created by processing two JSON files. One of them contained information about markets in each region and the other contained information about alert for price spikes (alps) for each commodities in each market. These two JSONs were processed to get alert for all food commodities in each market. Two separate tables were created using the processed data so that they could be uploaded to Carto. Along with the regular table, an additional 'interaction' table was created. This table was created because, at any given time, a market may be experiencing alert for price spikes for multiple commodities. The 'interaction' on the Resource Watch map is only able to display one row of data for a given geometry. In order to display information about multiple food commodities in one market, information from each commodities had to be combined into a single row of data. All information about each commodities were combined into a single field for each market, which is what is shown on the Resource Watch map. In order to transform the data from the JSONs to the data tables used by Resource Watch, the following changes were made:
- A unique ID for each market in the alps table was created using the 'sn' variable (combination of all ids for the market), date and the availability of forecast data. This was stored in a new column called 'uid'.
- A unique ID for each market in the markets table was created using region id, market id and market name. This was stored in a new column called 'uid'.
- A unique ID for each market in the interaction table was created using region id, market id, market name and food category. This was stored in a new column called 'uid'.
- The 'lat' and 'lon' fields were used to construct the point geometry shown on Resource Watch.


Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/foo_053_alerts_price_spikes/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
