## Volcanic Activity Report Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Smithsonian/USGS Weekly Volcanic Activity Report dataset](http://volcano.si.edu/reports_weekly.cfm#vn_358057) for [display on Resource Watch](https://resourcewatch.org/data/explore/dis003-Weekly-Volcano-Report-NEW).

This dataset was provided by the source as a XML file. This XML was first converted into a JSON and then transformed into a table so that it could be uploaded to Carto. In order to transform the data from the JSON to the data table used by Resource Watch, the following changes were made:
- The 'georss:point' variable from the 'item' feature of the JSON was used to create the geometry shown on Resource Watch.
- Dates of the volcano activity were obtained from the 'pubDate' feature of the JSON. This was converted to a datetime object.
- The description and sources of the volcano activities were obtained from the 'description' variable within the 'item' feature of the JSON. 
- - A unique ID for each event was created based on the latitude, longitude, and date. This was stored in a new column called 'uid'.

The data shown on Resource Watch can be found in the 'title' variable of the 'item' feature of the JSON. Name of volcano and it's country of origin were extracted by processing the 'title' variable.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/dis_003_volcano_reports/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run weekly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
