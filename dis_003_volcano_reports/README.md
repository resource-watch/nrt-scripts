## Volcanic Activity Report Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Smithsonian/USGS Weekly Volcanic Activity Report dataset](http://volcano.si.edu/reports_weekly.cfm#vn_358057) for [display on Resource Watch](https://resourcewatch.org/data/explore/dis003-Weekly-Volcano-Report-NEW).

This dataset was provided by the source as a xml file. The data shown on Resource Watch can be found in the 'title' tag of the xml file. The contents of the 'title' tag was processed to obtain volcano name and country name. Source of the volcano report was obtained by processing the contents of the description tag. Datetime was retrieved from the 'pubDate' tag. The resulting table was uploaded to Carto.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/dis_003_volcano_reports/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run weekly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
