## Volcano Eruptions Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Volcanoes of the World Database - Eruptions dataset](http://volcano.si.edu/search_eruption.cfm) for [display on Resource Watch](https://resourcewatch.org/data/explore/dis013-Volcanoes-CSV).

This dataset was provided by the source as a XML file. This XML was first converted into a JSON and then transformed into a table so that it could be uploaded to Carto. The 'GeoLocation' was used to create the geometry shown on Resource Watch.


Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/dis_013_volcano_eruptions/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
