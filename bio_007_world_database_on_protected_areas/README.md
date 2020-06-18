## World Database on Protected Areas Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [World Database on Protected Areas dataset](https://protectedplanet.net/) for [display on Resource Watch](https://resourcewatch.org/data/explore/bio007-World-Database-on-Protected-Areas_replacement).

This dataset was provided by the source as a JSON file. This JSON was transformed into a table so that it could be uploaded to Carto. In order to transform the data from the JSON to the data table used by Resource Watch, the following changes were made:
- In certain cases, there were more than one country for the same entry. 'country_name' and 'iso3' columns were adjusted for those situations so that they can hold multiple values separated by a semicolon. 
- For columns that were string, any leading or tailing whitespace were removed.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/bio_007_world_database_on_protected_areas/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run monthly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
