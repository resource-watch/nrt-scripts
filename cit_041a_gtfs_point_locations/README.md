## Cities with Transit Feed Data Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Cities with GTFS dataset](https://openmobilitydata.org/) for [display on Resource Watch](https://resourcewatch.org/data/explore/cit041-Transit-Feed).

This dataset was provided by the source as a JSON file. The JSON file contains a list of all the locations for which transit feed data is available. The id feature from this JSON was used to access individual JSON files for each location that contain more attributes about them. Those JSONs were transformed into a table so that it could be uploaded to Carto. In order to transform the data from the JSON to the data table used by Resource Watch, the following changes were made:
- A pandas dataframe was created using the 'feeds' variable from the 'results' feature of each secondary JSON. The nested structure of the JSON was unpacked so that each variable had its own column with only one value. 
- The columns in the dataframe were renamed.
- The 'lat' and 'lng' variables from the JSON were used to create the geometry shown on Resource Watch.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cit_041a_gtfs_point_locations/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run monthly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Ken Wakabayashi](https://www.wri.org/profile/ken-wakabayashi) and [Kristine Lister](https://www.wri.org/profile/kristine-lister), and is currently maintained by [Kristine Lister](https://www.wri.org/profile/kristine-lister).
