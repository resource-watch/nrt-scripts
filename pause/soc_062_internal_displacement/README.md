## Annual Internal Displacement from Natural Disasters Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Raw Incident Data dataset](https://incidentnews.noaa.gov/) for [display on Resource Watch](https://resourcewatch.org/data/explore/soc062c-Internal-Displacement-NRT).

This dataset was provided by the source as a JSON file. This JSON was transformed into a table so that it could be uploaded to Carto. In order to transform the data from the JSON to the data table used by Resource Watch, the following changes were made:
- The latitude and longitude features from the JSON were used to create the geometry shown on Resource Watch.
- The date of each event was obtained from the 'displacement_date' feature of the json. Some of the events had errors where the 'displacement_date' was a date that has not happened yet. In those cases, the year indicated in the 'displacement_date' was replaced with the year used in the 'year' feature of the json. This was converted to a datetime object.
- The description of each event was collected from the 'standard_popup_text' feature.
- A unique ID for each event was created based on the 'id' feature of the json. This was stored in a new column called 'uid'.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/soc_062_internal_displacement/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
