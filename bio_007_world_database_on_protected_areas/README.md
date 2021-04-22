## World Database on Protected Areas Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [World Database on Protected Areas dataset](https://protectedplanet.net/) for [display on Resource Watch](https://resourcewatch.org/data/explore/bio007-World-Database-on-Protected-Areas_replacement).

This dataset was provided by the source as a file geodatabase within a zipped folder. Years stored in the 'STATUS_YR' column have been converted to datetime objects and stored in a new column 'legal_status_updated_at'. Column names were converted to lowercase before we uploaded it to Carto.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/bio_007_world_database_on_protected_areas/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run monthly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Yujing Wu](https://www.wri.org/profile/yujing-wu), and is currently maintained by [Yujing Wu](https://www.wri.org/profile/yujing-wu).
