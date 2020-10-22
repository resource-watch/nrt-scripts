## Water Conflict Map Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [the World Water Conflict Chronology Map](https://www.worldwater.org/water-conflict/ ) for [display on Resource Watch](https://resourcewatch.org/data/explore/24928aa3-28d3-457c-ad2a-62f3c83ef663).

The source provided the data as a PHP file.

The 'Start' and 'End' columns were renamed to be 'start_year' and 'end_year' since 'end' is a reserved word in PostgreSQL. The start and end year of the conflicts were converted to datetime objects (using first day of January to fill day and month for each conflict) and stored in two new columns 'start_dt' and 'end_dt'. The 'Conflict Type' column was renamed to be 'conflict_type'. The resulting table was uploaded to Carto.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/soc_049_water_conflict_map/contents/src/__init__.py) for more details on this processing.

You can view the processed Water Conflict Map dataset [on Resource Watch](https://resourcewatch.org/data/explore/24928aa3-28d3-457c-ad2a-62f3c83ef663).

You can also download the original dataset [from the source website](https://www.worldwater.org/water-conflict/ ).

**Schedule**

This script is run monthly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.


###### Note: This script was originally written by [Yujing Wu](https://www.wri.org/profile/yujing-wu), and is currently maintained by [Yujing Wu](https://www.wri.org/profile/yujing-wu).
