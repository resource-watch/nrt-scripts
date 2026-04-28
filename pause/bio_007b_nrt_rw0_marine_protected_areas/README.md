## Marine Protected Areas Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes marine protected areas within the [World Database on Protected Areas (WDPA) and other effective area-based conservation measures (OECM) database](http://www.protectedplanet.net/) for [display on Resource Watch](https://resourcewatch.org/data/explore/483c87c7-8724-4758-b8f0-a536b3a8f8a9).

The source provided the data as three point shapefiles and three polygon shapefiles within zipped folders. They were merged into one point shapefile and one polygon shapefile before being uploaded to Carto.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/bio_007b_nrt_rw0_marine_protected_areas/contents/src/__init__.py) for more details on this processing.

You can view the processed Marine Protected Areas dataset [on Resource Watch](https://resourcewatch.org/data/explore/483c87c7-8724-4758-b8f0-a536b3a8f8a9).

You can also download the original dataset [from the source website](https://www.protectedplanet.net/en/thematic-areas/marine-protected-areas).

**Schedule**

This script is run monthly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.


###### Note: This script was originally written by [Yujing Wu](https://www.wri.org/profile/yujing-wu), and is currently maintained by [Weiqi Zhou](https://www.wri.org/profile/weiqi-zhou).
 