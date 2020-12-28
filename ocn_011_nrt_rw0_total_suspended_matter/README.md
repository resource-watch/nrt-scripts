## Total Suspended Matter Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [the Total Suspended Matter Concentration dataset](https://www.globcolour.info/CDR_Docs/GlobCOLOUR_PUG.pdf) for [display on Resource Watch](https://resourcewatch.org/data/explore/6ad0f556-20fd-4ddf-a5cc-bf93c003a463).

The dataset is available as two NetCDF files, one for monthly product and the other for 8-day product. Each NetCDF has a primary data layer and a secondary flags layer. Only the primary data layers are shown on Resource Watch.

To process this data for display on Resource Watch, the NetCDFs were downloaded, and the primary data layer was extracted from each into a single-band GeoTIFF.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/ocn_011_nrt_rw0_total_suspended_matter/contents/src/__init__.py) for more details on this processing.

You can view the processed Total Suspended Matter dataset [on Resource Watch](https://resourcewatch.org/data/explore/6ad0f556-20fd-4ddf-a5cc-bf93c003a463).

You can also download the original dataset [from the source website](https://www.globcolour.info/products_description.html).

**Schedule**

This script is run weekly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.


###### Note: This script was originally written by [Peter Kerins](https://www.wri.org/profile/peter-kerins), and is currently maintained by [Yujing Wu](https://www.wri.org/profile/yujing-wu).
