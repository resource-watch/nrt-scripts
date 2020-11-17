## GLAD Deforestation Alerts Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [the Global Land Analysis and Discovery (GLAD) Alert](http://iopscience.iop.org/article/10.1088/1748-9326/11/3/034008) for [display on Resource Watch](http://resourcewatch.org/data/explore/6ec78a52-3fb2-478f-a02b-abafa5328244).

The source provided the data as an image collection on Google Earth Engine. Each day of data were split into five images of different regions.

For each day, data of five different regions are mosaicked to create a global dataset for display on Resource Watch.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/for_003_nrt_glad_deforestation_alerts/contents/src/__init__.py) for more details on this processing.

You can view the processed GLAD Deforestation Alerts dataset [on Resource Watch](http://resourcewatch.org/data/explore/6ec78a52-3fb2-478f-a02b-abafa5328244).

You can also download the original dataset [from the source website](http://iopscience.iop.org/article/10.1088/1748-9326/11/3/034008).

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.


###### Note: This script was originally written by [Yujing Wu](https://www.wri.org/profile/yujing-wu), and is currently maintained by [Yujing Wu](https://www.wri.org/profile/yujing-wu).
