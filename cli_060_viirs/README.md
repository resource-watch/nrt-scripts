## Daily Imagery Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [VIIRS_SNPP_CorrectedReflectance_TrueColor dataset](https://wiki.earthdata.nasa.gov/display/GIBS/GIBS+Available+Imagery+Products) for [display on Resource Watch](https://resourcewatch.org/data/explore/cli060nrt).

This visualization for this dataset on Resource Watch is pulled directly from NASA. Each day, the layer on Resource Watch is updated to pull the most recent tile layer visualization (which would be yesterday's imagery).

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cli_060_viirs/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Francis Gassert](https://www.wri.org/profile/francis-gassert), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
