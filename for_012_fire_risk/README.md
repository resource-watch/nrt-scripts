## Fire Weather Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Global Fire WEather Database (GFWED) Fire Weather Index (FWI) System dataset](https://data.giss.nasa.gov/impacts/gfwed/) for [display on Resource Watch](https://resourcewatch.org/data/explore/for012-Fire-Risk-Index).

This dataset is provided by the source as netcdf files, with one file for each day. Inside each netcdf file, the fire weather index data can be found in the 'GPM.LATE.v5_FWI' variable of the netcdf file, the fine fuel moisture code data in 'GPM.LATE.v5_FFMC', the duff moisture code data in 'GPM.LATE.v5_DMC', the buildup index data in 'GPM.LATE.v5_BUI', the drought code data in 'GPM.LATE.v5_DC', and the initial spread index data in 'GPM.LATE.v5_ISI'.

To process this data for display on Resource Watch, the data in each of these netcdf variables is first converted to tif files. Then, the tif files for each variable are combined into daily files.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/for_012_fire_risk/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
