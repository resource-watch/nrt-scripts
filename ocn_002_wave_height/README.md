## Wave Height Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [WAVEWATCH III® Significant Wave Height dataset](https://polar.ncep.noaa.gov/waves/index.shtml?) for [display on Resource Watch](https://resourcewatch.org/data/explore/ocn002nrt-Wave-Height_1).

This dataset is provided by the source as GRiB files. The source offers data in 6-hour interval, and include forecasts of every hour from the initial time out to 120 hours, and then forecasts at 3-hour intervals out to 180 hours. Resource Watch shows the most recent data with current prediction, 12th forecast, 24th forecast and 48th forecast.

Data for each of these predictions (for the described times) are converted to tif files. They are then merged into a single tif file as separate bands and then uploaded to Google Earth Engine.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/ocn_002_wave_height/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid), and is currently maintained by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid).
