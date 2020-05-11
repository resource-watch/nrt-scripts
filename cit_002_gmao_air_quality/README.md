## Air Quality: Nitrogen Dioxide (NO₂)/Fine Particulate Matter (PM2.5)/Ozone (O₃) Modeled 5-day Forecast Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Goddard Earth Observing System Composition Forecast (GEOS-CF) dataset](https://gmao.gsfc.nasa.gov/weather_prediction/GEOS-CF/) for display on Resource Watch as the following datasets:
* [Air Quality: Nitrogen Dioxide (NO₂) Modeled 5-day Forecast](https://resourcewatch.org/data/explore/cit002-GMAO-Air-Quality-Forecast-NO2)
* [Air Quality: Fine Particulate Matter (PM2.5) Modeled 5-day Forecast](https://resourcewatch.org/data/explore/cit002-GMAO-Air-Quality-Forecast-PM25)
* [Air Quality: Ozone (O₃) Modeled 5-day Forecast](https://resourcewatch.org/data/explore/cit002-GMAO-Air-Quality-Forecast-O3)
test
This dataset is provided by the source as netcdf files, with one file for each hour of the day. Inside each netcdf file, the nitrogen dioxide data can be found in the 'NO2' variable of the netcdf file, the ozone data in 'O3', and the fine particulate matter in 'PM25_RH35_GCC'.

To process this data for display on Resource Watch, the data in each of these netcdf variables is first converted to tif files. Then, the hourly tif files are combined into daily files by calculating a specific metric for each variable. For both nitrogen dioxide and PM2.5, a daily average is calculated, and for ozone, the daily maximum value is calculated. Finally, the units for ozone and nitrogen dioxide are converted from mol/mol to to parts per billion (ppb), while the original source units are kept for fine particulate matter.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cit_002_gmao_air_quality/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
