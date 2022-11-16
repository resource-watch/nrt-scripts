## Mexico City Air Quality Forecast Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Air Quality and Meteorological Forecast for Mexico City dataset](http://www.aire.cdmx.gob.mx/pronostico-aire/pronostico-por-contaminante.php) for display on Resource Watch as the following datasets:
* [Air Quality: Mexico City Nitrogen Dioxide (NO₂) Forecast](https://resourcewatch.org/data/explore/918ba6bc-69ed-44fb-9b29-5fb445fdfef6)
* [Air Quality: Mexico City Fine Particulate Matter (PM2.5) Forecast](https://resourcewatch.org/data/explore/7a34b770-83f9-4c6a-acb8-31edcff7241e)
* [Air Quality: Mexico City Ozone (O₃) Forecast](https://resourcewatch.org/data/explore/00d6bae1-e105-4165-8230-ee73a8128538)
* [Air Quality: Mexico City Sulfur Dioxide (SO₂) Forecast](https://resourcewatch.org/data/explore/59790e64-d95d-43fa-a124-5c7eb1cb4456)
* [Air Quality: Mexico City Carbon Monoxide (CO) Forecast](https://resourcewatch.org/data/explore/e39f5910-a9b8-4ef1-b4b4-f6b141b15541)

This dataset was provided by the source as a NETCDF file. The air quality data for each compound at each timestep was converted to a tif file so that it could be uploaded to Google Earth Engine.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/loc_mcaqf_mexico_city_aq_forecast/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and is currently maintained by [Weiqi Zhou](https://www.wri.org/profile/weiqi-zhou).
