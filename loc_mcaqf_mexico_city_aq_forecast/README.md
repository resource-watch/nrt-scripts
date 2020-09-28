## Mexico City Air Quality Forecast Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Air quality and Meteorological Forecast for Mexico City dataset](http://www.aire.cdmx.gob.mx/pronostico-aire/pronostico-por-contaminante.php) for display on Resource Watch as the following datasets:
* [Air Quality: Mexico City Nitrogen Dioxide (NO₂) Forecast](https://resourcewatch.org/data/explore/)
* [Air Quality: Mexico City Fine Particulate Matter (PM2.5) Forecast](https://resourcewatch.org/data/explore/)
* [Air Quality: Mexico City Ozone (O₃) Forecast](https://resourcewatch.org/data/explore/)
* [Air Quality: Mexico City Sulfur Dioxide (SO₂) Forecast](https://resourcewatch.org/data/explore/)
* [Air Quality: Mexico City Carbon Monoxide (CO) Forecast](https://resourcewatch.org/data/explore/)

This dataset was provided by the source as a NETCDF file. The air quality data for each compound at each timestep was converted to a tif file so that it could be uploaded to Google Earth Engine.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/\loc_mcaqf_mexico_city_aq_forecast/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).