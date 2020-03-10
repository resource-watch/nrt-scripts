## Air Quality: Nitrogen Dioxide (NO₂)/Fine Particulate Matter (PM2.5)/Ozone (O₃) Modeled 5-day Forecast Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Goddard Earth Observing System Composition Forecast (GEOS-CF) dataset](https://gmao.gsfc.nasa.gov/weather_prediction/GEOS-CF/) to display five-day forecast for average daily
* [NO₂ concentrations](https://resourcewatch.org/data/explore/cit002-GMAO-Air-Quality-Forecast-NO2)
* [Fine Particulate Matter (PM2.5) concentrations](https://resourcewatch.org/data/explore/cit002-GMAO-Air-Quality-Forecast-PM25)
* [Ozone (O₃) concentrations](https://resourcewatch.org/data/explore/cit002-GMAO-Air-Quality-Forecast-O3)

on Resource Watch.

{Describe how the original data came from the source.}

{Describe the steps used to process the data, e.g., "convert variable X from the original netcdf file to a tif to upload to Google Earth Engine."}

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cit_002_gmao_air_quality/contents/src/__init__.py) for more details on this processing.

You can view the processed 
* Air Quality: Nitrogen Dioxide (NO₂) Modeled 5-day Forecast dataset [on Resource Watch](https://resourcewatch.org/data/explore/cit002-GMAO-Air-Quality-Forecast-NO2).
* Air Quality: Fine Particulate Matter (PM2.5) Modeled 5-day Forecast dataset [on Resource Watch](https://resourcewatch.org/data/explore/cit002-GMAO-Air-Quality-Forecast-PM25).
* Air Quality: Ozone (O₃) Modeled 5-day Forecast dataset [on Resource Watch](https://resourcewatch.org/data/explore/cit002-GMAO-Air-Quality-Forecast-O3).

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
