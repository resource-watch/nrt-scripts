## Air Quality: Fine Particulate Matter (PM2.5)/Particulate Matter (PM10)/Sulfur Dioxide (SO₂)/Nitrogen Dioxide (NO₂)/Ozone (O₃)/Carbon Monoxide (CO)/Black Carbon Station Measurements Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [OpenAQ Air Quality Readings dataset](https://gmao.gsfc.nasa.gov/weather_prediction/GEOS-CF/) to display real-time station level outdoor measurements of
* [Fine Particulate Matter (PM2.5)](https://resourcewatch.org/data/explore/cit003anrt-Air-Quality-Measurements-PM-25)
* [Particulate Matter (PM10)](https://resourcewatch.org/data/explore/cit003bnrt-Air-Quality-Measurements-PM-10)
* [Sulfur Dioxide (SO₂) concentrations](https://resourcewatch.org/data/explore/cit003cairqualityso2)
* [Nitrogen Dioxide (NO₂) concentrations](https://resourcewatch.org/data/explore/cit003dnrt-Air-Quality-Measurements-NO)
* [Ozone (O₃) concentrations](https://resourcewatch.org/data/explore/cit003cnrt-Air-Quality-Measurements-O)
* [Carbon Monoxide (CO)](https://resourcewatch.org/data/explore/cit003fnrt-Air-Quality-Measurements-CO)
* [Black Carbon](https://resourcewatch.org/data/explore/cit003gnrt-Air-Quality-Measurements-BC_1)
<br>on Resource Watch.

{Describe how the original data came from the source.}

{Describe the steps used to process the data, e.g., "convert variable X from the original netcdf file to a tif to upload to Google Earth Engine."}

Please see the [Python script](https://github.com/Taufiq06/nrt-scripts/blob/master/cit_003_air_quality/contents/src/__init__.py) for more details on this processing.

You can view the processed 
* Air Quality: Fine Particulate Matter (PM2.5) Station Measurements dataset [on Resource Watch](https://resourcewatch.org/data/explore/cit003anrt-Air-Quality-Measurements-PM-25).
* Air Quality: Particulate Matter (PM10) Station Measurements dataset [on Resource Watch](https://resourcewatch.org/data/explore/cit003bnrt-Air-Quality-Measurements-PM-10).
* Air Quality: Sulfur Dioxide (SO₂) Station Measurements dataset [on Resource Watch](https://resourcewatch.org/data/explore/cit003cairqualityso2).
* Air Quality: Nitrogen Dioxide (NO₂) Station Measurements dataset [on Resource Watch](https://resourcewatch.org/data/explore/cit003dnrt-Air-Quality-Measurements-NO).
* Air Quality: Ozone (O₃) Station Measurements dataset [on Resource Watch](https://resourcewatch.org/data/explore/cit003cnrt-Air-Quality-Measurements-O).
* Air Quality: Carbon Monoxide (CO) Station Measurements dataset [on Resource Watch](https://resourcewatch.org/data/explore/cit003fnrt-Air-Quality-Measurements-CO).
* Air Quality: Black Carbon Station Measurements dataset [on Resource Watch](https://resourcewatch.org/data/explore/cit003gnrt-Air-Quality-Measurements-BC_1).
**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
