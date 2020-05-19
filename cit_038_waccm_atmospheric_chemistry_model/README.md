## Air Quality: Sulfur Dioxide (SO₂)/Fine Particulate Matter (PM2.5)/Black Carbon/Ozone (O₃)/Nitrogen Dioxide (NO₂)/Carbon Monoxide (CO) Modeled Next-day Forecast Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Whole Atmosphere Community Climate Model (WACCM) dataset](https://www2.acom.ucar.edu/gcm/waccm) for display on Resource Watch as the following datasets:
* [Air Quality: Sulfur Dioxide (SO₂) Modeled Next-day Forecast](https://resourcewatch.org/data/explore/cit038-Air-Quality-Modeled-Forecast-WACCM-SO)
* [Air Quality: Fine Particulate Matter (PM2.5) Modeled Next-day Forecast](https://resourcewatch.org/data/explore/cit038-Air-Quality-Modeled-Forecast-WACCM-PM-25)
* [Air Quality: Black Carbon Modeled Next-day Forecast](https://resourcewatch.org/data/explore/cit038-Air-Quality-Modeled-Forecast-WACCM-Black-Carbon)
* [Air Quality: Ozone (O₃) Modeled Next-day Forecast](https://resourcewatch.org/data/explore/cit038-Air-Quality-Modeled-Forecast-WACCM-O)
* [Air Quality: Nitrogen Dioxide (NO₂) Modeled Next-day Forecast](https://resourcewatch.org/data/explore/cit038-Air-Quality-Modeled-Forecast-WACCM-NO)
* [Air Quality: Carbon Monoxide (CO) Modeled Next-day Forecast](https://resourcewatch.org/data/explore/cit038-Air-Quality-Modeled-Forecast-WACCM-CO)

This dataset is provided by the source as netcdf files, with one file for each day. Inside each netcdf file, the nitrogen dioxide data can be found in the 'NO2' variable of the netcdf file, the ozone data in 'O3', the carbon monoxide data in 'CO', the sulfur dioxide data in 'SO2', the black carbon data in 'bc_a4', and the fine particulate matter data in 'PM25_SRF'.

Several of these variables have more than one atmospheric pressure level available. Only the surface level data (992.5 hPa) is processed for display on Resource Watch.

Mutiple time intervals are also available in each netcdf. The source offers data in 3-hour and 6-hour intervals. Resource Watch uses the 6-hour interval data.

Each time a new forecast becomes available from the source, data for each available time between the start date of the forecast and the next day at 12:00 UTC is processed. However, only the image for the next day at 12:00 UTC is visualized on Resource Watch.

Data for each of these netcdf variables (at surface level, for the described times) is converted to tif files and uploaded to Google Earth Engine.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cit_038_waccm_atmospheric_chemistry_model/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
