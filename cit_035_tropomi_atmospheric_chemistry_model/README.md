## Air Quality: Nitrogen Dioxide (NO₂)/Carbon Monoxide (CO)/Aerosol Index/Ozone (O₃) Satellite Measurements Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Sentinel 5 Precursor Tropospheric Monitoring Instrument (S5P/TROPOMI) Offline (OFFL) Nitrogen Dioxide (NO₂) L3 Monthly Averages dataset](https://sentinel.esa.int/web/sentinel/missions/sentinel-5p) to display average monthly density of
* [Nitrogen Dioxide (NO₂)](https://resourcewatch.org/data/explore/Air-Quality-Measurements-TROPOMI-NO)
* [Carbon Monoxide (CO)](https://resourcewatch.org/data/explore/Air-Quality-Measurements-TROPOMI-CO)
* [Aerosol Index](https://resourcewatch.org/data/explore/Air-Quality-Measurements-TROPOMI-AER-AI)
* [Ozone (O₃)](https://resourcewatch.org/data/explore/Air-Quality-Measurements-TROPOMI-O)

on Resource Watch.

{Describe how the original data came from the source.}

{Describe the steps used to process the data, e.g., "convert variable X from the original netcdf file to a tif to upload to Google Earth Engine."}

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cit_035_tropomi_atmospheric_chemistry_model/contents/src/__init__.py) for more details on this processing.

You can view the processed 
* Air Quality: Nitrogen Dioxide (NO₂) Satellite Measurements dataset [on Resource Watch](https://resourcewatch.org/data/explore/Air-Quality-Measurements-TROPOMI-NO).
* Air Quality: Carbon Monoxide (CO) Satellite Measurements dataset [on Resource Watch](https://resourcewatch.org/data/explore/Air-Quality-Measurements-TROPOMI-CO).
* Air Quality: Aerosol Index Satellite Measurements dataset [on Resource Watch](https://resourcewatch.org/data/explore/Air-Quality-Measurements-TROPOMI-AER-AI).
* Air Quality: Ozone (O₃) Satellite Measurements dataset [on Resource Watch](https://resourcewatch.org/data/explore/Air-Quality-Measurements-TROPOMI-O).

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
