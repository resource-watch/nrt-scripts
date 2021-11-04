## Air Quality: Nitrogen Dioxide (NO₂)/Carbon Monoxide (CO)/Aerosol Index/Ozone (O₃) Satellite Measurements Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Sentinel 5 Precursor Tropospheric Monitoring Instrument (S5P/TROPOMI) Offline (OFFL) Nitrogen Dioxide (NO₂) L3 Monthly Averages dataset](https://sentinel.esa.int/web/sentinel/missions/sentinel-5p) for display on Resource Watch as the following datasets:
* [Air Quality: Nitrogen Dioxide (NO₂) Satellite Measurements](https://resourcewatch.org/data/explore/Air-Quality-Measurements-TROPOMI-NO)
* [Air Quality: Carbon Monoxide (CO) Satellite Measurements](https://resourcewatch.org/data/explore/Air-Quality-Measurements-TROPOMI-CO)
* [Air Quality: Aerosol Index Satellite Measurements](https://resourcewatch.org/data/explore/Air-Quality-Measurements-TROPOMI-AER-AI)
* [Air Quality: Ozone (O₃) Satellite Measurements](https://resourcewatch.org/data/explore/Air-Quality-Measurements-TROPOMI-O)

The data shown on Resource Watch for this dataset is pulled and processed from the Google Earth Engine catalog. The nitrogen dioxide data can be found in the 'tropospheric_NO2_column_number_density' band, the ozone data in 'O3_column_number_density', the carbon monoxide data in 'CO_column_number_density', and the absorbing aerosol index data in 'absorbing_aerosol_index'.

For each variable, the band of interest is pulled and the most recent 30 days of data are filtered out and averaged. This 30-day average is saved as a new asset in Google Earth Engine to be displayed on Resource Watch. The nitrogen dioxide data is uploaded to AWS S3 storage for MapBuilder.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cit_035_tropomi_atmospheric_chemistry_model/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and is currently maintained by [Weiqi Zhou](https://www.wri.org/profile/weiqi-zhou).