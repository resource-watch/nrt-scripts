## Coral Bleaching Monitoring Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Daily Global 5km Satellite Coral Bleaching Heat Stress Monitoring dataset](https://coralreefwatch.noaa.gov/product/5km/index.php) for [display on Resource Watch](https://resourcewatch.org/data/explore/).

This dataset is provided by the source in a number of separate NetCDF files. The following variables are shown on Resource Watch:
- Bleaching Alert Area (bleaching_alert_area_7d): Areas where bleaching thermal stress satisfies specific criteria based on HotSpot and Degree Heating Week (DHW) values
- HotSpot (hotspots): Measures the magnitude of daily thermal stress that can lead to coral bleaching
- Degree Heating Week (degree_heating_week): Indicates duration and magnitude of thermal stress (one DHW is equal to one week of SST 1°C warmer than the historical average for the warmest month of the year)
- Sea Surface Temperature Anomaly (sea_surface_temperature_anomaly): Difference between the daily SST and the corresponding daily SST climatology
- Sea Surface Temperature (sea_surface_temperature): Night-time temperature of ocean at surface
- Sea Surface Tempature Trend (sea_surface_temperature_trend_7d): Pace and direction of the SST variation and thus coral bleaching heat stress

To process this data for display on Resource Watch, each NetCDF file was downloaded, and the relevant subdatasets were extracted to individual, single-band GeoTIFFs. These GeoTIFF files were then merged into a single multi-band GeoTIFF, where each band corresponds to one variable. The merged GeoTIFF was then uploaded to Google Earth Engine.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/ocn_007_coral_bleaching_monitoring/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid) and [Peter Kerins](https://www.wri.org/profile/peter-kerins), and is currently maintained by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid).
