## Food Price Spikes Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Alert for Price Spikes (ALPS) dataset](https://documents.wfp.org/stellent/groups/public/documents/manual_guide_proced/wfp264186.pdf?_ga=2.155059965.418661181.1556120721-1045976685.1553722904) for [display on Resource Watch](https://resourcewatch.org/data/explore/foo053-Food-Price-Spikes).

{Describe how the original data came from the source.}

{Describe the steps used to process the data, e.g., "convert variable X from the original netcdf file to a tif to upload to Google Earth Engine."}

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/foo_053_alerts_price_spikes/contents/src/__init__.py) for more details on this processing.

You can view the processed Food Price Spikes dataset [on Resource Watch](https://resourcewatch.org/data/explore/foo053-Food-Price-Spikes).

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Nathan Suberi](mailto:nathan.suberi@wri.org), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
