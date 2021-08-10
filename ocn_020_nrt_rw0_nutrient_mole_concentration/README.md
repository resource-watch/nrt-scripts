## Nutrient Concentration Datasets Near Real-time Script
This file describes the near real-time script that retrieves and processes the [GLOBAL OCEAN BIOGEOCHEMISTRY ANALYSIS AND FORECAST](https://resources.marine.copernicus.eu/?option=com_csw&view=details&product_id=GLOBAL_ANALYSIS_FORECAST_BIO_001_028) for display of [nitrate](https://resourcewatch.org/data/explore/92327c78-a473-402b-8edf-409869823216), [phosphate](https://resourcewatch.org/data/explore/f1aa9ec7-c3b6-441c-b395-96fc796b7612), and [dissolved oxygen](https://resourcewatch.org/data/explore/877cdf39-5536-409c-bcba-2220e1b72796) on Resource Watch.

The dataset is available as two NetCDF files, one for monthly products and the other for daily products. Each NetCDF contains 10 sub-datasets that correspond to different biogeochemical parameters. Each sub-dataset has 50 bands that correspond to depths from 0.5 to 5727.9 meters. Only the sub-datasets for the concentration of nitrate, phosphate, and dissolved oxygen averaged from 0.5 to 5.1 meters are shown on Resource Watch.

To process this data for display on Resource Watch, 
1. The NetCDFs were downloaded, and the relevant subdatasets were extracted into 50 band GeoTIFFs.
2. For each GeoTiff, the first 5 bands (corresponding depths: 0.5, 1.5, 2.6, 3.8, and 5.1 meters) were averaged to create a single band GeoTiff.


Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/ocn_020_nrt_rw0_nutrient_mole_concentration/contents/src/__init__.py) for more details on this processing.

You can view the processed Ocean Nutrient Concentration datasets for [nitrate](https://resourcewatch.org/data/explore/92327c78-a473-402b-8edf-409869823216), [phosphate](https://resourcewatch.org/data/explore/f1aa9ec7-c3b6-441c-b395-96fc796b7612), and [dissolved oxygen](https://resourcewatch.org/data/explore/877cdf39-5536-409c-bcba-2220e1b72796) on Resource Watch. 

You can also download the original dataset [from the source website](https://resources.marine.copernicus.eu/?option=com_csw&view=details&product_id=GLOBAL_ANALYSIS_FORECAST_BIO_001_028).

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.


###### Note: This script was written by [Rachel Thoms](https://www.wri.org/profile/rachel-thoms) and is currently maintained by [Rachel Thoms](https://www.wri.org/profile/rachel-thoms).
