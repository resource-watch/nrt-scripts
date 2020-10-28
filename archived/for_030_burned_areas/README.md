## Burned Areas Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [MCD64A1 Version 6 Burned Area dataset](https://lpdaac.usgs.gov/products/mcd64a1v006/) for [display on Resource Watch]().

This script was written to get the burned areas data from USGS catalogue (https://e4ftl01.cr.usgs.gov/MOTA/MCD64A1.006/2000.11.01/). However, we found out that this dataset is available as an asset in Google Earth Engine. So, we decided to pull data from the GEE asset instead rather than using a script to download the data. This script is archived in case we need to use it in the future. Although the script is almost ready to go but it didn't get the finishing touch. So, all the components of the script should be thoroughly reviewed before putting it into production.

Please see the [Python script]() for more details on this processing.

**Schedule**

This script is run monthly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid), and is currently maintained by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid).
