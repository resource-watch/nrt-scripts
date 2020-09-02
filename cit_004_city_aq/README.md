## GMAO AQ Forcast Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [GMAO AQ Forcast dataset]() for [display on Resource Watch]().

This dataset was provided by the source as a JSON file. A unique ID for each air quality forecast was created using forecast date and station number. This was stored in a new column called 'uid'. The JSON was transformed into a table and the resulting table was then uploaded to Carto. 

Please see the [Python script]() for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid), and is currently maintained by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid).

###### Archive Note: This dataset is still on hold and saved in archive folder until we get a final confirmation from the dataprovider about their plan of how they want to see it on Resource Watch. The processed data is saved in a Carto table named 'xxx_xxx_devseed_air_quality'.
