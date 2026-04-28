## GMAO AQ Forcast Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [GMAO AQ Forcast dataset]() for display on Resource Watch as the following datasets:

* [Air Quality: Nitrogen Dioxide (NO₂) Station Forecasts](https://resourcewatch.org/data/explore/)
* [Air Quality: Fine Particulate Matter (PM2.5) Station Forecasts](https://resourcewatch.org/data/explore/)
* [Air Quality: Ozone (O₃) Station Forecasts](https://resourcewatch.org/data/explore/)

This dataset was provided by the source as a JSON file. A unique ID for each air quality forecast was created using forecast date, creation date, and station number. This was stored in a new column called 'uid'. The JSON was transformed into a table and the resulting table was then uploaded to Carto. 

The raw data is provided as hourly data. After the raw data is uploaded to Carto, another table is created to store air quality metrics that are calculated from the raw data. The following metrics were calculated for each compound:
* NO₂: maximum daily 1-hour average
* PM2.5: daily average
* O₃: maximum daily 8-hour average

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cit_004_city_aq/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid), and is currently maintained by [Weiqi Zhou](https://wri.org.cn/en/profile/weiqi-zhou).
