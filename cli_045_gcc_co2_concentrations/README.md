## Carbon Dioxide Concentration Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [CO₂ Direct Measurements 2005-Present dataset](https://climate.nasa.gov/vital-signs/carbon-dioxide/) for [display on Resource Watch](https://resourcewatch.org/data/explore/d287c201-4d7b-4b41-b352-edfcc6f96cb0).

This dataset was provided by the source as a text file. A pandas dataframe was created using the contents of the text file. Decimal dates included in the file were converted to datetime objects, and the resulting table was uploaded to Carto.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cli_045_gcc_co2_concentrations/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run weekly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid), and is currently maintained by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid).
