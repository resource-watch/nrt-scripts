## Global Temperature Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Global Land-Ocean Temperature Index dataset](https://climate.nasa.gov/vital-signs/global-temperature/) for [display on Resource Watch](https://resourcewatch.org/data/explore/917f1945-fff9-4b6f-8290-4f4b9417079e).

This dataset was provided by the source as a text file. The following changes were made before uploading the data to the Resource Watch:
- The table was converted from a wide to a long form.
- The 'date' column was constructed using the 'Year' column. The day was assumed to be January 1st when constructing this date.

The data shown on Resource Watch can be found in the 'No_Smoothing' and 'Lowess(5)' columns of the text file.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cli_044_global_land_temperature/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run monthly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid), and is currently maintained by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid).
