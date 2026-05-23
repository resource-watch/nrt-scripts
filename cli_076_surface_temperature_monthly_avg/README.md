
## {Resource Watch Public Title} Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [Berkeley Earth Surface Temperature dataset](http://berkeleyearth.lbl.gov/auto/Global/Gridded/Gridded_README.txt) for [display on Resource Watch]({link to dataset's metadata page on Resource Watch}).

This dataset was provided by the source as a netcdf file. The data shown on Resource Watch can be found in the 'temperature' variable of the netcdf file. This variable was converted to a set of tif files so that it could be uploaded to Google Earth Engine. The netcdf file contained all the monthly temperature anomaly data since 1850. We are only interested in data going back to 1950. So, only the temperature data since 1950 were processed. To process this data for display on Resource Watch, all the monthly tif files were combined into yearly files to get an annual average temperature anomaly from 1950 till now.

Please see the [Python script]({link to Python script on Github}) for more details on this processing.

**Schedule**

This script is run monthly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid), and is currently maintained by [Taufiq Rashid](https://www.wri.org/profile/taufiq-rashid).
