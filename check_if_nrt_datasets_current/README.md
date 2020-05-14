## Script to check if near real-time datasets are up-to-date
All of the [near real-time datasets on Resource Watch](https://bit.ly/2yzT8of) are updated by one of the following methods:
 1) a script in this nrt-scripts Github repository updates the data on a regular schedule
 2) the Carto sync feature updates the data on a regular schedule
 3) the dataset is pulled directly from the Google Earth Engine data catalog, and the data is automatically updated as soon as new data is available

This script is used to make sure all of these near real-time datasets are up-to-date. If a dataset becomes out of date, we need to investigate why the dataset is no longer updating to find out if the source has moved the data to a different location or is no longer maintaining the dataset.

This script is currently the only system we use to monitor the datasets that are updated through the Google Earth Engine data catalog or the Carto sync feature to make sure they are being updated as expected.

Our datasets that are updated through the nrt-scripts repository include error logging that is intended to send us a notification if the data doesn't update properly; however, there are times that this logging does not catch a dataset that is not updating. For example, if we have a dataset that updates somewhat irregularly, every few days, we may choose to run the update script every day. However, we do not want to receive an error log every time the script runs and doesn't find data because we know the source is not adding data every day. This script is also able to track datasets like this and to ensure they are staying up to date.

This script checks if datasets are up-to-date with the following steps:
1) Pull metadata for all datasets on Resource Watch and filter out the near real-time datasets.
2) Check when each dataset was last updated on the Resource Watch API.
3) Compare this date to the expected frequency of updates noted in our metadata.
4) Send an error log if any of the datasets are out of date.

Two important things to note about this script:
- Many of the datasets update on a lag. A dataset may update monthly, but on 3 month lag. This would cause the script to send a notification that the dataset is out of date because it has not updated in the past month. Therefore, the information pulled from the expected frequency of updates is often overwritten by a custom timeframe directly in the script.
- Sometimes a dataset will be out of date, and after investigation, we determine that we do not need to take any action because we expect the source to start updating again in the future. For example, the source may be doing temporary maintenance that causes the dataset to not update. We do not want to receive daily error notifications for these datasets after we have already investigated why they are not working. We track the datasets that we have already invesitgated in the [outdated_nrt_scripts csv](https://github.com/resource-watch/nrt-scripts/master/check_if_nrt_datasets_current/outdated_nrt_scripts.csv) included in this file. Once a dataset has been added to this sheet with an explanation of why the dataset is out of date, no error logs will be sent for one week. After a week, you will be sent a reminder that this dataset is still out of date. You must update the 'Last Check' column to pause this notification for another week.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/check_if_nrt_datasets_current/contents/src/__init__.py) for more details.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
