## Script to update near real-time timestamps on Resource Watch
All of the [near real-time datasets on Resource Watch](https://bit.ly/2yzT8of) are updated by one of the following methods:
1) a script in this nrt-scripts Github repository updates the data on a regular schedule
2) the Carto sync feature updates the data on a regular schedule
3) the dataset is pulled directly from the Google Earth Engine data catalog, and the data is automatically updated as soon as new data is available

After the underlying data for a dataset is updated, the 'last update date' associated with the dataset must also be changed to reflect this update. For any dataset the is updated through a script in the nrt-scripts repository, the 'last update date' will be adjusted each time the script is run. However, we need a different mechanism to update the 'last update date' for datasets that are updated updated through the Google Earth Engine data catalog or the Carto sync feature. This script is intended to fulfuill that need.

This script updates the 'last update date' for each dataset on Resource Watch with the following steps:
1) Check the current 'last update date' shown on Resource Watch.
2) Check the most recent time of synchronization for tables using Carto sync or the most recent asset's timestamp for GEE datasets. 
3) Update the 'last update date' on Resource Watch, if needed.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/update_nrt_timestamps/contents/src/__init__.py) for more details.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
