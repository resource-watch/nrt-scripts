## Nationally Determined Contributions Ratification Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [CAIT Paris Contributions Map: Summary and Analysis of INDC/NDC contents dataset](https://www.climatewatchdata.org/ndcs-content?indicator=pa_status) for [display on Resource Watch](https://resourcewatch.org/data/explore/cli047-NDC-ratification-status).

{Describe how the original data came from the source.}

{Describe the steps used to process the data, e.g., "convert variable X from the original netcdf file to a tif to upload to Google Earth Engine."}

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cli_047_ndc_ratification/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run monthly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
