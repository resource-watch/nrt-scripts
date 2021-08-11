## Air Quality: Fine Particulate Matter (PM2.5)/Particulate Matter (PM10)/Sulfur Dioxide (SO₂)/Nitrogen Dioxide (NO₂)/Ozone (O₃)/Carbon Monoxide (CO)/Black Carbon Station Measurements Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [OpenAQ Air Quality Readings dataset](https://openaq.org/) for display on Resource Watch as the following datasets:
* [Air Quality: Fine Particulate Matter (PM2.5) Station Measurements](https://resourcewatch.org/data/explore/cit003anrt-Air-Quality-Measurements-PM-25)
* [Air Quality: Particulate Matter (PM10) Station Measurements](https://resourcewatch.org/data/explore/cit003bnrt-Air-Quality-Measurements-PM-10)
* [Air Quality: Sulfur Dioxide (SO₂) Station Measurements](https://resourcewatch.org/data/explore/cit003cairqualityso2)
* [Air Quality: Nitrogen Dioxide (NO₂) Station Measurements](https://resourcewatch.org/data/explore/cit003dnrt-Air-Quality-Measurements-NO)
* [Air Quality: Ozone (O₃) Station Measurements](https://resourcewatch.org/data/explore/cit003cnrt-Air-Quality-Measurements-O)
* [Air Quality: Carbon Monoxide (CO) Station Measurements](https://resourcewatch.org/data/explore/cit003fnrt-Air-Quality-Measurements-CO)
* [Air Quality: Black Carbon Station Measurements](https://resourcewatch.org/data/explore/cit003gnrt-Air-Quality-Measurements-BC_1)

{Describe how the original data came from the source.}

{Describe the steps used to process the data, e.g., "convert variable X from the original netcdf file to a tif to upload to Google Earth Engine."}

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cit_003_air_quality/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run twice daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

This script has been archived since we are no longer fetching data using OpenAQ API. We are using the [Realtime JSON dumps](https://openaq-fetches.s3.amazonaws.com/index.html).

###### Note: This script was originally written by [Francis Gassert](https://www.wri.org/profile/francis-gassert), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
