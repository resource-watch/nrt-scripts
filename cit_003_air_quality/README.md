## Air Quality: Fine Particulate Matter (PM2.5)/Particulate Matter (PM10)/Sulfur Dioxide (SO₂)/Nitrogen Dioxide (NO₂)/Ozone (O₃)/Carbon Monoxide (CO)/Black Carbon Station Measurements Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [OpenAQ Air Quality Readings dataset](https://openaq.org/) for display on Resource Watch as the following datasets:
* [Air Quality: Fine Particulate Matter (PM2.5) Station Measurements](https://resourcewatch.org/data/explore/cit003anrt-Air-Quality-Measurements-PM-25)
* [Air Quality: Particulate Matter (PM10) Station Measurements](https://resourcewatch.org/data/explore/cit003bnrt-Air-Quality-Measurements-PM-10)
* [Air Quality: Sulfur Dioxide (SO₂) Station Measurements](https://resourcewatch.org/data/explore/cit003cairqualityso2)
* [Air Quality: Nitrogen Dioxide (NO₂) Station Measurements](https://resourcewatch.org/data/explore/cit003dnrt-Air-Quality-Measurements-NO)
* [Air Quality: Ozone (O₃) Station Measurements](https://resourcewatch.org/data/explore/cit003cnrt-Air-Quality-Measurements-O)
* [Air Quality: Carbon Monoxide (CO) Station Measurements](https://resourcewatch.org/data/explore/cit003fnrt-Air-Quality-Measurements-CO)
* [Air Quality: Black Carbon Station Measurements](https://resourcewatch.org/data/explore/cit003gnrt-Air-Quality-Measurements-BC_1)

This dataset was provided by the source as JSON dumps within a [Amazon Simple Storage Service(AWS S3)](https://openaq-fetches.s3.amazonaws.com/index.html). Unique locations of the obsertaions  were stored in a seperate Carto table. All chemicals were converted units and seperated before we uploaded it to Carto.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cit_003_air_quality/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run once daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Francis Gassert](https://www.wri.org/profile/francis-gassert), and is currently maintained by [Weiqi Zhou](https://www.wri.org/profile/weiqi-zhou).
