## CO₂ Concentrations Dataset Near Real-time Script
This file describes the near real-time script that retrieves and processes the [AIRS3C2M: AIRS/Aqua L3 Monthly CO₂ in the free troposphere (AIRS-only) 2.5 degrees x 2 degrees V005 dataset](https://disc.gsfc.nasa.gov/datasets/AIRS3C2M_005/summary) for [display on Resource Watch](https://resourcewatch.org/data/explore/).

This dataset was provided by the source as a hdf file. The data shown on Resource Watch can be found in the 'CO2:mole_fraction_of_carbon_dioxide_in_free_troposphere' variable of the hdf file. This variable was converted to a tif file so that it could be uploaded to Google Earth Engine.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/cli_012_co2_concentrations/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was originally written by [Francis Gassert](https://www.wri.org/profile/francis-gassert), and is currently maintained by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).