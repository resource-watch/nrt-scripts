## Batch Upload of World Bank Datasets
This file describes the process used to upload datasets from the [World Bank API](https://data.worldbank.org/) to [Resource Watch](resourcewatch.org). On Resource Watch, we host numerous datasets from the World Bank, which are all available through their API in a standard format. You can see some of the datasets available from the World Bank [here](https://data.worldbank.org/indicator/). We use this script to update all of the datasets on Resource Watch that come from World Bank datasets at once.

The World Bank indicators that are processed by this script can be found in the [WB_RW_dataset_names_ids.csv](https://github.com/resource-watch/nrt-scripts/blob/master/upload_worldbank_data/WB_RW_dataset_names_ids.csv), along with their corresponding Resource Watch API IDS and other infomation we use for the processing.

Below, we describe the steps used to process the data from the World Bank API.

1. For each dataset, we download the dataset from the World Bank API using an API query and reformat the information into a table.
2. Regions that include multiple countries, such as the European Union, are removed from the data so that we are left with only country-level data.
3. Country names are replaced with Resource Watch's set of standardized country names.
5. The formatted data table is uploaded to the Resource Watch Carto account.

Each time this script is run, it updates all Carto tables for these datasets and also makes new layers on Resource Watch, if any new years of data have been added. Layers on Resource Watch are not created for years that have fewer than 10 data points and are also more than ten years old. For datasets that are not "timelines" and instead show a single year of data with layers for multiple indicators, the existing layers on Resource Watch are updated to the latest year of data available.

Please see the [Python script](https://github.com/resource-watch/data-pre-processing/blob/master/upload_worldbank_data/contents/main.py) for more details on this processing.

**Adding new World Bank datasets to Resource Watch**
Any time a new World Bank dataset is added to Resource Watch, it should be added using this script, if at all possible. To add a new dataset to this script, you can follow these steps:
1. Add a row to the [WB_RW_dataset_names_ids.csv](https://github.com/resource-watch/nrt-scripts/blob/master/upload_worldbank_data/WB_RW_dataset_names_ids.csv) with the required information (you do not need to add the Resource Watch dataset ID yet). Please note that each dataset on the World Bank API has a unique indicator code; for example, the [Unemployment Rate page](https://data.worldbank.org/indicator/SL.UEM.TOTL.ZS) indicator code is "SL.UEM.TOTL.ZS," which can be found at the end of the URL.
2. Run the main() function of the [update_worldbank_data_on_carto.py](https://github.com/resource-watch/nrt-scripts/blob/master/upload_worldbank_data/contents/src/update_worldbank_data_on_carto.py) script, which will create a Carto table for this dataset.
3. Make a dataset in the Resource Watch back office for this table and create at least one layer.
4. Add the Resource Watch ID to the [WB_RW_dataset_names_ids.csv](https://github.com/resource-watch/nrt-scripts/blob/master/upload_worldbank_data/WB_RW_dataset_names_ids.csv).
4. Run the main() funtion of the [update_worldbank_layers_on_rw.py](https://github.com/resource-watch/nrt-scripts/blob/master/upload_worldbank_data/contents/src/update_worldbank_layers_on_rw.py) script, which will duplicate the layer you created for all other years of available data.

After you have followed these steps, the dataset on Carto and Resource Watch will be automatically updated each week.

**Schedule**

This script is run weekly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This dataset processing was done by [Kristine Lister](https://www.wri.org/profile/kristine-lister), [Amelia Snyder](https://www.wri.org/profile/amelia-snyder), and [Nathan Suberi](https://www.wri.org/profile/nathan-suberi).
