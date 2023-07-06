## Batch Upload of EIA Datasets

This file describes the process used to upload datasets from the [EIA API](https://www.eia.gov/opendata/) to [Resource Watch](https://resourcewatch.org). On Resource Watch, we host numerous datasets from the EIA, which are all available through their API in a standard format. You can see some of the datasets available from the EIA [here](https://www.eia.gov/international/data/world). We use this script to update all of the datasets on Resource Watch that come from EIA datasets at once.

The EIA data that are processed by this script can be found in the [EIA_RW_dataset_names_ids.csv](https://github.com/resource-watch/nrt-scripts/blob/master/upload_eia_data/EIA_RW_dataset_names_ids.csv), along with their corresponding Resource Watch API IDS and other infomation we use for the processing.

The data was provided by the source through its API in a json format. Below, we describe the steps used to process the data from the EIA API.

1. For each dataset, we read in the data as a pandas dataframe.
2. Delete rows without data, and remove data of regions that consist of multiple geographies. 
3. Create a new column 'datetime' to store the time period of the data as the first date of the year. 
4. The formatted data table is uploaded to the Resource Watch Carto account.

Each time this script is run, it updates all Carto tables for these datasets and also makes new layers on Resource Watch, if any new years of data have been added. Layers on Resource Watch are not created for years that have fewer than 10 data points and are also more than ten years old. For datasets that are not "timelines" and instead show a single year of data with layers for multiple indicators, the existing layers on Resource Watch are updated to the latest year of data available.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/upload_eia_data/contents/main.py) for more details on this processing.

**Adding new EIA datasets to Resource Watch**

Any time a new EIA dataset is added to Resource Watch, it should be added using this script, if at all possible. To add a new dataset to this script, you can follow these steps:

1. Add a row to the [EIA_RW_dataset_names_ids.csv](https://github.com/resource-watch/nrt-scripts/blob/master/upload_eia_data/EIA_RW_dataset_names_ids.csv) with the required information (you do not need to add the Resource Watch dataset ID yet). Please note that each dataset on the EIA API has a unique combination of activityId, productId, and unit; for example, the [Total Energy Production](https://www.eia.gov/international/data/world/total-energy/total-energy-production) activityId is 1, productId is 44, and unit is QBTU, which can be found at the [API Dashboard](https://www.eia.gov/opendata/browser/international).
2. Run the main() function of the [update_eia_data_on_carto.py](https://github.com/resource-watch/nrt-scripts/blob/master/upload_eia_data/contents/src/update_eia_data_on_carto.py) script, which will create a Carto table for this dataset.
3. Make a dataset in the Resource Watch back office for this table and create at least one layer.
4. Add the Resource Watch ID to the [EIA_RW_dataset_names_ids.csv](https://github.com/resource-watch/nrt-scripts/blob/master/upload_eia_data/EIA_RW_dataset_names_ids.csv).
5. Run the main() funtion of the [update_eia_layers_on_rw.py](https://github.com/resource-watch/nrt-scripts/blob/master/upload_eia_data/contents/src/update_eia_layers_on_rw.py) script, which will duplicate the layer you created for all other years of available data.

After you have followed these steps, the dataset on Carto and Resource Watch will be automatically updated each week.

**Schedule**

This script is run weekly. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This dataset processing was done by [Weiqi Zhou](https://www.wri.org/profile/weiqi-zhou), and is currently maintained by [Weiqi Zhou](https://www.wri.org/profile/weiqi-zhou).
