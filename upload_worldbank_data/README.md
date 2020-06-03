## Batch Upload of World Bank Datasets
This file describes the data upload process used in this docker container to upload a number of datasets from the [World Bank API](https://data.worldbank.org/) to [Resource Watch](resourcewatch.org).

On Resource Watch, we host numerous datasets from the World Bank, which are all available through their API in a standard format. You can see some of the datasets available from the World Bank [here](https://data.worldbank.org/indicator/). We use this script to update all of the datasets on Resource Watch that come from World Bank datasets at once.

Below, we describe the steps used to process the data from the World Bank API.

1. A table is created that contains the dataset names, the names of the data tables in Carto (where we store the data), and the corresponding World Bank indicator code for each dataset. Each dataset on the World Bank API has a unique indicator code; for example, the [Unemployment Rate page](https://data.worldbank.org/indicator/SL.UEM.TOTL.ZS) indicator code is "SL.UEM.TOTL.ZS," which can be found at the end of the URL.
2. For each dataset we download the dataset from the World Bank API, using an API query. For the Unemployment Rate dataset, the query is "http://api.worldbank.org/countries/all/indicators/SL.UEM.TOTL.ZS?format=json&per_page=10000"
3. Regions that include multiple countries, such as the European Union, are removed from the data so that we are left with only country-level data.
4. Every country is matched with Resource Watch's standard country names using its ISO3 code.
5. The formatted data table is uploaded to the Resource Watch Carto account.

Please see the [Python script](https://github.com/resource-watch/data-pre-processing/blob/master/upload_worldbank_data/contents/main.py) for more details on this processing.

###### Note: This dataset processing was done by [Kristine Lister](https://www.wri.org/profile/kristine-lister), and QC'd by [Amelia Snyder](https://www.wri.org/profile/amelia-snyder).
