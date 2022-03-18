## Power observatory Mexico update nodes-load_zones location script
This file describes the near real-time script that retrieves and processes the [Local Marginal Prices datasets]() for display on Resource Watch as the following datasets:

* [Mexico Energy Local Marginal Prices ](https://bit.ly/3tjnuF8)
* [Mexico Energy Load assigned ](https://bit.ly/3N347bt)

This dataset was provided by the source as an excel file. The excel file was transformed into a table and the resulting tables were then uploaded to Carto. One table holds the node level data and the other the load zones data.

This script fetches the location of the nodes and load zones in Mexico.

Please see the [Python script](https://github.com/resource-watch/nrt-scripts/blob/master/loc_mxene_mexico_nodes_catalog/contents/src/__init__.py) for more details on this processing.

**Schedule**

This script is run daily. The exact time that the script is run to update the dataset can be found in the the `time.cron` file. This time is in Coordinated Universal Time (UTC), expressed in cron format.

###### Note: This script was adapted to NRT form by [Eduardo Castillero Reyes](https://wrimexico.org/profile/eduardo-castillero-reyes) based on scripts developed by an external consultant.
