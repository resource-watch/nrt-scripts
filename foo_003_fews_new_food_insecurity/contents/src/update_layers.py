import logging
import sys
import requests as req
import json
import cartosql
import os
from datetime import datetime

RW_API_TOKEN = os.environ['rw_api_token']
FOO_003_API_ID = 'b0f859ce-f13b-462e-9063-ebc68ed88420'
LOG_LEVEL = logging.INFO
DATE_FORMAT = '%Y-%m'
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

def update_layers():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    # 4. Update layer definitions - is this the best place to do so?
    rw_api_url = 'https://api.resourcewatch.org/v1/dataset/{}/layer'.format(FOO_003_API_ID)
    layers = req.get(rw_api_url).json()['data']
    for layer in layers:
        layer_id = layer['id']
        logging.info('Layer ID: {}'.format(layer_id))
        logging.info(layer['attributes']['layerConfig']['body']['layers'][0]['options'])

        most_recent = layer['attributes']['layerConfig']['body']['layers'][0]['options']['most_recent']
        sql = "(SELECT distinct {} from (SELECT {}, dense_rank() over (order by {} desc) as rn from foo_003_fews_net_food_insecurity where ifc_type = '{}') t where rn={})"

        date_start = cartosql.sendSql(sql.format('start_date', 'start_date', 'start_date', 'CS', most_recent))
        date_start = date_start.json()['rows'][0]['start_date']
        date_start = datetime.strptime(date_start, DATETIME_FORMAT).strftime(DATE_FORMAT)

        date_end = cartosql.sendSql(sql.format('end_date', 'end_date', 'end_date', 'CS', most_recent))
        date_end = date_end.json()['rows'][0]['end_date']
        date_end = datetime.strptime(date_end, DATETIME_FORMAT).strftime(DATE_FORMAT)

        new_description = 'Start date = {}, end date = {}'.format(date_start, date_end)

        new_layer_metadata = layer['attributes'].copy()
        new_layer_metadata.update(description=new_description, name='Data for CS beginning in {}'.format(date_start))
        layer.update(attributes=new_layer_metadata)

        patch_url = 'https://api.resourcewatch.org/v1/dataset/{}/layer/{}'.format(FOO_003_API_ID, layer_id)
        headers = {
            'content-type': "application/json",
            'authorization': "Bearer {}".format(RW_API_TOKEN)
        }
        logging.debug('URL: {}'.format(patch_url))
        # Tried json.dumps(layer) and that didn't work
        res = req.request("PATCH", patch_url, data=json.dumps(new_layer_metadata), headers = headers)
        logging.debug(res.text)

    logging.info('LAYER UPDATE SUCCESS')
