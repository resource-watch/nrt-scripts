import logging
import sys
import requests as req
import json
from . import eeUtil
import os
from datetime import datetime

RW_API_TOKEN = os.environ['rw_api_token']
CLI_035_API_ID = 'f4875c0e-9b2b-4f86-a338-f63dc5b33863'
LOG_LEVEL = logging.INFO
DATE_FORMAT = '%Y-%m-%d'
EE_COLLECTION = 'cli_035_surface_temp_analysis'

def formatDate(asset):
    dt = os.path.splitext(os.path.basename(asset))[0][-8:]
    dt = datetime.strptime(dt, '%Y%m%d').strftime(DATE_FORMAT)
    return(dt)

def update_layers(available_dates):
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    # sort existing dates
    available_dates = sorted(available_dates, reverse=True)

    rw_api_url = 'https://api.resourcewatch.org/v1/dataset/{}/layer'.format(CLI_035_API_ID)
    layers = req.get(rw_api_url).json()['data']
    for layer in layers:
        layer_id = layer['id']
        logging.info('Layer ID: {}'.format(layer_id))

        most_recent = layer['attributes']['layerConfig']['most_recent']

        ### DANGER HERE: must be at least as many available dates as layers,
        # or this will throw a list index out of range error
        asset = available_dates[most_recent-1]

        new_description = 'Data corresponds to asset {}'.format(asset)
        new_layer_metadata = layer['attributes'].copy()
        layerConfig = new_layer_metadata['layerConfig'].copy()
        layerConfig.update(assetId='users/resourcewatch_wri/{}/{}'.format(EE_COLLECTION,asset),
                           dateTime=formatDate(asset))
        new_layer_metadata.update(name='Data for asset {}'.format(asset),
                                  description=new_description,
                                  layerConfig=layerConfig)

        patch_url = 'https://api.resourcewatch.org/v1/dataset/{}/layer/{}'.format(CLI_035_API_ID, layer_id)
        headers = {
            'content-type': "application/json",
            'authorization': "Bearer {}".format(RW_API_TOKEN)
        }
        logging.info('URL: {}'.format(patch_url))
        # Tried json.dumps(layer) and that didn't work
        res = req.request("PATCH", patch_url, data=json.dumps(new_layer_metadata), headers = headers)
        logging.info(res.text)

    logging.info('LAYER UPDATE SUCCESS')
