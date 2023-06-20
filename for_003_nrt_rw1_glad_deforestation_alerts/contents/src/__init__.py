import os
import sys
import datetime
import logging
import requests
import ee
import re
import time
import json

# name of image collection in GEE where we will upload the final data
EE_COLLECTION = 'projects/resource-watch-gee/for_003_nrt_rw1_glad_deforestation_alerts'

# name of the image collection in GEE where the original data is stored  
EE_COLLECTION_ORI = 'projects/glad/alert/UpdResult'

# do you want to delete everything currently in the GEE collection when you run this script?
CLEAR_COLLECTION_FIRST = False

# how many assets can be stored in the GEE collection before the oldest ones are deleted?
MAX_ASSETS = 2

# format of date (used in both the source data files and GEE)
DATE_FORMAT = '%Y%m%d'

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = '6ec78a52-3fb2-478f-a02b-abafa5328244'

'''
FUNCTIONS FOR ALL DATASETS

The functions below must go in every near real-time script.
Their format should not need to be changed.
'''

def lastUpdateDate(dataset, date):
    '''
    Given a Resource Watch dataset's API ID and a datetime,
    this function will update the dataset's 'last update date' on the API with the given datetime
    INPUT   dataset: Resource Watch API dataset ID (string)
            date: date to set as the 'last update date' for the input dataset (datetime)
    '''
    # generate the API url for this dataset
    apiUrl = 'http://api.resourcewatch.org/v1/dataset/{0}'.format(dataset)
    # create headers to send with the request to update the 'last update date'
    headers = {
    'Content-Type': 'application/json',
    'Authorization': os.getenv('apiToken')
    }
    # create the json data to send in the request
    body = {
        "dataLastUpdated": date.isoformat() # date should be a string in the format 'YYYY-MM-DDTHH:MM:SS'
    }
    # send the request
    try:
        r = requests.patch(url = apiUrl, json = body, headers = headers)
        logging.info('[lastUpdated]: SUCCESS, '+ date.isoformat() +' status code '+str(r.status_code))
        return 0
    except Exception as e:
        logging.error('[lastUpdated]: '+str(e))
        
'''
FUNCTIONS FOR RASTER DATASETS

The functions below must go in every near real-time script for a RASTER dataset.
Their format should not need to be changed.
'''
def getLastUpdate(dataset):
    '''
    Given a Resource Watch dataset's API ID,
    this function will get the current 'last update date' from the API
    and return it as a datetime
    INPUT   dataset: Resource Watch API dataset ID (string)
    RETURN  lastUpdateDT: current 'last update date' for the input dataset (datetime)
    '''
    # generate the API url for this dataset
    apiUrl = 'http://api.resourcewatch.org/v1/dataset/{}'.format(dataset)
    # pull the dataset from the API
    r = requests.get(apiUrl)
    # find the 'last update date'
    lastUpdateString=r.json()['data']['attributes']['dataLastUpdated']
    if lastUpdateString == None:
        return None 
    else:
        # split this date into two pieces at the seconds decimal so that the datetime module can read it:
        # ex: '2020-03-11T00:00:00.000Z' will become '2020-03-11T00:00:00' (nofrag) and '000Z' (frag)
        nofrag, frag = lastUpdateString.split('.')
        # generate a datetime object
        nofrag_dt = datetime.datetime.strptime(nofrag, "%Y-%m-%dT%H:%M:%S")
        # add back the microseconds to the datetime
        lastUpdateDT = nofrag_dt.replace(microsecond=int(frag[:-1])*1000)
        return lastUpdateDT

def getLayerIDs(dataset):
    '''
    Given a Resource Watch dataset's API ID,
    this function will return a list of all the layer IDs associated with it
    INPUT   dataset: Resource Watch API dataset ID (string)
    RETURN  layerIDs: Resource Watch API layer IDs for the input dataset (list of strings)
    '''
    # generate the API url for this dataset - this must include the layers
    apiUrl = 'http://api.resourcewatch.org/v1/dataset/{}?includes=layer'.format(dataset)
    # pull the dataset from the API
    r = requests.get(apiUrl)
    #get a list of all the layers
    layers = r.json()['data']['attributes']['layer']
    # create an empty list to store the layer IDs
    layerIDs =[]
    # go through each layer and add its ID to the list
    for layer in layers:
        # only add layers that have Resource Watch listed as its application
        if layer['attributes']['application']==['rw']:
            layerIDs.append(layer['id'])
    return layerIDs

def flushTileCache(layer_id):
    """
    Given the API ID for a GEE layer on Resource Watch,
    this function will clear the layer cache.
    If the cache is not cleared, when you view the dataset on Resource Watch, old and new tiles will be mixed together.
    INPUT   layer_id: Resource Watch API layer ID (string)
    """
    # generate the API url for this layer's cache
    apiUrl = 'http://api.resourcewatch.org/v1/layer/{}/expire-cache'.format(layer_id)
    # create headers to send with the request to clear the cache
    headers = {
    'Content-Type': 'application/json',
    'Authorization': os.getenv('apiToken')
    }

    # clear the cache for the layer
    # sometimetimes this fails, so we will try multiple times, if it does

    # specify that we are on the first try
    try_num=1
    # specify the maximum number of attempt we will make
    tries = 4
    while try_num<tries:
        try:
            # try to delete the cache
            r = requests.delete(url = apiUrl, headers = headers, timeout=1000)
            # if we get a 200, the cache has been deleted
            # if we get a 504 (gateway timeout) - the tiles are still being deleted, but it worked
            if r.ok or r.status_code==504 or r.status_code==500:
                logging.info('[Cache tiles deleted] for {}: status code {}'.format(layer_id, r.status_code))
                return r.status_code
            # if we don't get a 200 or 504:
            else:
                # if we are not on our last try, wait 60 seconds and try to clear the cache again
                if try_num < (tries-1):
                    logging.info('Cache failed to flush: status code {}'.format(r.status_code))
                    time.sleep(60)
                    logging.info('Trying again.')
                # if we are on our last try, log that the cache flush failed
                else:
                    logging.error('Cache failed to flush: status code {}'.format(r.status_code))
                    logging.error('Aborting.')
            try_num += 1
        except Exception as e:
            logging.error('Failed: {}'.format(e))
            
'''
FUNCTIONS FOR THIS DATASET

The functions below have been tailored to this specific dataset.
They should all be checked because their format likely will need to be changed.
'''
def getAssetName(date):
    '''
    create asset name for the mosaicked images 
    INPUT   date: date (datetime)
    RETURN  GEE asset name for the final processed image (string)
    '''
    return '/'.join([EE_COLLECTION, 'for_003_nrt_rw1_glad_deforestation_alerts_{}'.format(date.strftime(DATE_FORMAT))])
                         
def getDate(image):
    '''
    get date from the asset id of the image in the RW image collection (last 8 characters of asset ids)
    INPUT   image: the asset id of image that ends in a date of the format YYYYMMDD (string)
    RETURN  date (string)
    '''
    return image[-8:]

def getNewDates(exclude_dates):
    '''
    Get new dates we want to try to fetch data for
    INPUT   exclude_dates: list of dates that we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
    RETURN  new_dates: the date of data we want to try to get, in the format of the DATE_FORMAT variable (list of strings)
    '''
    # create empty list to store dates we want to fetch
    new_dates = []
    # start with yesterday since today's data may not be availabele yet 
    date = datetime.date.today() - datetime.timedelta(days=1)
    # if the current date string is not in the list of dates we already have
    # add the date to the list of new dates to try and fetch 
    if date.strftime(DATE_FORMAT) not in exclude_dates:
        # generate a string from the date
        datestr = date.strftime(DATE_FORMAT)
        # add to list of new dates
        new_dates.append(datestr)
    else:
        logging.info('latest data already available in RW')
    return new_dates

def fetch(new_dates):
    '''
    Fetch files by datestamp
    INPUT   new_dates: list of dates we want to try to fetch, in the format YYYYMMDD (list of strings)
    RETURN  files: a dictionary that stores a list of asset ids for each new date in the image collection (dictionary)
    '''
    # list all the available assets in the GEE image collection that stores original data 
    file_list = [image['id'] for image in ee.data.getList({'id': EE_COLLECTION_ORI})]
    # make an empty dictionary to store asset ids for each new date 
    files = {}
    if new_dates:
        # go through each input date
        for date in new_dates:
            # create a regular expression to search with in the list of available asset ids
            search = '.*{mm}_{dd}_.*'.format(mm = date[-4:-2], dd = date[-2:])
            # each new date will be a key while the list of corresponding asset ids will be the value 
            files[date] = list((filter(re.compile(search).match, file_list)))
            logging.info('Finding {} files for data of {}'.format(len(files[date]), date))   
    return files
        
def mosaic(files):
    '''
    Mosaic the images of different regions into a single image for each date 
    INPUT   files: a dictionary that stores a list of asset ids for each new date in the image collection (dictionary)
    RETURN  image_mosaicked: list of mosaicked images (GEE images)
    '''
    if files:
        # create an empty list to store mosaicked images 
        image_mosaicked= []
        # go through each key,value pair in the dictionary 
        for k, v in files.items():
            # determine the band that's showing the alerts in the current year 
            band = 'conf{yy}'.format(yy = k[2:4])
            # mosaic the selected band of the images of the five different regions 
            mosaicked = ee.ImageCollection(v).select(band).mosaic()
            # since the data is encoded as no loss (0), probable loss (2), confirmed loss (3)
            # and we are only interested in the alerts 
            # we create a mask of all the cells whose values are greater than 0
            mask_alerts = mosaicked.gt(0)
            # mask the mosaicked image and rename the band to be 'b1'
            mosaicked = mosaicked.mask(mask_alerts).select([band], ['b1'])
            # add the masked mosaicked image to the list of processed images 
            image_mosaicked.append(ee.Image(mosaicked))
            
        return image_mosaicked
    else:
        return []
    
def processNewData(existing_dates):
    '''
    fetch, process, upload, and clean new data
    INPUT   existing_dates: list of dates we already have in GEE, in the format of the DATE_FORMAT variable (list of strings)
    RETURN  asset_pro: the id of the new GEE asset that has been created (string)
    '''
    # Get list of new dates we want to try to fetch data for
    new_dates = getNewDates(existing_dates)

    # Fetch the asset ids of the new images 
    logging.info('Fetching files')
    files = fetch(new_dates)

    # If we have successfully been able to fetch new data files
    if files:
        # convert images of different regions into one single image 
        logging.info('Converting files')
        # mosaic the images for each new date 
        images = mosaic(files)
        # the extent of the data we want to export 
        bounds = ee.Geometry.Rectangle([-179.999, -90, 180, 90], 'EPSG:4326', False)
        # create an asset id for the composite image
        asset_pro = getAssetName(datetime.date.today()-datetime.timedelta(days=1))
        # create a task to export the processed image to an asset in the corresponding GEE image collection 
        task = ee.batch.Export.image.toAsset(image=images[0],  
                                     description='export mosaicked image to asset',
                                     region=bounds,
                                     pyramidingPolicy= {'b1': 'SAMPLE'},
                                     scale=30,
                                     maxPixels=1e12,
                                     assetId=asset_pro)
        logging.info('Creating asset {}'.format(asset_pro))
        # start the task to export the mosaicked image to an asset
        task.start()
        # set the state to 'RUNNING' because we have started the task
        state = 'RUNNING'
        # set a start time to track the time it takes to upload the image
        start = time.time()
        # wait for task to complete, but quit if it takes more than 43200 seconds
        while state == 'RUNNING' and (time.time() - start) < 43200:
            # wait for 20 minutes before checking the state
            time.sleep(1200)
            # check the status of the upload
            status = task.status()['state']
            logging.info('Current Status: ' + status +', run time (min): ' + str((time.time() - start)/60))
            # log if the task is completed and change the state
            if status == 'COMPLETED':
                state = status
                logging.info(status)
            # log an error if the task fails and change the state
            elif status == 'FAILED':
                state = status
                logging.error(task.status()['error_message'])
                logging.debug(task.status())
        return asset_pro

def checkCreateCollection(collection):
    '''
    List assests in collection if it exists, else create new collection
    INPUT   collection: GEE collection to check or create (string)
    RETURN  list of assets in collection (list of strings)
    '''
    # if collection exists, return list of assets in collection
    if ee.data.getInfo(collection) != None:
        return [image['id'] for image in ee.data.getList({'id': EE_COLLECTION})]
    # if collection does not exist, create it and return an empty list (because no assets are in the collection)
    else:
        logging.info('{} does not exist, creating'.format(collection))
        ee.data.createAsset({'type': 'ImageCollection'}, collection)
        # set image collection's privacy to public
        acl = {"all_users_can_read": True}
        ee.data.setAssetAcl(collection, acl)
        print('Privacy set to public.')
        return []

def deleteExcessAssets(dates, max_assets):
    '''
    Delete oldest assets, if more than specified in max_assets variable
    INPUT   dates: dates for all the assets currently in the GEE collection; dates should be in the format specified
                    in DATE_FORMAT variable (list of strings)
            max_assets: maximum number of assets allowed in the collection (int)
    '''
    # sort the list of dates so that the oldest is first
    dates.sort()
    # if we have more dates of data than allowed,
    if len(dates) > max_assets:
        # go through each date, starting with the oldest, and delete until we only have the max number of assets left
        for date in dates[:-max_assets]:
            ee.data.deleteAsset(getAssetName(datetime.datetime.strptime(date, DATE_FORMAT)))

def get_most_recent_date(collection):
    '''
    Get most recent date from the data in the GEE collection
    INPUT   collection: GEE collection to check dates for (string)
    RETURN  most_recent_date: most recent date in GEE collection (datetime)
    '''
    # get list of assets in collection
    existing_assets = checkCreateCollection(collection)
    # get a list of strings of dates in the collection
    existing_dates = [a[-8:] for a in existing_assets]
    # sort these dates oldest to newest
    existing_dates.sort()
    # get the most recent date (last in the list) and turn it into a datetime
    most_recent_date = datetime.datetime.strptime(existing_dates[-1], DATE_FORMAT)
    return most_recent_date

def create_headers():
    '''
    Create headers to perform authorized actions on API

    '''
    return {
        'Content-Type': "application/json",
        'Authorization': "{}".format(os.getenv('apiToken')),
    }

def pull_layers_from_API(dataset_id):
    '''
    Pull dictionary of current layers from API
    INPUT   dataset_id: Resource Watch API dataset ID (string)
    RETURN  layer_dict: dictionary of layers (dictionary of strings)
    '''
    # generate url to access layer configs for this dataset in back office
    rw_api_url = 'https://api.resourcewatch.org/v1/dataset/{}/layer?page[size]=100'.format(dataset_id)
    # request data
    r = requests.get(rw_api_url)
    # convert response into json and make dictionary of layers
    layer_dict = json.loads(r.content.decode('utf-8'))['data']
    return layer_dict

def update_layer(layer, new_date):
    '''
    Update layers in Resource Watch back office.
    INPUT   layer: layer that will be updated (string)
            new_date: date of asset to be shown in this layer (datetime)
    '''
    # get current layer titile
    cur_title = layer['attributes']['name']

    # isolate the part of the layer title that indicates time period of the data 
    old_date_text = cur_title.replace(' GLAD Deforestation Alerts', '')

    # get text for the new dates 
    new_date_text = '{}-{}'.format(datetime.datetime.strftime(datetime.datetime(new_date.year,1,1), "%B %d, %Y"),
                                   datetime.datetime.strftime(new_date, "%B %d, %Y"))

    # replace dates in layer's title with new dates
    layer['attributes']['name'] = layer['attributes']['name'].replace(old_date_text, new_date_text)

    # replace the asset id in the layer def with new asset id
    layer['attributes']['layerConfig']['assetId'] = getAssetName(new_date)

    # send patch to API to replace layers
    # generate url to patch layer
    rw_api_url_layer = "https://api.resourcewatch.org/v1/dataset/{dataset_id}/layer/{layer_id}".format(
        dataset_id=layer['attributes']['dataset'], layer_id=layer['id'])
    # create payload with new title and layer configuration
    payload = {
        'application': ['rw'],
        'name': layer['attributes']['name'],
        'layerConfig': layer['attributes']['layerConfig']
    }
    # patch API with updates
    r = requests.request('PATCH', rw_api_url_layer, data=json.dumps(payload), headers=create_headers())
    # check response
    # if we get a 200, the layers have been replaced
    # if we get a 504 (gateway timeout) - the layers are still being replaced, but it worked
    # if we get a 503 - the layers are still being replaced, but it worked
    if r.ok or r.status_code==504 or r.status_code==503:
        logging.info('Layer replaced: {}'.format(layer['id']))
    else:
        logging.error('Error replacing layer: {} ({})'.format(layer['id'], r.status_code))

def updateResourceWatch():
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date', flushing the tile cache, and updating any dates on layers
    '''
    # get the most recent date from the data in the GEE collection
    most_recent_date = get_most_recent_date(EE_COLLECTION)
    # get the current 'last update date' from the dataset on Resource Watch
    current_date = getLastUpdate(DATASET_ID)
    # update the dates on layer legends
    logging.info('Updating {}'.format(EE_COLLECTION))
    # pull dictionary of current layers from API
    layer_dict = pull_layers_from_API(DATASET_ID)
    # go through each layer, pull the definition and update
    for layer in layer_dict:
        # replace layer title with new dates
        update_layer(layer, most_recent_date)
    # If the most recent date from the GEE collection does not match the 'last update date' on the RW API, update it
    if current_date != most_recent_date:
        logging.info('Updating last update date and flushing cache.')
        # update dataset's last update date on Resource Watch
        lastUpdateDate(DATASET_ID, most_recent_date)
        # get layer ids and flush tile cache for each
        layer_ids = getLayerIDs(DATASET_ID)
        for layer_id in layer_ids:
            flushTileCache(layer_id)
            
def initialize_ee():
    '''
    Initialize ee module
    '''
    # get GEE credentials from env file
    GEE_JSON = os.environ.get("GEE_JSON")
    _CREDENTIAL_FILE = 'credentials.json'
    GEE_SERVICE_ACCOUNT = os.environ.get("GEE_SERVICE_ACCOUNT")
    with open(_CREDENTIAL_FILE, 'w') as f:
        f.write(GEE_JSON)
    auth = ee.ServiceAccountCredentials(GEE_SERVICE_ACCOUNT, _CREDENTIAL_FILE)
    ee.Initialize(auth)


def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # initialize ee modules for uploading to Google Earth Engine
    initialize_ee()

    # Clear the GEE collection, if specified above
    if CLEAR_COLLECTION_FIRST:
        if ee.data.getInfo(EE_COLLECTION) != None:
            ee.data.deleteAsset(EE_COLLECTION)

    # Check if collection exists, create it if it does not
    # If it exists return the list of assets currently in the collection
    existing_assets = checkCreateCollection(EE_COLLECTION)
    # Get a list of the dates of data we already have in the collection
    existing_dates = [getDate(a) for a in existing_assets]

    # Fetch, process, and upload the new data
    new_asset = processNewData(existing_dates)
    # Get the date of the new data we have added
    new_date = getDate(new_asset)

    print('Previous asset: {}, new: {}, max: {}'.format(
        len(existing_dates), new_date, MAX_ASSETS))

    # Delete excess assets
    deleteExcessAssets(existing_dates + [new_date], MAX_ASSETS)

    # Update Resource Watch
    updateResourceWatch()

    logging.info('SUCCESS')
