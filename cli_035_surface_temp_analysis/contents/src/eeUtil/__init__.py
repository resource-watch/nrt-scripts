'''
Python wrapper for easier data management on Google Earth Engine.
Files are staged via Google Cloud Storage for upload.
A service account with access to GEE and Storage is required.
See: https://developers.google.com/earth-engine/service_account
```
import eeUtil
# initialize from environment variables
eeUtil.init()
# create image collection
eeUtil.createFolder('mycollection', imageCollection=True)
# upload image to collection
eeUtil.upload('image.tif', 'mycollection/myasset')
eeUtil.setAcl('mycollection/myasset', 'public')
eeUtil.ls('mycollection')
```
'''
from __future__ import unicode_literals
import os
import ee
import logging
import time
import datetime
from google.cloud import storage

STRICT = True
GEE_SERVICE_ACCOUNT = os.environ.get("GEE_SERVICE_ACCOUNT")
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS")
GCS_PROJECT = os.environ.get("CLOUDSDK_CORE_PROJECT")
GEE_STAGING_BUCKET = os.environ.get("GEE_STAGING_BUCKET")

# Unary bucket object
_gsBucket = None
# Unary GEE home directory
_home = ''


def init(account=GEE_SERVICE_ACCOUNT,
         credential_path=GOOGLE_APPLICATION_CREDENTIALS,
         project=GCS_PROJECT, bucket=GEE_STAGING_BUCKET):
    '''
    Initialize EE and GS bucket connection with service credentials.
    Defaults to read from environment.
    `account`         Service account name. Will need access to both GEE and
                      storage
    `credential_path` Path to json file containing private key
    `project`         GCS project containing bucket
    `bucket`          Storage bucket for staging assets for ingestion
    https://developers.google.com/earth-engine/service_account
    '''
    global _gsBucket
    auth = ee.ServiceAccountCredentials(account, credential_path)
    ee.Initialize(auth)
    # GCS auth prefers to read from environment
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
    _gsBucket = storage.Client(project).bucket(bucket)
    if not _gsBucket.exists():
        logging.info('Bucket {} does not exist, creating'.format(bucket))
        _gsBucket.create()


def getHome():
    '''Get user root directory'''
    global _home
    _home = ee.data.getAssetRoots()[0]['id']
    logging.debug("Home: {}".format(_home))
    return _home

def _getHome():
    '''Cached get user root directory'''
    global _home
    return _home if _home else getHome()

def _path(path):
    '''Add user root directory to path if not already existing'''
    if path:
        if len(path) > 6 and path[:6] == 'users/':
            return path
        else:
            return os.path.join(_getHome(), path)
    return _getHome()


def getQuota():
    '''Get GEE usage quota'''
    return ee.data.getAssetRootQuota(_getHome())


def info(asset=''):
    '''Get asset info'''
    return ee.data.getInfo(_path(asset))


def exists(asset):
    '''Check if asset exists'''
    return True if info(asset) else False


def ls(path='', abspath=False):
    '''List assets in path'''
    if abspath:
        return [a['id']
                for a in ee.data.getList({'id': _path(path)})]
    else:
        return [os.path.basename(a['id'])
                for a in ee.data.getList({'id': _path(path)})]


def getAcl(asset):
    '''Get ACL of asset or folder'''
    return ee.data.getAssetAcl(_path(asset))


def setAcl(asset, acl='public'):
    '''Set ACL of asset
    `acl` ('public'|'private'|json ACL specification)
    '''
    if acl == 'public':
        acl = '{"all_users_can_read": true}'
    elif acl == 'private':
        acl = '{"all_users_can_read": false}'
    logging.debug('Setting ACL to {} on {}'.format(acl, asset))
    ee.data.setAssetAcl(_path(asset), acl)


def setProperties(asset, properties={}):
    '''Set asset properties'''
    return ee.data.setAssetProperties(_path(asset), properties)


def createFolder(path, imageCollection=False, overwrite=False,
                 public=False):
    '''Create folder or image collection'''
    ftype = (ee.data.ASSET_TYPE_IMAGE_COLL if imageCollection
             else ee.data.ASSET_TYPE_FOLDER)
    ee.data.createAsset({'type': ftype}, _path(path), overwrite)
    if public:
        setAcl(path)


def _checkTaskCompleted(task_id):
    '''Return status of task if completed else False'''
    status = ee.data.getTaskStatus(task_id)[0]
    if status['state'] in (ee.batch.Task.State.CANCELLED,
                           ee.batch.Task.State.FAILED):
        if 'error_message' in status:
            logging.error(status['error_message'])
        if STRICT:
            raise Exception(status)
        logging.error('Task ended with state {}'.format(status['state']))
        return status['state']
    elif status['state'] == ee.batch.Task.State.COMPLETED:
        return status['state']
    return False


def waitForTasks(task_ids, timeout=300):
    '''Wait for tasks to complete, fail, or timeout'''
    start = time.time()
    elapsed = 0
    while elapsed < timeout:
        elapsed = time.time() - start
        state = [_checkTaskCompleted(task) for task in task_ids]
        if all(state):
            return True
        time.sleep(5)
    logging.error('Tasks timed out after {} seconds'.format(timeout))
    if STRICT:
        raise Exception(task_ids)
    return False


def waitForTask(task_id, timeout=300):
    '''Wait for task to complete, fail, or timeout'''
    start = time.time()
    elapsed = 0
    while elapsed < timeout:
        elapsed = time.time() - start
        state = _checkTaskCompleted(task_id)
        if state == ee.batch.Task.State.COMPLETED:
            return True
        elif state:
            return False
        time.sleep(5)
    logging.error('Task timed out after {} seconds'.format(timeout))
    if STRICT:
        raise Exception(task_id)
    return False


def formatDate(date, dateformat='%Y%m%d'):
    '''Format date as ms since last epoch'''
    if isinstance(date, int):
        return date
    if isinstance(date, str):
        date = datetime.datetime.strptime(date, dateformat)
    seconds = (date - datetime.datetime.utcfromtimestamp(0)).total_seconds()
    return int(seconds * 1000)


def ingestAsset(gs_uri, asset, date='', wait_timeout=0):
    '''
    Upload asset from GS to EE
    `gs_uri`       should be formatted `gs://<bucket>/<blob>`
    `asset`        destination path
    `date`         optional date tag (datetime.datetime or int ms since epoch)
    `wait_timeout` if non-zero, wait timeout secs for task completion
    '''
    task_id = ee.data.newTaskId()[0]
    params = {'id': _path(asset),
              'tilesets': [{'sources': [{'primaryPath': gs_uri}]}]}
    if date:
        params['properties'] = {'system:time_start': formatDate(date),
                                'system:time_end': formatDate(date)}
    logging.debug('Ingesting {} to {}: {}'.format(gs_uri, asset, task_id))
    ee.data.startIngestion(task_id, params, True)
    if wait_timeout:
        waitForTask(task_id, wait_timeout)
    return task_id


def uploadAsset(filename, asset, gs_prefix='', date='', public=False,
                timeout=300, clean=True):
    '''
    Stage file to GS and ingest to EE
    `file`         local file paths
    `asset`        destination path
    `gs_prefix`    GS folder for staging (else files are staged to bucket root)
    `date`         Optional date tag (datetime.datetime or int ms since epoch)
    `public`       set acl public if True
    `timeout`      wait timeout secs for completion of GEE ingestion
    `clean`        delete files from GS after completion
    '''
    gs_uris = gsStage(filename, gs_prefix)
    try:
        ingestAsset(gs_uris[0], asset, date, timeout)
        if public:
            setAcl(asset, 'public')
    except Exception as e:
        logging.error(e)
    if clean:
        gsRemove(gs_uris)


def uploadAssets(files, assets, gs_prefix='', dates='', public=False,
                 timeout=300, clean=True):
    '''
    Stage files to GS and ingest to EE
    `files`        local file paths
    `assets`       destination paths
    `gs_prefix`    GS folder for staging (else files are staged to bucket root)
    `dates`        Optional date tags (datetime.datetime or int ms since epoch)
    `public`       set acl public if True
    `timeout`      wait timeout secs for completion of GEE ingestion
    `clean`        delete files from GS after completion
    '''
    gs_uris = gsStage(files, gs_prefix)
    task_ids = [ingestAsset(gs_uris[i], assets[i], dates[i], timeout)
                for i in range(len(files))]
    try:
        waitForTasks(task_ids, timeout)
        if public:
            for asset in assets:
                setAcl(asset, 'public')
    except Exception as e:
        logging.error(e)
    if clean:
        gsRemove(gs_uris)
    return assets


def removeAsset(asset, recursive=False):
    '''Delete asset from GEE'''
    if recursive:
        if info(asset)['type'] in (ee.data.ASSET_TYPE_FOLDER,
                                   ee.data.ASSET_TYPE_IMAGE_COLL):
            sub_assets = ls(asset, abspath=True)
            logging.debug("Sub assets of {}: {}".format(asset, sub_assets))
            for child in sub_assets:
                removeAsset(child)
    asset_path = _path(asset)
    logging.debug('Deleting asset {}'.format(asset_path))
    ee.data.deleteAsset(asset_path)

def gsStage(files, prefix=''):
    '''Upload files to GS with prefix'''
    files = (files,) if isinstance(files, str) else files
    if not _gsBucket:
        raise Exception('GS Bucket not initialized, run init()')
    gs_uris = []
    for f in files:
        path = os.path.join(prefix, os.path.basename(f))
        uri = 'gs://{}/{}'.format(_gsBucket.name, path)
        logging.debug('Uploading {} to {}'.format(f, uri))
        _gsBucket.blob(path).upload_from_filename(f)
        gs_uris.append(uri)
    return gs_uris


def gsRemove(gs_uris):
    '''
    Remove blobs from GS
    `gs_uris` must be full paths `gs://<bucket>/<blob>`
    '''
    if not _gsBucket:
        raise Exception('GS Bucket not initialized, run init()')
    paths = []
    for path in gs_uris:
        if path[:len(_gsBucket.name) + 5] == 'gs://{}'.format(_gsBucket.name):
            paths.append(path[6+len(_gsBucket.name):])
        else:
            raise Exception('Path {} does not match gs://{}/<blob>'.format(
                path, _gsBucket.name))
    # on_error null function to ignore NotFound
    logging.debug("Deleting {} from gs://{}".format(paths, _gsBucket.name))
    _gsBucket.delete_blobs(paths, lambda x:x)
