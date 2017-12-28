import ee
import time
from datetime import datetime
from google.cloud import storage 
import os
import rasterio


TASK_FINISHED_STATES = (ee.batch.Task.State.COMPLETED,
                        ee.batch.Task.State.FAILED,
                        ee.batch.Task.State.CANCELLED)


class getJsonEnv():
    """
    Grabs .env 
    """
    def __init__(self):
        
        with open('gcsPrivatekey.json','w') as f:
            f.write(os.getenv('GCS_JSON'))

        with open('geePrivatekey.json','w') as f:
            f.write(os.getenv('GEE_JSON'))

        
    


class assetManagement(object):
    ## Connects to bucket, upload the image and once it is ready transfers it to GEE associated collection.
    """ 
    ImageObject:
    {
    'sources':['/Users/alicia/Downloads/results%2Fhistorical%2Fdecadal_test_historical_1991_2000_hdds.tiff'],
    'gcsBucket':'gee-image-transfer',
    'collectionAsset':'users/Aliciaarenzana/testcollection',
    'assetName':'t2000',
    'bandNames':[{'id': 'R'}, {'id': 'G'}, {'id': 'B'}],
    'pyramidingPolicy':'MODE',
    'properties':{
        'my_imageProperties':'to add to the collection'
        }   
    }
    """
    def __init__(self,imageObject):
        """checks the image and sets up the properties """
        getJsonEnv()
        self.meta=imageObject
        self.imageNames=self.getImageName()
        self.sources = []
        
    def getImageName(self):
        """gets the listof names from sources"""
        return [os.path.basename(name) for name in self.meta['sources']]
    
    def checksImages(self):
        """Checks the images that we will compose have the same n bands as they are going to become one image and part of the image collection"""
        metadata=[]
        for image in self.meta['sources']:
            with rasterio.open(image) as src:
                metaData=src.meta
                
                assert metaData['driver'] == 'GTiff', "Driver is not supported: {0}".format(metaData['driver'])
                assert metaData['count'] == len(self.meta['bandNames']), "Nbands incorrect, expected: {0}, {1} provided".format(metaData['count'],len(self.meta['bandNames']))
                
                metadata.append({'dtype': metaData['dtype'], 'driver': metaData['driver'], 'nodata': metaData['nodata'], 'nBands': metaData['count'],'crs': src.crs.to_string()})
        
        assert len(set([item['dtype'] for item in metadata])) == 1, "Images list dtypes aren't compatibles. Expected: 1, {1} provided".format(metaData['count'],len(set([item['dtype'] for item in metadata])))
        assert len(set([item['driver'] for item in metadata])) == 1, "Images list drivers aren't compatibles. Expected: 1, 1 provided".format(metaData['count'],len(set([item['driver'] for item in metadata])))
        assert len(set([item['nodata'] for item in metadata])) == 1, "Images list nodata values aren't compatibles. Expected: 1, {1} provided".format(metaData['count'],len(set([item['nodata'] for item in metadata])))
        assert len(set([item['nBands'] for item in metadata])) == 1, "Images list nBands number aren't compatibles. Expected: 1, {1} provided".format(metaData['count'],len(set([item['nBands'] for item in metadata])))
        assert len(set([item['crs'] for item in metadata])) == 1, "Images list crs aren't compatibles. Expected: 1, {1} provided".format(metaData['count'],len(set([item['crs'] for item in metadata])))       
        return metadata[0]
                    
    
    def setUpCredentials(self):
        """Sets up the credentials"""
        credentials = ee.ServiceAccountCredentials(os.getenv('GEE_SACCOUNT'), 'geePrivatekey.json')
        ee.Initialize(credentials)
        #ee.data.createAssetHome('users/test-api')
        #ee.data.setAssetAcl('users/test-api/testcollection', '{"writers": ["alicia.arenzana@gmail.com"], "all_users_can_read" : true}')
        #ee.data.createAsset({'type': 'ImageCollection'}, 'users/test-api/testcollection')
        storage_client=storage.Client.from_service_account_json('gcsPrivatekey.json')
        return storage_client.get_bucket(self.meta['gcsBucket'])
    
    def uploadGCS(self, imageName):
        """Upload the image to google cloud storage"""
        imageIndex = self.imageNames.index(imageName)
        blob = self.gcsBucket.blob('{0}/{1}'.format(self.meta['collectionAsset'],imageName))
        blob.upload_from_filename(self.meta['sources'][imageIndex])
        blob.make_public()
        
        return {'primaryPath': 'gs://{gcsBucket}/{collectionName}/{imageNa}'.format(gcsBucket=self.meta['gcsBucket'],collectionName=self.meta['collectionAsset'],imageNa=imageName)}
        
    def transferGEE(self):
        """Transfers the images from google cloud storage to gee asset"""
        task_id = ee.data.newTaskId()[0]
        request = {
            'id':'{collectionAsset}/{assetName}'.format(collectionAsset= self.meta['collectionAsset'],assetName =self.meta['assetName']),
            'properties':self.meta['properties'],
            'tilesets': [{'sources': self.sources}],
            'pyramidingPolicy':self.meta['pyramidingPolicy'].upper(),
            'bands':self.meta['bandNames']
        }
        print('______________________________________')
        print(request)
        print('______________________________________')
        ee.data.startIngestion(task_id, request, True)
        return task_id
    
    def taskStatus(self, task_id, timeout=90, log_progress=True):
        """Waits for the specified task to finish, or a timeout to occur."""
        start = time.time()
        elapsed = 0
        last_check = 0
        while True:
            elapsed = time.time() - start
            status = ee.data.getTaskStatus(task_id)[0]
            state = status['state']
            if state in TASK_FINISHED_STATES:
              error_message = status.get('error_message', None)
              print('Task %s ended at state: %s after %.2f seconds'
                    % (task_id, state, elapsed))
              if error_message:
                print('Error: %s' % error_message)
                raise
              return
            if log_progress and elapsed - last_check >= 30:
              print('[{:%H:%M:%S}] Current state for task {}: {}'
                    .format(datetime.now(), task_id, state))
              last_check = elapsed
            remaining = timeout - elapsed
            if remaining > 0:
              time.sleep(min(10, remaining))
            else:
              break
        print('Wait for task %s timed out after %.2f seconds' % (task_id, elapsed))





    def execute(self):
        #Checks if the images are correct
        self.checksImages()
        
        #sets up credentials
        self.gcsBucket=self.setUpCredentials()
        
        #Uploads file/s to GCS
        self.sources = list(map(self.uploadGCS, self.imageNames))
        
        #Transfers it from GCS to GEE
        task_id = self.transferGEE()
        
        print('TaskID: {0}'.format(task_id))
        print('Status: {0}'.format(ee.data.getTaskStatus(task_id)[0]))
        self.taskStatus(task_id)
        