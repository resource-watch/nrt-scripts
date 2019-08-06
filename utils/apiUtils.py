import logging
import requests


def lastUpdateDate(dataset, date):
   apiUrl = 'http://api.resourcewatch.org/v1/dataset/{dataset}'
   headers = {
   'Content-Type': 'application/json',
   'Authorization': os.getenv('apiToken')
   }
   body = {
       "dataLastUpdated": date.isoformat()
   }
   try:
       r = requests.patch(url = apiUrl, json = body, headers = headers)
       logging.info('[lastUpdated]: SUCCESS, '+ date.isoformat() +' status code '+str(r.status_code))
       return 0
   except Exception as e:
       logging.error('[lastUpdated]: '+str(e))

def flushTileCache(dataset, layer):
   apiUrl = 'http://api.resourcewatch.org/v1/dataset/{dataset}/layer/{layer}'
   headers = {
   'Content-Type': 'application/json',
   'Authorization': os.getenv('apiToken')
   }
   try:
       r = requests.delete(url = apiUrl, headers = headers)
       logging.info('[Cache tiles deleted]: status code '+str(r.status_code))
       return r.status_code
   except Exception as e:
       logging.error('[lastUpdated]: '+str(e))