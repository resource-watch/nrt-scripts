import os
import requests
import tinys3
import csv
from lxml import html
from bs4 import BeautifulSoup

class bcolors:
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    ENDC = '\033[0m'
    UNDERLINE = '\033[4m'

dataUrl='http://floodobservatory.colorado.edu/Archives/MasterListrev.htm'

def my_parse(html):
    records = []
    table2 = BeautifulSoup(html, "lxml").find_all('table')[0]
    for tr in table2.find_all('tr'):
        tds = tr.find_all('td')
        records.append([elem.text.encode('utf-8').replace("\r\n", "").replace("\xc2\xa0","").replace("#N/A","").replace("Centroid X","longitude").replace("Centroid Y","latitude") for elem in tds])
    return records


r = requests.get(dataUrl)
data = my_parse(r.content)
with open('flood_observatory.csv', 'wb') as f:
    writer = csv.writer(f)
    writer.writerows(data)

conn = tinys3.Connection(os.getenv('S3_ACCESS_KEY'),os.getenv('S3_SECRET_KEY'),default_bucket=os.getenv('BUCKET'), tls=True)

# So we could skip the bucket parameter on every request

f = open('flood_observatory.csv','rb')
response = conn.upload('/flood_observatory.csv',f)

if response.status_code==200:
    print bcolors.OKGREEN+'SUCCESS'+bcolors.ENDC
else:
    print bcolors.WARNING+'UPLOAD PROCESS FAILURE STATUS CODE:' + str(response.status_code)+bcolors.ENDC
    print response.content
