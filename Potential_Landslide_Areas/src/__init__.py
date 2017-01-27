import os
import requests
import json
from lxml import html
from bs4 import BeautifulSoup

class bcolors:
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    ENDC = '\033[0m'
    UNDERLINE = '\033[4m'
   
dataUrl='https://trmm.gsfc.nasa.gov/trmm_rain/Events/latest_7_day_landslide.html'
cartoBaseUrl='https://insights.carto.com/api/v1/sql?'
args = {'q':'', 'api_key': os.getenv('CARTO_API_KEY')} 
baseStr = "INSERT INTO potential_landslide_areas (dates,hour,type,the_geom,distance_km) VALUES ('"
rows=[]
row=[]
r = requests.get(dataUrl)
tree = BeautifulSoup(r.content, "lxml").pre.get_text().strip()
test=tree.split("\n")
for i in xrange(0,len(test)-1):
	data=[]
	row.append(test[i].split())
	# date
	data.append(row[i][0])
	# hour
	data.append(row[i][1])
	# type
	data.append(row[i][2]+' '+row[i][3])
	# lat 1
	data.append(row[i][6])
	# lon 1
	data.append(row[i][7])
	# distance
	data.append(row[i][9])
	# lat 2
	data.append(row[i][-1])
	# lon 2
	data.append(row[i][-2])
	
	rows.append(data)

strs=[]
for i in xrange(0,len(rows)-1):
	str_s= rows[i][0] + "', '" + rows[i][1] + "', '" + rows[i][2] + "', ST_SetSRID(ST_MakePoint(" + rows[i][4]+"," + rows[i][3]+  "), 4326), '" + rows[i][5]
	strs.append(str_s)

args['q']=baseStr + "'), ('".join(strs) + "')"

response = requests.get(cartoBaseUrl, params=args)
if response.status_code==200:
    print bcolors.OKGREEN+'SUCCESS'+bcolors.ENDC
else:
    print bcolors.WARNING+'UPLOAD PROCESS FAILURE STATUS CODE:' + response.status_code+bcolors.ENDC