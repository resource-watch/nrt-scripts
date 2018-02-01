from __future__ import print_function, division
import os
import requests
import tinys3
import datetime
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
import fiona
import zipfile

def zipDir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))

def downloadFile(url, path):
    local_filename = path + url.split('/')[-1]
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
    return local_filename

# Download big file
def getFwsFile(path):
    zipFiles=[]

    url = 'https://rw-nrt-scripts.s3.amazonaws.com/fws.zip'
    zipFiles.append(downloadFile(url, path))

    for file in zipFiles:
        with zipfile.ZipFile(file,"r") as zip_ref:
            zip_ref.extractall(path)
        os.remove(file)

# Download fews files
def getFiles(date, path):
    regions = ['west-africa','southern-africa','central-asia','east-africa','caribbean-central-america']
    zipFiles=[]
    for region in regions:
        url = 'http://shapefiles.fews.net.s3.amazonaws.com/HFIC/WA/'+ region + date + '.zip'
        zipFiles.append(downloadFile(url, path))

    for file in zipFiles:
        print(file)
        zipfile.ZipInfo(file)
        with zipfile.ZipFile(file,"r") as zip_ref:
            zip_ref.extractall(path)
        os.remove(file)


# Get all downloaded files 

def fileList(dataPath):
    shpList=[]
    for root, dirs, files in os.walk(dataPath):
        for file in files:
            if file.endswith(".shp"):
                text = file.split('.')[0].split('_')
                if len(text)>2:
                    shpList.append({'path': os.path.join(root, file), 'stype': text[2], 'date': datetime.date(int(text[1][0:4]), int(text[1][4:6]), 1).isoformat(), 'region':text[0]})
                else:
                    shpList.append({'path': os.path.join(root, file), 'stype': text[1], 'date': datetime.date(int(text[0][-6::][0:4]),int(text[0][-6::][4:6]), 1).isoformat(), 'region':text[0][0:-6]})
    return shpList

# manage eachindividual shapefile aconditioning it to our require structure

def manage_shp(path, stype, date, region):
    '''
    takes a shapefile modifies its structure and gives light into new structure
    path: shapefile path
    stype: ml1 or ml2
    date: YYYY-MM-DD iso format
    region: region
    '''
    newdata = gpd.GeoDataFrame(columns=['geometry', 'value', 'type','date','region'], crs = fiona.crs.from_epsg(4326))
    dataset = gpd.read_file(path)
    for index, rows in dataset.iterrows():
        newdata.loc[index, 'geometry']=rows['geometry'].simplify(0.04,True)
        newdata.loc[index,'value']=rows[0]
        newdata.loc[index,'type']=stype
        newdata.loc[index,'date']=date
        newdata.loc[index,'region']=region
    return newdata

# S3 upload
def s3Upload(outFile):
    conn = tinys3.Connection(os.getenv('S3_ACCESS_KEY'),os.getenv('S3_SECRET_KEY'), tls=True, default_bucket=os.getenv('BUCKET'), endpoint="s3.amazonaws.com")
    # So we could skip the bucket parameter on every request
    response = conn.upload(key=outFile, local_file=open(outFile,'rb'), public=True, close=True)
    if response.status_code==200:
        print('SUCCESS')
    else:
        print('UPLOAD PROCESS FAILURE STATUS CODE:' + str(response.status_code))
        print(response.content)


def main():
    '''
    1.- check for updates
    2.- if there are updates, it will download everything in this folder
    3._ it will download the current file in a separe place
    4._ it will convert the new download to our requered format
    5._ it will merge everything into one file 
    6.- it will zip it 
    7.- it will upload it to s3
    '''
    dfList=[]
    outdir = 'dst'
    outfile = outdir+'/fws.shp'
    path = 'fws'
    path2f = 'fws_original'
    zipfilen = 'fws.zip'
    args = {'q':'select date from fws order by date::date desc limit 1'}
    url = 'https://wri-rw.carto.com/api/v2/sql'
    
    dataDate = requests.get(url, params=args).json()['rows'][0]['date'].split('-')
    lastDate = datetime.date(int(dataDate[0]), int(dataDate[1]), int(dataDate[2]))
    date = str(lastDate.year)+"%02d" % (lastDate.month+4)
    pingUrl='http://shapefiles.fews.net.s3.amazonaws.com/HFIC/WA/west-africa'+ date +'.zip'
    re=requests.get(pingUrl)
    if re.status_code!=200:
        print('There is not new data for this date: ', date)
    else:
        print(re.status_code)
        print(re.headers)
        os.mkdir(path)
        os.mkdir(path2f)
        getFiles(date, path)
        os.mkdir(outdir)
        shpList=fileList(path)
        getFwsFile(path2f)

        print("starting ...")

        for shp in shpList:
            dfList.append(manage_shp(shp['path'],shp['stype'],shp['date'],shp['region']))

        print("merging ...")
        dfList.append(gpd.read_file('fws_original/fws.shp'))
        dst = pd.concat(dfList)
        dst.to_file(outfile)

        print("compressing ...")
        with zipfile.ZipFile(zipfilen, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipDir(outdir, zipf)

        print("uploading ....")

        s3Upload(zipfilen)


# Execution
main()

