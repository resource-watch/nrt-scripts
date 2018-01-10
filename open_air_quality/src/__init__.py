import requests
import json
import pandas as pd
from shapely.geometry import Point
import zipfile
import os
from geopandas import GeoDataFrame
from pandas.io.json import json_normalize
import tinys3



url='https://api.openaq.org/v1/latest'
payload = {
    'limit':10000,
    'has_geo':True
}
r = requests.get(url, params=payload)
r.status_code

data = r.json()['results']
df = json_normalize(data, ['measurements'],[['coordinates', 'latitude'], ['coordinates', 'longitude'],'location', 'city', 'country'])

print(df.columns.values)
df.head(10)

geometry = [Point(xy) for xy in zip(df['coordinates.longitude'], df['coordinates.latitude'])]
df = df.drop(['coordinates.longitude', 'coordinates.latitude'], axis=1)
crs = {'init': 'epsg:4326'}
geo_df = GeoDataFrame(df, crs=crs, geometry=geometry)

def export2shp(data, outdir, outname):
    current = os.getcwd()
    path= current+outdir
    os.mkdir(path)
    data.to_file(filename=(outname+'.shp'),driver='ESRI Shapefile')
    #with zipfile.ZipFile(outname+'.zip', 'w', zipfile.ZIP_DEFLATED) as zipf:
    #    zipDir(path, zipf)
    os.rmdir(path)

def ZipShp (inShp, outname, Delete = True):

    #List of shapefile file extensions
    extensions = [".shp",".shx",".dbf",".sbn",".sbx",".fbn",".fbx",".ain",".aih",".atx",".ixs",".mxs",".prj",
                  ".xml",".cpg"]

    #Directory of shapefile
    inLocation = './'
    #Base name of shapefile
    inName = outname
    #Create zipfile name
    zipfl = os.path.join (inLocation, inName + ".zip")
    #Create zipfile object
    ZIP = zipfile.ZipFile (zipfl, "w")

    #Iterate files in shapefile directory
    for fl in os.listdir (inLocation):
        #Iterate extensions
        for extension in extensions:
            #Check if file is shapefile file
            if fl == inName + extension:
                #Get full path of file
                inFile = os.path.join (inLocation, fl)
                #Add file to zipfile
                ZIP.write (inFile, fl)
                os.remove(fl)
                break


    #Close zipfile object
    ZIP.close()

    #Return zipfile full path
    return zipfl

def s3Upload(outFile):
    # Push to Amazon S3 instance
    conn = tinys3.Connection(os.getenv('S3_ACCESS_KEY'),os.getenv('S3_SECRET_KEY'),tls=True)
    f = open(outFile,'rb')
    conn.upload(outFile,f,os.getenv('BUCKET'))

outdir='/dst'
outname='PM2-5_PM10_NO2_SO2_O3_CO_BC_OpenAQ'
export2shp(geo_df, outdir, outname)
ZipShp(os.getcwd(), outname)
s3Upload(outname+'.zip')
print 'ready'
