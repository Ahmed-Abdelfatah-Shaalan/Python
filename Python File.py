from azure.storage.blob import BlobServiceClient, BlobClient
import numpy as np
import pandas as pd
import datetime 
import requests
import pyodbc
import json

server = 'XXXXXX'
username = 'XXXXXXX'
password = 'XXXXXX'
db = 'XXXX'
connection = None
maxdate = ''

try:
    connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';Database='+db+';UID='+username+';PWD='+password)
    print('Connected to SQL Server Database');
except Exception as e:
    print(e);
if connection is not None:
    connection.autocommit = True;
cur = connection.cursor()
cur.execute("SELECT MAX(CAST(column AS DATE)) FROM [schema].[table];")
for data in cur.fetchall() :
       maxdate = data[0]
       
dt = datetime.datetime.today()
container_name="your container name"
constr = "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=XXXXXXX;AccountKey=XXXXXXXXXXXXXXXXXXXXXXX"
account_url = "https://ikeadatelakeg2.blob.core.windows.net/"
blob_name = "OUTPUT FILE_"+str(dt.year)+"-"+str('{:02d}'.format(dt.month))+"-"+str('{:02d}'.format(dt.day))+".csv"
blob_url = f"{account_url}/{container_name}/{blob_name}"
blobclient = BlobClient.from_blob_url(blob_url=blob_url,credential = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
csvObject = []
print("Calling API")
for i in range (1,50):
    print(i)
    r = requests.get("https://api.alchemer.eu/v4/survey/90407326/surveyresponse?filter[field][0]=datesubmitted&filter[operator][0]=>=&filter[value][0]="+str(maxdate)+"+00:00:00&filter[field][1]&filter[operator][1]==&filter[value][1]=Complete&page="+str(i)+"&resultsperpage=500&api_token=XXXXXXXXXXXXXXX&api_token_secret=XXXXXXXXXXXX");
    csvObject.append(r.json())
 
df = pd.json_normalize(csvObject, record_path =['data'])
   
df.columns=  df.columns.str.replace("[\"]", "")
df.columns=  df.columns.str.replace("[]]", "")
df.columns=  df.columns.str.replace("[[]", "")

data_file = df.to_csv(index = False ,sep =';')   
try:
   blobclient.upload_blob(data =data_file,overwrite=True)
   print('Alchemer csv file has been generated at '+str(datetime.datetime.now()))
except Exception as e:
    print(e)