from azure.storage.blob import BlobServiceClient, BlobClient
import numpy as np
import pandas as pd
import datetime 
import requests
import pyodbc
import json

server = ''
username = ''
password = ''
db = ''
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
cur.execute("SQL Code;")
for data in cur.fetchall() :
       maxdate = data[0]
       
dt = datetime.datetime.today()
container_name="alchemer"
constr = "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=ikeadatelakeg2;AccountKey=xxx"
account_url = "https://ikeadatelakeg2.blob.core.windows.net/"
blob_name = "POST_PURCHASE_STORE_KSA_"+str(dt.year)+"-"+str('{:02d}'.format(dt.month))+"-"+str('{:02d}'.format(dt.day))+".csv"
blob_url = f"{account_url}/{container_name}/{blob_name}"
blobclient = BlobClient.from_blob_url(blob_url=blob_url,credential = "kUEZMccioDckX9SfxK6vUZMcYxayRNsx/X0fsm9JiCgGTmh/wN8JpjTEAqtjSppwkettek++q3yftShTfcturg==")
csvObject = []
print("Calling API")
for i in range (1,51):
    print(i)
    r = requests.get("https://api.alchemer.eu/v4/survey/90384748/surveyresponse?filter[field][0]=datesubmitted&filter[operator][0]=>=&filter[value][0]="+str(maxdate)+"+00:00:00&filter[field][1]&filter[operator][1]==&filter[value][1]=Complete&page="+str(i)+"&resultsperpage=500&api_token=xxxx");
    csvObject.append(r.json())
 
df = pd.json_normalize(csvObject, record_path =['data'])
   
df.columns=  df.columns.str.replace("[\"]", "")
df.columns=  df.columns.str.replace("[]]", "")
df.columns=  df.columns.str.replace("[[]", "")
header = ['id','responseID','datestarted','datesubmitted','Language','SessionID','status','url(department)','url(interaction_id)','url(recruitment_method)', 'url(recruitment)','url(sLanguage)','url(sglocale)','url(store_id)','url(transaction_id)','url(store_number)','url(transid)','url(sguid)','question(77)','question(435), option(13071)','question(435), option(13073)','question(435), option(13077)','question(435), option(13081)','question(435), option(13082)','question(435), option(13089)','question(435), option(13090)','question(435), option(13091)', 'question(435), option(13095)', 'question(435), option(13096)', 'question(435), option(13100)','question(435), option(13098)','question(435), option(13101)','question(435), option(13103)','question(435), option(13104)','question(435), option(13105)','question(435), option(13106)','question(435), option(13109)','question(435), option(13110)','question(435), option(13111)','question(435), option(13112)','question(435), option(13113)','question(435), option(13114)','question(435), option(13116)','question(435), option(13115)', 'question(435), option(13117)', 'question(435), option(13119)','question(435), option(13118)','question(435), option(13120)', 'question(435), option(13121)', 'question(435), option(13122)', 'question(435), option(13124)','question(435), option(13125)','question(435), option(13488)','question(435), option(13489)','question(464), option(13435)','question(435), option(13101-other)','question(459), option(13295)','question(459), option(13296)','question(459), option(13298)','question(459), option(13299)','question(459), option(13300)','question(459), option(13301)', 'question(459), option(13303)', 'question(459), option(13302)', 'question(459), option(13305)', 'question(459), option(13310)', 'question(459), option(13314)','question(459), option(13490)','question(460), option(13320)','question(460), option(13328)','question(460), option(13336)','question(460), option(13337)','question(460), option(13376)','question(460), option(13492)','question(460), option(13491)','question(460), option(13494)','question(460), option(13495)','question(460), option(13493)','question(464), option(13434)', 'question(464), option(13436)', 'question(464), option(13476)','question(467), option(13477)','question(467), option(13478)', 'question(467), option(13479)','question(465), option(13452)', 'question(465), option(13472)','question(465), option(13475)','question(231)','question(433)','question(472)','variable(432-shown)','calc(433)']
data_file = df.to_csv(index = False ,columns =header,sep =';')   
try:
   blobclient.upload_blob(data =data_file,overwrite=True)
   print('Alchemer csv file has been generated at '+str(datetime.datetime.now()))
except Exception as e:
    print(e)


