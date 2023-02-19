import pyarrow
import sys
import boto3
import requests
import pandas as pd
import json
from datetime import date

#params
params = {
'access_key': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI0IiwianRpIjoiMWMwYmU1Y2M4YWRmY2FmYjVjYzRjMTQ5MGFhYTM4YmNjNzU2YjFmM2Q1YjkyZDM4ZjAxZjNlNzc3ZDJjN2MzZWQzYWQwZDg3ZTlhNjM4YmYiLCJpYXQiOjE2NzQ5ODQ4MTcsIm5iZiI6MTY3NDk4NDgxNywiZXhwIjoxNzA2NTIwODE3LCJzdWIiOiIxOTg2MyIsInNjb3BlcyI6W119.GnPFpS8Bv-3emBt-NW6XGAi7mAAq7nUCvGSzWUdEEesNKaaNQTVdHHzdyrD32DUC87rlXJIxtAF3jNqx1OfnMA', 
 }

#endpoint
endpoint_cities = 'https://app.goflightlabs.com/cities?'

#api response
api_result_cities = requests.get(endpoint_cities, params)
api_response_cities = api_result_cities.json()


#creating list of variables
variables_list = ["GMT","cityId","codeIataCity","codeIso2Country","geonameId","latitudeCity","longitudeCity"\
                  ,"nameCity","timezone"]

#vartypes
var_types = ["str", "str","str","str","int64","float64","float64","str","str"]

#creation of dict with variables_Types
var_names_types = dict()

for variable, vartype in zip(variables_list,var_types):
    var_names_types[variable] = vartype

#empty lists
GMT = list()
cityId = list()
codeIataCity = list()
codeIso2Country = list()
geonameId = list()
latitudeCity = list()
longitudeCity = list()
nameCity = list()
timezone = list()

#data ingestion
for i in range(len(api_response_cities["data"])):
    GMT.append(api_response_cities["data"][i].get("GMT"))
    cityId.append(api_response_cities["data"][i].get("cityId"))
    codeIataCity.append(api_response_cities["data"][i].get("codeIataCity"))
    codeIso2Country.append(api_response_cities["data"][i].get("codeIso2Country"))
    geonameId.append(api_response_cities["data"][i].get("geonameId"))
    latitudeCity.append(api_response_cities["data"][i].get("latitudeCity"))
    longitudeCity.append(api_response_cities["data"][i].get("longitudeCity"))
    nameCity.append(api_response_cities["data"][i].get("nameCity"))
    timezone.append(api_response_cities["data"][i].get("timezone"))

#dataframe
data_cities = pd.DataFrame(data= zip (GMT,cityId,codeIataCity,codeIso2Country,geonameId,latitudeCity,longitudeCity\
                                ,nameCity,timezone
                                ), columns= variables_list)

#setting data types
data_cities = data_cities.astype(var_names_types)

#saving data to s3
data_cities.to_parquet(f"s3://lrpostgradudeflights/cities/cities.parquet")

