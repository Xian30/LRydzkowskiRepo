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
endpoint_airports = 'https://app.goflightlabs.com/airports?'

#api response
api_result_airports = requests.get(endpoint_airports, params)
api_response_airports = api_result_airports.json()


#creating list of variables
variables_list = ["GMT","airportId","codeIataAirport","codeIataCity","codeIcaoAirport","codeIso2Country"\
                 ,"geonameId","latitudeAirport","longitudeAirport","nameAirport","nameCountry","phone","timezone"]

#vartypes
var_types = ["str", "str", "str", "str","str","str","str","float64","float64", "str"\
           , "str","str","str"]


#creation of dict with variables_Types
var_names_types = dict()
for variable, vartype in zip(variables_list,var_types):
    var_names_types[variable] = vartype

#empty lists
GMT = list()
airportId = list()
codeIataAirport = list()
codeIataCity = list()
codeIcaoAirport = list()
codeIso2Country = list()
geonameId = list()
latitudeAirport = list()
longitudeAirport = list()
nameAirport = list()
nameCountry = list()
phone = list()
timezone = list()


#data ingestion
for i in range(len(api_response_airports["data"])):
    GMT.append(api_response_airports["data"][i].get("GMT"))
    airportId.append(api_response_airports["data"][i].get("airportId"))
    codeIataAirport.append(api_response_airports["data"][i].get("codeIataAirport"))
    codeIataCity.append(api_response_airports["data"][i].get("codeIataCity"))
    codeIcaoAirport.append(api_response_airports["data"][i].get("codeIcaoAirport"))
    codeIso2Country.append(api_response_airports["data"][i].get("codeIso2Country"))
    geonameId.append(api_response_airports["data"][i].get("geonameId"))
    latitudeAirport.append(api_response_airports["data"][i].get("latitudeAirport"))
    longitudeAirport.append(api_response_airports["data"][i].get("longitudeAirport"))
    nameAirport.append(api_response_airports["data"][i].get("nameAirport"))
    nameCountry.append(api_response_airports["data"][i].get("nameCountry"))
    phone.append(api_response_airports["data"][i].get("phone"))
    timezone.append(api_response_airports["data"][i].get("timezone"))


#dataframe
data_airports = pd.DataFrame(data = zip(GMT, airportId, codeIataAirport, codeIataCity, codeIcaoAirport,codeIso2Country\
                                  ,geonameId, latitudeAirport, longitudeAirport, nameAirport, nameCountry, phone, timezone),
                                  columns=variables_list) 
#setting data types
data_airports = data_airports.astype(var_names_types)

#saving data to s3
data_airports.to_parquet(f"s3://lrpostgradudeflights/airports/airports.parquet")
