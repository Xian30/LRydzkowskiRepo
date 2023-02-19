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
 "code": "WAW",
 "type": "departure",
 }

#date of the job
current_date= date.today()
 
 #change format of date
if len(str(current_date.month)) ==  1:
    miesiac = f'0{current_date.month-1}'
else:
    miesiac = current_date.month-1
    
if len(str(current_date.day)) ==  1:
    dzien = f'0{current_date.day}'
else:
    dzien = current_date.day

#date of the jobi api format
current_date2 = f'{current_date.year}-{miesiac}-{dzien}'

 #endpoint
endpoint_dep_hist = f'https://app.goflightlabs.com/historical/{current_date2}?'

#api response
api_result_hist = requests.get(endpoint_dep_hist, params)
api_response_hist = api_result_hist.json()


#creating list of variables
variables_list = ["flight_type", "status", "departure_iataCode","departure_icaoCode", "departure_terminal"\
                ,"departure_gate", "departure_delay", "departure_scheduledTime", "departure_estimatedTime"\
                ,"departure_actualTime", "departure_estimatedRunway", "departure_actualRunway" ,"arrival_iataCode"\
                ,"arrival_icaoCode", "arrival_terminal","arrival_baggage","arrival_gate","arrival_delay","arrival_scheduledTime",\
                 "arrival_estimatedTime", "arrival_actualTime","arrival_estimatedRunawy","arrival_actualRunway", "airline_name"\
                 ,"airline_iataCode", "airline_icaoCode", "flight_number", "flight_iataNumber", "flight_icaoNumber"]

#var_types
var_types = ("str","str","str","str","str","str", "float64", "datetime64[ns]", "datetime64[ns]"\
             ,"datetime64[ns]","datetime64[ns]", "datetime64[ns]","str", "str", "str", "str","str"\
             ,"float64","datetime64[ns]", "datetime64[ns]","datetime64[ns]","datetime64[ns]","datetime64[ns]","str"\
             ,"str","str","str", "str", "str")

#creation of dict with variables_Types
var_names_types = dict()

for variable, vartype in zip(variables_list,var_types):
    var_names_types[variable] = vartype 


#empty lists
flight_type = list() #ok
status = list() #ok 
departure_iataCode = list() #ok
departure_icaoCode = list() #ok
departure_terminal = list() #ok
departure_gate = list() #ok
departure_delay = list() #ok
departure_scheduledTime = list() #ok
departure_estimatedTime = list() #ok
departure_actualTime = list() #ok
departure_estimatedRunway = list() #ok
departure_actualRunway = list() #ok
arrival_iataCode = list() #ok 
arrival_icaoCode = list() #ok
arrival_terminal = list() #ok
arrival_baggage = list() #ok
arrival_gate = list() #ok
arrival_delay = list() #ok
arrival_scheduledTime = list() #ok
arrival_estimatedTime = list() #ok
arrival_actualTime = list() #ok
arrival_estimatedRunway = list() #ok
arrival_actualRunway = list() #ok
airline_name = list() #ok
airline_iataCode = list() #ok
airline_icaoCode = list() #ok
flight_number = list() #ok
flight_iataNumber = list() #ok
flight_icaoNumber = list() #ok


#data ingestion
for i in range(len(api_response_hist["data"])):
    flight_type.append(api_response_hist["data"][i]["type"])
    status.append(api_response_hist["data"][i]["status"])
    departure_iataCode.append(api_response_hist["data"][i]["departure"].get("iataCode"))
    departure_icaoCode.append(api_response_hist["data"][i]["departure"].get("icaoCode"))
    departure_terminal.append(api_response_hist["data"][i]["departure"].get("terminal")) 
    departure_gate.append(api_response_hist["data"][i]["departure"].get("gate"))
    departure_delay.append(api_response_hist["data"][i]["departure"].get("delay")) 
    departure_scheduledTime.append(api_response_hist["data"][i]["departure"].get("scheduledTime"))
    departure_estimatedTime.append(api_response_hist["data"][i]["departure"].get('estimatedTime')) 
    departure_actualTime.append(api_response_hist["data"][i]["departure"].get('actualTime'))
    departure_estimatedRunway.append(api_response_hist["data"][i]["departure"].get('estimatedRunway')) 
    departure_actualRunway.append(api_response_hist["data"][i]["departure"].get('actualRunway')) 
    arrival_iataCode.append(api_response_hist["data"][i]["arrival"].get("iataCode")) 
    arrival_icaoCode.append(api_response_hist["data"][i]["arrival"].get("icaoCode")) 
    arrival_terminal.append(api_response_hist["data"][i]["arrival"].get("terminal"))
    arrival_baggage.append(api_response_hist["data"][i]["arrival"].get("baggage"))
    arrival_gate.append(api_response_hist["data"][i]["arrival"].get("gate"))
    arrival_delay.append(api_response_hist["data"][i]["arrival"].get("delay"))                   
    arrival_scheduledTime.append(api_response_hist["data"][i]["arrival"].get("scheduledTime")) 
    arrival_estimatedTime.append(api_response_hist["data"][i]["arrival"].get("estimatedTime")) 
    arrival_actualTime.append(api_response_hist["data"][i]["arrival"].get("actualTime"))
    arrival_estimatedRunway.append(api_response_hist["data"][i]["arrival"].get("estimatedRunway"))
    arrival_actualRunway.append(api_response_hist["data"][i]["arrival"].get("actualRunway"))
    airline_name.append(api_response_hist["data"][i]["airline"].get("name")) 
    airline_iataCode.append(api_response_hist["data"][i]["airline"].get("iataCode")) 
    airline_icaoCode.append(api_response_hist["data"][i]["airline"].get("icaoCode")) 
    flight_number.append(api_response_hist["data"][i]["flight"].get("number")) 
    flight_iataNumber.append(api_response_hist["data"][i]["flight"].get("iataNumber")) 
    flight_icaoNumber.append(api_response_hist["data"][i]["flight"].get("icaoNumber")) 

#creating data frame
dane_departure = pd.DataFrame(data = zip(flight_type, status, departure_iataCode, departure_icaoCode\
                              ,departure_terminal, departure_gate, departure_delay, departure_scheduledTime\
                              ,departure_estimatedTime, departure_actualTime, departure_estimatedRunway\
                              ,departure_actualRunway, arrival_iataCode, arrival_icaoCode, arrival_terminal\
                              ,arrival_baggage, arrival_gate, arrival_delay, arrival_scheduledTime\
                              ,arrival_estimatedTime, arrival_actualTime,arrival_estimatedRunway, arrival_actualRunway\
                              ,airline_name, airline_iataCode, airline_icaoCode, flight_number, flight_iataNumber\
                              ,flight_icaoNumber), columns = variables_list)

#setting data types
dane_departure = dane_departure.astype(var_names_types, copy = False)

#modyfing columns' values 
dane_departure["departure_iataCode"] = dane_departure["departure_iataCode"].map(lambda x: x.upper())
dane_departure["arrival_iataCode"] = dane_departure["arrival_iataCode"].map(lambda x: x.upper())


#konwersja do jsona i okreslenie obiektu
#dane_departure_json = dane_departure.reset_index().to_json(orient='records')
dane_departure.to_parquet(f"s3://lrpostgradudeflights/departures/departure_{current_date2}.parquet")


#api_response_rt = json.dumps(api_response_rt)

#s3bucket = "lrpostgradudeflights/departure/"

#s3 = boto3.resource('s3')
#s3object = s3.Object(f'lrpostgradudeflights', f'departures/departure_{current_date2}')
#s3object.put(Body=dane_departure_json)


