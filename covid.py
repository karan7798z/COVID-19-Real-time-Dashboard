import pandas as pd
import numpy as np

#Storing the Source URL's common directory path in a variable
url = "https://github.com/imdevskp/covid_19_jhu_data_web_scrap_and_cleaning/raw/master/"

#Storing the csv filename's as a list of strings
df_names = ['covid_19_clean_complete.csv', 'usa_county_wise.csv']

#Fetch State Dataset
df_usa_states = pd.read_csv(url+df_names[1])

#Transform as per requirement
columns_to_remove = ['UID', 'iso2', 'iso3', 'code3', 'FIPS', 'Admin2', 'Combined_Key', 'Lat', 'Long_']

df_usa_states.drop(columns_to_remove, axis=1, inplace=True)

df2 = df_usa_states.groupby(['Country_Region', 'Province_State', 'Date'])['Confirmed', 'Deaths'].sum().reset_index()

df2.rename(columns={'Country_Region': 'Country', 'Province_State': 'Province', 'Confirmed': 'Confirmed State', 'Deaths': 'Deaths State'}, inplace=True)

#Fetch Countries Dataset
df_countries = pd.read_csv(url+df_names[0])
#Store a copy of this dataset before it gets modified, since we need this entire dataset as a whole later on
df_countries_full = df_countries.copy()
df_countries.dropna(axis=0, inplace=True)
df_countries = df_countries[['Country/Region', 'Province/State', 'Date', 'Confirmed', 'Deaths']]
df_countries.rename(columns={'Country/Region': 'Country', 'Province/State': 'Province', 'Confirmed': 'Confirmed State', 'Deaths': 'Deaths State'}, inplace=True)

#Combine Countries and State Dataframes, to generalize and format the State Dataframe for correct use
df_combined = pd.concat([df2, df_countries])

#Install pygsheets by running below code in Python Terminal

import pygsheets
from datetime import datetime

client = pygsheets.authorize(service_file='/Users/karan7798z/Desktop/Data_Science/credentials.json')

#We will keep the logs written in a log file
log_file = open('/Users/karan7798z/Desktop/Data_Science/corona-virus-report/Logs/Log.txt', 'a+')

date, time = str(datetime.now()).split(' ')
log_file.write('------------------------------'+date+' '+time+'----------------------------------\n\n')
print("--Authorized--")
log_file.write('---Authorized---\n\n')

sheet1 = client.open("covid_19_clean_complete")
print("--Sheet 1 Opened--")
log_file.write('---Worksheet 1 Opened---\n')

worksheet1 = sheet1[0]
print("--First Sheet accessed--")
log_file.write('---Sheet Accessed---\n')

worksheet1.set_dataframe(df_countries_full, (1,1))
print("--First Sheet Data Written--")
log_file.write('---Data Written to sheet---\n\n')

sheet2 = client.open("usa_state_wise")
print("--Sheet 2 Opened--")
log_file.write('---Worksheet 2 Opened---\n')

worksheet2 = sheet2[0]
print("--Second Sheet accessed--")
log_file.write('---Sheet Accessed---\n')

worksheet2.set_dataframe(df_combined, (1,1))
print("--Second Sheet Data Written--")
log_file.write('---Data Written to sheet---\n\n')
log_file.write('==========================================================================================\n\n\n')
log_file.close()