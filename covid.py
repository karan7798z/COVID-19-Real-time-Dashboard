import pandas as pd
import numpy as np
import pygsheets
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

def extract_data_from_url(url):
    #Storing the csv filename's as a list of strings
    df_names = ['time_series_covid19_confirmed_global.csv', 'time_series_covid19_deaths_global.csv', 'time_series_covid19_recovered_global.csv', 'time_series_covid19_confirmed_US.csv', 'time_series_covid19_deaths_US.csv']

    #Fetch Confirmed Dataset
    df_confirmed = pd.read_csv(url+df_names[0])
    #Fetch Deaths Dataset
    df_deaths = pd.read_csv(url + df_names[1])
    #Fetch Recovered Dataset
    df_recovered = pd.read_csv(url + df_names[2])
    #Fetch USA Confrimed Dataset
    df_confirmed_usa = pd.read_csv(url + df_names[3])
    #Fetch USA Deaths Dataset
    df_deaths_usa = pd.read_csv(url + df_names[4])

    return df_confirmed, df_deaths, df_recovered, df_confirmed_usa, df_deaths_usa


def transform_dataframes_global(df_confirmed, df_deaths, df_recovered):
    #Extracting dates
    # ==================================================================================================================
    dates = df_confirmed.columns[4:]
    df_confirmed_long = df_confirmed.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'],
                                value_vars=dates, var_name='Date', value_name='Confirmed')

    df_deaths_long = df_deaths.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'],
                                    value_vars=dates, var_name='Date', value_name='Deaths')

    df_recovered_long = df_recovered.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'],
                                value_vars=dates, var_name='Date', value_name='Recovered')

    df_recovered_long = df_recovered_long[df_recovered_long['Country/Region'] != 'Canada']

    #Merge dataframes
    # ==================================================================================================================
    full_table = pd.merge(left=df_confirmed_long, right=df_deaths_long, how='left',
                          on=['Province/State', 'Country/Region', 'Date', 'Lat', 'Long'])
    full_table = pd.merge(left=full_table, right=df_recovered_long, how='left',
                          on=['Province/State', 'Country/Region', 'Date', 'Lat', 'Long'])


    #Data Preprocessing
    # ==================================================================================================================
    ## 1. Date formatting
    full_table['Date'] = pd.to_datetime(full_table['Date'])

    # 4. fixing Country names
    # 4.1 renaming countries, regions, provinces
    full_table['Country/Region'] = full_table['Country/Region'].replace('Korea, South', 'South Korea')

    # 4.2 Greenland
    full_table.loc[full_table['Province/State'] == 'Greenland', 'Country/Region'] = 'Greenland'

    # 4.3 Mainland china to China
    full_table['Country/Region'] = full_table['Country/Region'].replace('Mainland China', 'China')

    # Removing
    # ==================================================================================================================

    # Removing canada's recovered values
    full_table = full_table[full_table['Province/State'].str.contains('Recovered') != True]

    # Removing county wise data to avoid double counting
    full_table = full_table[full_table['Province/State'].str.contains(',') != True]

    # Active Case = confirmed - deaths - recovered
    full_table['Active'] = full_table['Confirmed'] - full_table['Deaths'] - full_table['Recovered']

    # Filling missing values
    # ==================================================================================================================

    # fill missing province/state value with ''
    full_table[['Province/State']] = full_table[['Province/State']].fillna('')

    # fill missing numerical values with 0
    cols = ['Confirmed', 'Deaths', 'Recovered', 'Active']
    full_table[cols] = full_table[cols].fillna(0)

    # fixing datatypes
    full_table['Recovered'] = full_table['Recovered'].astype(int)

    # fixing off data

    # new values
    feb_12_conf = {'Hubei': 34874}

    # function to change value
    def change_val(date, ref_col, val_col, dtnry):
        for key, val in dtnry.items():
            full_table.loc[(full_table['Date'] == date) & (full_table[ref_col] == key), val_col] = val

    # changing values
    change_val('2/12/20', 'Province/State', 'Confirmed', feb_12_conf)

    # Removing Ship Data
    # ==================================================================================================================

    # ship rows containing ships with COVID-19 reported cases
    ship_rows = full_table['Province/State'].str.contains('Grand Princess') | \
                full_table['Province/State'].str.contains('Diamond Princess') | \
                full_table['Country/Region'].str.contains('Diamond Princess') | \
                full_table['Country/Region'].str.contains('MS Zaandam')

    # ship
    ship = full_table[ship_rows]

    # Latest cases from the ships
    ship_latest = ship[ship['Date'] == max(ship['Date'])]

    # skipping rows with ships info
    full_table = full_table[~(ship_rows)]

    return full_table


def transform_dataframes_usa(df_global, df_confirmed_usa, df_deaths_usa):

    #Preparing USA Statewise Dataframe
    # ==================================================================================================================
    # ids
    ids = df_confirmed_usa.columns[0:11]
    # dates
    us_dates = df_deaths_usa.columns[11:]

    #df_deaths_usa.drop(['Population'], axis=1, inplace=True)
    #df_confirmed_usa.drop(['Population'], axis=1, inplace=True)

    # melt to longer format
    df_confirmed_usa_long = df_confirmed_usa.melt(id_vars=ids, value_vars=us_dates, var_name='Date', value_name='Confirmed')
    df_deaths_usa_long = df_deaths_usa.melt(id_vars=ids, value_vars=us_dates, var_name='Date', value_name='Deaths')
    df_usa_complete = pd.concat([df_confirmed_usa_long, df_deaths_usa_long[['Deaths']]], axis=1)


    # Merging the USA County and Combined Dataframe to prepare a separate dataframe for State-wise list of countries
    # ==================================================================================================================
    columns_to_remove = ['UID', 'iso2', 'iso3', 'code3', 'FIPS', 'Admin2', 'Combined_Key', 'Lat', 'Long_']
    df_usa_complete.drop(columns_to_remove, axis=1, inplace=True)

    df2 = df_usa_complete.groupby(['Country_Region', 'Province_State', 'Date'])['Confirmed', 'Deaths'].sum().reset_index()
    df2.rename(columns={'Country_Region': 'Country', 'Province_State': 'Province', 'Confirmed': 'Confirmed State',
                        'Deaths': 'Deaths State'}, inplace=True)
    df_countries_mod = df_global[df_global['Province/State']!='']
    df_countries_mod = df_countries_mod[['Country/Region', 'Province/State', 'Date', 'Confirmed', 'Deaths']]
    df_countries_mod.rename(
        columns={'Country/Region': 'Country', 'Province/State': 'Province', 'Confirmed': 'Confirmed State',
                 'Deaths': 'Deaths State'}, inplace=True)

    # Combine Countries and State Dataframes, to generalize and format the State Dataframe for correct use
    df_combined = pd.concat([df2, df_countries_mod])

    return df_combined


def load_dataframe_in_gsheets(df_countries, df_combined):
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

    worksheet1.set_dataframe(df_countries, (1,1))
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

def etl():
    # Storing the Source URL's common directory path in a variable
    url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/"

    #Calling the extract function to extract dataframes
    df_confirmed, df_death, df_recovered, df_confirmed_usa, df_deaths_usa = extract_data_from_url(url)

    #Calling the transform function to get the aggregated states dataset
    df_global = transform_dataframes_global(df_confirmed, df_death, df_recovered)

    df_deaths_usa.drop(['Population'], axis=1, inplace=True)

    df_combined = transform_dataframes_usa(df_global, df_confirmed_usa, df_deaths_usa)

    #Calling the load function to load the data in Google Sheets
    load_dataframe_in_gsheets(df_global, df_combined)


dag = DAG(dag_id="covid_pipeline",
          start_date=datetime(2020, 6, 12),
          schedule_interval="30 6 * * *",
          catchup=False)

covid_task = PythonOperator(task_id="covid_task",
                      python_callable=etl,
                      dag=dag,
                      retries=3)