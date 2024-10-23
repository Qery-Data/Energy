from pyjstat import pyjstat
import requests
import os
import json
from datetime import datetime
import locale
import io
import pandas as pd
import numpy as np

os.makedirs('data', exist_ok=True)
rename_dict = {
    'Bosnia and Herzegovina': 'Bosnia and Herz.',
    'Czechia': 'Czech Rep.',
    'Euro area (EA11-1999, EA12-2001, EA13-2007, EA15-2008, EA16-2009, EA17-2011, EA18-2014, EA19-2015, EA20-2023)': 'Euro area',
    'European Union - 27 countries (from 2020)': 'EU27',
    'Kosovo*':'Kosovo',
    'Germany (until 1990 former territory of the FRG)': 'Germany',
    'Kosovo (under United Nations Security Council Resolution 1244/99)': 'Kosovo',
    'Türkiye': 'Turkey'
}

#Electricity prices non-household consumers

#Electricity prices over time non-household consumers EU27
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_205?lang=en&nrg_cons=MWH500-1999&tax=X_TAX&tax=X_VAT&tax=I_TAX&currency=EUR&geo=EU27_2020')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Taxes', columns='Time', values='value')
df_new.columns = [col.replace('-S1', ' June') if '-S1' in col else col.replace('-S2', ' December') for col in df_new.columns]
df_new.dropna(how='all', inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Non_Household_Consumers_Time_EU27.csv', index=True)

#Electricity prices over time non-household consumers all countries
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_205?lang=en&nrg_cons=MWH500-1999&tax=X_VAT&currency=EUR')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new.columns = [col.replace('-S1', ' June') if '-S1' in col else col.replace('-S2', ' December') for col in df_new.columns]
df_new.dropna(how='all', inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Non_Household_Consumers_Time_All_Countries.csv', index=True)

#Electricity prices non-households latest Europe (ex. VAT and other recoverable taxes and levies)
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_205?lang=en&lastTimePeriod=1&nrg_cons=MWH500-1999&tax=X_VAT&currency=EUR')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new.dropna(how='all', inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Non_Household_Consumers_Latest_Europe.csv', index=True)

#Electricity price non-household consumers comparison by time Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_205?lang=en&lastTimePeriod=4&nrg_cons=MWH500-1999&tax=X_VAT&currency=NAC')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new["Pct_change"] = ((df_new.iloc[:, -1] - df_new.iloc[:, 1]) / df_new.iloc[:, 1]) * 100
df_new.dropna(inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Non_Household_Consumers_Change_Europe.csv', index=True)

#Electricity price non-household by consumer group Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_205?lang=en&lastTimePeriod=1&tax=X_VAT&currency=EUR')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Energy consumption', values='value')
df_new.dropna(how='all', inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Non_Household_Consumers_Groups_Europe.csv', index=True)

#Electricity price non-household consumers composition Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_205?lang=en&lastTimePeriod=1&nrg_cons=MWH500-1999&tax=X_TAX&tax=X_VAT&tax=I_TAX&currency=EUR')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Taxes', values='value')
df_new["Share of taxes and levies (%)"] = ((df_new["Excluding VAT and other recoverable taxes and levies"] - df_new["Excluding taxes and levies"]) /(df_new["Excluding VAT and other recoverable taxes and levies"]) * 100)
df_new.dropna(how='all', inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Non_Household_Consumers_Composition_Europe.csv', index=True)

#Electricity price non-household consumers PPS Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_205?lang=en&lastTimePeriod=1&nrg_cons=MWH500-1999&tax=X_VAT&currency=EUR&currency=PPS')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Currency', values='value')
df_new = df_new.rename(columns={"Euro": "EUR", "Purchasing Power Standard": "PPS"})
df_new.dropna(inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Non_Household_Consumers_PPS_Europe.csv', index=True)  