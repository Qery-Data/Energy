from pyjstat import pyjstat
import requests
import os
import json
from datetime import datetime
import locale
import io
import pandas as pd
os.makedirs('data', exist_ok=True)
rename_dict = {
    'Bosnia and Herzegovina': 'Bosnia and Herz.',
    'Czechia': 'Czech Rep.',
    'Euro area (EA11-1999, EA12-2001, EA13-2007, EA15-2008, EA16-2009, EA17-2011, EA18-2014, EA19-2015, EA20-2023)': 'Euro area',
    'European Union - 27 countries (from 2020)': 'EU27',
    'Germany (until 1990 former territory of the FRG)': 'Germany',
    'Kosovo (under United Nations Security Council Resolution 1244/99)': 'Kosovo',
    'Türkiye': 'Turkey'
}

#Electricity prices household consumers

#Electricity prices over time household consumers EU27
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_204?lang=en&nrg_cons=KWH2500-4999&tax=X_TAX&tax=X_VAT&tax=I_TAX&currency=EUR&geo=EU27_2020')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Taxes', columns='Time', values='value')
df_new.columns = [col.replace('-S1', ' June') if '-S1' in col else col.replace('-S2', ' December') for col in df_new.columns]
df_new.dropna(how='all', inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Household_Consumers_Time_EU27.csv', index=True)

#Electricity prices households latest Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_204?lang=en&lastTimePeriod=1&nrg_cons=KWH2500-4999&tax=I_TAX&currency=EUR')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new.dropna(how='all', inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Household_Consumers_Latest_Europe.csv', index=True)

#Electricity price household consumers comparison by time Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_204?lang=en&lastTimePeriod=4&nrg_cons=KWH2500-4999&tax=I_TAX&currency=NAC')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new["Pct_change"] = ((df_new.iloc[:, -1] - df_new.iloc[:, 1]) / df_new.iloc[:, 1]) * 100
df_new.dropna(inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Household_Consumers_Change_Europe.csv', index=True)

#Electricity price household by consumer group Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_204?lang=en&lastTimePeriod=1&nrg_cons=KWH_LT1000&nrg_cons=KWH1000-2499&nrg_cons=KWH2500-4999&nrg_cons=KWH5000-14999&nrg_cons=KWH_GE15000&tax=I_TAX&currency=EUR')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Energy consumption', values='value')
df_new.dropna(how='all', inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Household_Consumers_Groups_Europe.csv', index=True)

#Electricity price household consumers composition Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_204?lang=en&lastTimePeriod=1&nrg_cons=KWH2500-4999&tax=X_TAX&tax=X_VAT&tax=I_TAX&currency=EUR')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Taxes', values='value')
df_new.dropna(how='all', inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Household_Consumers_Composition_Europe.csv', index=True)

#Electricity price household consumers PPS Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_204?lang=en&lastTimePeriod=1&nrg_cons=KWH2500-4999&tax=I_TAX&currency=EUR&currency=PPS')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Currency', values='value')
df_new = df_new.rename(columns={"Euro": "EUR", "Purchasing Power Standard": "PPS"})
df_new.dropna(inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Household_Consumers_PPS_Europe.csv', index=True)

#Electricity prices non-household consumers

#Electricity prices over time non-household consumers EU27
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_205?lang=en&nrg_cons=MWH500-1999&tax=X_TAX&tax=X_VAT&tax=I_TAX&currency=EUR&geo=EU27_2020')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Taxes', columns='Time', values='value')
df_new.columns = df_new.columns.str.replace("S", "H")
df_new.dropna(how='all', inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Non_Household_Consumers_Time_EU27.csv', index=True)

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

#Electricity price household consumers composition Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_205?lang=en&lastTimePeriod=1&nrg_cons=MWH500-1999&tax=X_TAX&tax=X_VAT&tax=I_TAX&currency=EUR')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Taxes', values='value')
df_new["Share of taxes and levies (%)"] = ((df_new["Excluding VAT and other recoverable taxes and levies"] - df_new["Excluding taxes and levies"]) /(df_new["Excluding VAT and other recoverable taxes and levies"]) * 100)
df_new.dropna(how='all', inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Non_Household_Consumers_Composition_Europe.csv', index=True)

#Electricity price household consumers PPS Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_205?lang=en&lastTimePeriod=1&nrg_cons=MWH500-1999&tax=X_VAT&currency=EUR&currency=PPS')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Currency', values='value')
df_new = df_new.rename(columns={"Euro": "EUR", "Purchasing Power Standard": "PPS"})
df_new.dropna(inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Non_Household_Consumers_PPS_Europe.csv', index=True)  

#Natural gas prices household consumers 

#Natural gas prices over time household consumers EU27
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_202?lang=en&nrg_cons=GJ20-199&unit=KWH&tax=X_TAX&tax=X_VAT&tax=I_TAX&currency=EUR&geo=EU27_2020')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Taxes', columns='Time', values='value')
df_new.columns = [col.replace('-S1', ' June') if '-S1' in col else col.replace('-S2', ' December') for col in df_new.columns]
df_new.dropna(how='all', inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Natural_Gas_Prices_Household_Consumers_Time_EU27.csv', index=True)

#Natural gas prices households consumers latest 
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_202?lang=en&lastTimePeriod=1&nrg_cons=GJ20-199&unit=KWH&tax=I_TAX&currency=EUR')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new.dropna(how='all', inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Natural_Gas_Prices_Household_Consumers_Latest_Europe.csv', index=True)

#Natural gas price households consumers comparison by time
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_202?lang=en&lastTimePeriod=4&nrg_cons=GJ20-199&unit=KWH&tax=I_TAX&currency=NAC')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new["Pct_change"] = ((df_new.iloc[:, -1] - df_new.iloc[:, 1]) / df_new.iloc[:, 1]) * 100
df_new.dropna(inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Natural_Gas_Prices_Household_Consumers_Change_Europe.csv', index=True)

#Natural gas price household by consumer group 
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_202?lang=en&lastTimePeriod=1&nrg_cons=GJ_LT20&nrg_cons=GJ20-199&nrg_cons=GJ_GE200&unit=KWH&tax=I_TAX&currency=EUR')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Energy consumption', values='value')
df_new.dropna(how='all', inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Natural_Gas_Prices_Household_Consumers_Groups_Europe.csv', index=True)

#Natural gas price consumers composition Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_202?lang=en&lastTimePeriod=1&nrg_cons=GJ20-199&unit=KWH&tax=X_TAX&tax=X_VAT&tax=I_TAX&currency=EUR')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Taxes', values='value')
df_new.dropna(how='all', inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Natural_Gas_Prices_Household_Consumers_Composition_Europe.csv', index=True)

#Natural gas price consumers PPS Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_202?lang=en&lastTimePeriod=1&nrg_cons=GJ20-199&unit=KWH&tax=I_TAX&currency=EUR&currency=PPS')
df = dataset.write('dataframe')
df.replace(rename_dict, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Currency', values='value')
df_new = df_new.rename(columns={"Euro": "EUR", "Purchasing Power Standard": "PPS"})
df_new.dropna(inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Natural_Gas_Prices_Household_Consumers_PPS_Europe.csv', index=True)