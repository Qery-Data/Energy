from pyjstat import pyjstat
import requests
import os
import json
from datetime import datetime
import locale
import io
import pandas as pd
os.makedirs('data', exist_ok=True)

#Electricity prices over time consumers EU27
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_204?lang=en&lastTimePeriod=29&nrg_cons=KWH2500-4999&tax=I_TAX&currency=EUR&geo=EU27_2020')
type(dataset)
df = dataset.write('dataframe')
df=df.replace({'European Union - 27 countries (from 2020)':'EU27'})
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new.columns = df_new.columns.str.replace("S", "H")
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Consumers_Time_EU27.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Natural gas prices over time consumers EU27
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_202?lang=en&lastTimePeriod=29&nrg_cons=GJ20-199&unit=KWH&tax=I_TAX&currency=EUR&geo=EU27_2020')
type(dataset)
df = dataset.write('dataframe')
df=df.replace({'European Union - 27 countries (from 2020)':'EU27'})
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new.columns = df_new.columns.str.replace("S", "H")
df_new.to_csv('data_Eurostat_Energy_Prices/Natural_Gas_Prices_Consumers_Time_EU27.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Electricity prices households latest Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_204?time=2022-S2&nrg_cons=KWH2500-4999&tax=I_TAX&currency=EUR&geo=AL&geo=AT&geo=BA&geo=BE&geo=BG&geo=CY&geo=CZ&geo=DE&geo=DK&geo=EA&geo=EE&geo=EL&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=GE&geo=HR&geo=HU&geo=IE&geo=IS&geo=IT&geo=LI&geo=LT&geo=LU&geo=LV&geo=MD&geo=ME&geo=MK&geo=MT&geo=NL&geo=NO&geo=PL&geo=PT&geo=RO&geo=RS&geo=SE&geo=SI&geo=SK&geo=TR&geo=UA&geo=UK&geo=XK')
type(dataset)
df = dataset.write('dataframe')
df=df.replace({'Bosnia and Herzegovina':'Bosnia and Herz.','Czechia':'Czech Rep.','Euro area (EA11-1999, EA12-2001, EA13-2007, EA15-2008, EA16-2009, EA17-2011, EA18-2014, EA19-2015, EA20-2023)':'Euro area', 'European Union - 27 countries (from 2020)':'EU27', 'Germany (until 1990 former territory of the FRG)':'Germany', 'Kosovo (under United Nations Security Council Resolution 1244/99)':'Kosovo','Türkiye':'Turkey'})
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Consumers_Latest_Europe.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Natural gas prices households latest Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_202?time=2022-S2&nrg_cons=GJ20-199&unit=KWH&tax=I_TAX&currency=EUR&geo=AL&geo=AT&geo=BA&geo=BE&geo=BG&geo=CY&geo=CZ&geo=DE&geo=DK&geo=EA&geo=EE&geo=EL&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=GE&geo=HR&geo=HU&geo=IE&geo=IS&geo=IT&geo=LI&geo=LT&geo=LU&geo=LV&geo=MD&geo=ME&geo=MK&geo=MT&geo=NL&geo=NO&geo=PL&geo=PT&geo=RO&geo=RS&geo=SE&geo=SI&geo=SK&geo=TR&geo=UA&geo=UK&geo=XK')
type(dataset)
df = dataset.write('dataframe')
df=df.replace({'Bosnia and Herzegovina':'Bosnia and Herz.','Czechia':'Czech Rep.','Euro area (EA11-1999, EA12-2001, EA13-2007, EA15-2008, EA16-2009, EA17-2011, EA18-2014, EA19-2015, EA20-2023)':'Euro area', 'European Union - 27 countries (from 2020)':'EU27', 'Germany (until 1990 former territory of the FRG)':'Germany', 'Kosovo (under United Nations Security Council Resolution 1244/99)':'Kosovo','Türkiye':'Turkey'})
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new.to_csv('data_Eurostat_Energy_Prices/Natural_Gas_Prices_Consumers_Latest_Europe.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Electricity price comparison by time (S2) Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_204?nrg_cons=KWH2500-4999&tax=I_TAX&currency=NAC&geo=AL&geo=AT&geo=BA&geo=BE&geo=BG&geo=CY&geo=CZ&geo=DE&geo=DK&geo=EA&geo=EE&geo=EL&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=GE&geo=HR&geo=HU&geo=IE&geo=IS&geo=IT&geo=LI&geo=LT&geo=LU&geo=LV&geo=MD&geo=ME&geo=MK&geo=MT&geo=NL&geo=NO&geo=PL&geo=PT&geo=RO&geo=RS&geo=SE&geo=SI&geo=SK&geo=TR&geo=UA&geo=UK&geo=XK&time=2022-S2&time=2021-S2')
type(dataset)
df = dataset.write('dataframe')
df=df.replace({'Bosnia and Herzegovina':'Bosnia and Herz.','Czechia':'Czech Rep.','Euro area (EA11-1999, EA12-2001, EA13-2007, EA15-2008, EA16-2009, EA17-2011, EA18-2014, EA19-2015, EA20-2023)':'Euro area', 'European Union - 27 countries (from 2020)':'EU27', 'Germany (until 1990 former territory of the FRG)':'Germany', 'Kosovo (under United Nations Security Council Resolution 1244/99)':'Kosovo','Türkiye':'Turkey'})
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new.drop(index={'Euro area','Ukraine','United Kingdom', 'Turkey'}, inplace=True)
df_new['Pct change'] = df_new[['2021-S2','2022-S2']].pct_change(axis=1)['2022-S2']*100
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Consumers_Change_Europe.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Natural gas price comparison by time (S2) Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_202?nrg_cons=GJ20-199&tax=I_TAX&unit=KWH&currency=NAC&geo=AL&geo=AT&geo=BA&geo=BE&geo=BG&geo=CY&geo=CZ&geo=DE&geo=DK&geo=EA&geo=EE&geo=EL&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=GE&geo=HR&geo=HU&geo=IE&geo=IS&geo=IT&geo=LI&geo=LT&geo=LU&geo=LV&geo=MD&geo=ME&geo=MK&geo=MT&geo=NL&geo=NO&geo=PL&geo=PT&geo=RO&geo=RS&geo=SE&geo=SI&geo=SK&geo=TR&geo=UA&geo=UK&geo=XK&time=2022-S2&time=2021-S2')
type(dataset)
df = dataset.write('dataframe')
df=df.replace({'Bosnia and Herzegovina':'Bosnia and Herz.','Czechia':'Czech Rep.','Euro area (EA11-1999, EA12-2001, EA13-2007, EA15-2008, EA16-2009, EA17-2011, EA18-2014, EA19-2015, EA20-2023)':'Euro area', 'European Union - 27 countries (from 2020)':'EU27', 'Germany (until 1990 former territory of the FRG)':'Germany', 'Kosovo (under United Nations Security Council Resolution 1244/99)':'Kosovo','Türkiye':'Turkey'})
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new.drop(index={'Euro area','Ukraine','United Kingdom', 'Turkey'}, inplace=True)
df_new['Pct change'] = df_new[['2021-S2','2022-S2']].pct_change(axis=1)['2022-S2']*100
df_new.to_csv('data_Eurostat_Energy_Prices/Natural_Gas_Prices_Consumers_Change_Europe.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Electricity price by consumer group Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_204?time=2022-S2&nrg_cons=KWH_GE15000&nrg_cons=KWH1000-2499&nrg_cons=KWH2500-4999&nrg_cons=KWH5000-14999&nrg_cons=KWH_LT1000&tax=I_TAX&currency=EUR&geo=AL&geo=AT&geo=BA&geo=BE&geo=BG&geo=CY&geo=CZ&geo=DE&geo=DK&geo=EA&geo=EE&geo=EL&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=GE&geo=HR&geo=HU&geo=IE&geo=IS&geo=IT&geo=LI&geo=LT&geo=LU&geo=LV&geo=MD&geo=ME&geo=MK&geo=MT&geo=NL&geo=NO&geo=PL&geo=PT&geo=RO&geo=RS&geo=SE&geo=SI&geo=SK&geo=TR&geo=UA&geo=UK&geo=XK')
type(dataset)
df = dataset.write('dataframe')
df=df.replace({'Bosnia and Herzegovina':'Bosnia and Herz.','Czechia':'Czech Rep.','Euro area (EA11-1999, EA12-2001, EA13-2007, EA15-2008, EA16-2009, EA17-2011, EA18-2014, EA19-2015, EA20-2023)':'Euro area', 'European Union - 27 countries (from 2020)':'EU27', 'Germany (until 1990 former territory of the FRG)':'Germany', 'Kosovo (under United Nations Security Council Resolution 1244/99)':'Kosovo','Türkiye':'Turkey'})
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Energy consumption', values='value')
df_new.drop(index={'Ukraine','United Kingdom'}, inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Consumers_Groups_Europe.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Natural gas price by consumer group Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_202?time=2022-S2&nrg_cons=GJ_GE200&nrg_cons=GJ20-199&nrg_cons=GJ_LT20&tax=I_TAX&unit=KWH&currency=EUR&geo=AL&geo=AT&geo=BA&geo=BE&geo=BG&geo=CY&geo=CZ&geo=DE&geo=DK&geo=EA&geo=EE&geo=EL&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=GE&geo=HR&geo=HU&geo=IE&geo=IS&geo=IT&geo=LI&geo=LT&geo=LU&geo=LV&geo=MD&geo=ME&geo=MK&geo=MT&geo=NL&geo=NO&geo=PL&geo=PT&geo=RO&geo=RS&geo=SE&geo=SI&geo=SK&geo=TR&geo=UA&geo=UK&geo=XK')
type(dataset)
df = dataset.write('dataframe')
df=df.replace({'Bosnia and Herzegovina':'Bosnia and Herz.','Czechia':'Czech Rep.','Euro area (EA11-1999, EA12-2001, EA13-2007, EA15-2008, EA16-2009, EA17-2011, EA18-2014, EA19-2015, EA20-2023)':'Euro area', 'European Union - 27 countries (from 2020)':'EU27', 'Germany (until 1990 former territory of the FRG)':'Germany', 'Kosovo (under United Nations Security Council Resolution 1244/99)':'Kosovo','Türkiye':'Turkey'})
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Energy consumption', values='value')
df_new.drop(index={'Ukraine','United Kingdom'}, inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Natural_Gas_Prices_Consumers_Groups_Europe.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Electricity price consumers composition Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_204?time=2022-S2&nrg_cons=KWH2500-4999&tax=I_TAX&tax=X_TAX&tax=X_VAT&currency=EUR&geo=AL&geo=AT&geo=BA&geo=BE&geo=BG&geo=CY&geo=CZ&geo=DE&geo=DK&geo=EA&geo=EE&geo=EL&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=GE&geo=HR&geo=HU&geo=IE&geo=IS&geo=IT&geo=LI&geo=LT&geo=LU&geo=LV&geo=MD&geo=ME&geo=MK&geo=MT&geo=NL&geo=NO&geo=PL&geo=PT&geo=RO&geo=RS&geo=SE&geo=SI&geo=SK&geo=TR&geo=UA&geo=UK&geo=XK')
type(dataset)
df = dataset.write('dataframe')
df=df.replace({'Bosnia and Herzegovina':'Bosnia and Herz.','Czechia':'Czech Rep.','Euro area (EA11-1999, EA12-2001, EA13-2007, EA15-2008, EA16-2009, EA17-2011, EA18-2014, EA19-2015, EA20-2023)':'Euro area', 'European Union - 27 countries (from 2020)':'EU27', 'Germany (until 1990 former territory of the FRG)':'Germany', 'Kosovo (under United Nations Security Council Resolution 1244/99)':'Kosovo','Türkiye':'Turkey'})
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Taxes', values='value')
df_new.drop(index={'Ukraine','United Kingdom'}, inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Consumers_Composition_Europe.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Natural gas price consumers composition Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_202?time=2022-S2&nrg_cons=GJ20-199&tax=I_TAX&tax=X_TAX&tax=X_VAT&unit=KWH&currency=EUR&geo=AL&geo=AT&geo=BA&geo=BE&geo=BG&geo=CY&geo=CZ&geo=DE&geo=DK&geo=EA&geo=EE&geo=EL&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=GE&geo=HR&geo=HU&geo=IE&geo=IS&geo=IT&geo=LI&geo=LT&geo=LU&geo=LV&geo=MD&geo=ME&geo=MK&geo=MT&geo=NL&geo=NO&geo=PL&geo=PT&geo=RO&geo=RS&geo=SE&geo=SI&geo=SK&geo=TR&geo=UA&geo=UK&geo=XK')
type(dataset)
df = dataset.write('dataframe')
df=df.replace({'Bosnia and Herzegovina':'Bosnia and Herz.','Czechia':'Czech Rep.','Euro area (EA11-1999, EA12-2001, EA13-2007, EA15-2008, EA16-2009, EA17-2011, EA18-2014, EA19-2015, EA20-2023)':'Euro area', 'European Union - 27 countries (from 2020)':'EU27', 'Germany (until 1990 former territory of the FRG)':'Germany', 'Kosovo (under United Nations Security Council Resolution 1244/99)':'Kosovo','Türkiye':'Turkey'})
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Taxes', values='value')
df_new.drop(index={'Ukraine','United Kingdom'}, inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Natural_Gas_Prices_Consumers_Composition_Europe.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Electricity price consumers PPS Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_204?time=2022-S2&nrg_cons=KWH2500-4999&tax=I_TAX&currency=PPS&geo=AL&geo=AT&geo=BA&geo=BE&geo=BG&geo=CY&geo=CZ&geo=DE&geo=DK&geo=EA&geo=EE&geo=EL&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=GE&geo=HR&geo=HU&geo=IE&geo=IS&geo=IT&geo=LI&geo=LT&geo=LU&geo=LV&geo=MD&geo=ME&geo=MK&geo=MT&geo=NL&geo=NO&geo=PL&geo=PT&geo=RO&geo=RS&geo=SE&geo=SI&geo=SK&geo=TR&geo=UA&geo=UK&geo=XK')
type(dataset)
df_pps = dataset.write('dataframe')
df_pps=df_pps.replace({'Bosnia and Herzegovina':'Bosnia and Herz.','Czechia':'Czech Rep.','Euro area (EA11-1999, EA12-2001, EA13-2007, EA15-2008, EA16-2009, EA17-2011, EA18-2014, EA19-2015, EA20-2023)':'Euro area', 'European Union - 27 countries (from 2020)':'EU27', 'Germany (until 1990 former territory of the FRG)':'Germany', 'Kosovo (under United Nations Security Council Resolution 1244/99)':'Kosovo','Türkiye':'Turkey'})
df_new_pps = df_pps.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new_pps.rename(columns={'2022-S2': 'PPS'},inplace=True)
df_new_pps.drop(index={'United Kingdom'}, inplace=True)
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_204?time=2022-S2&nrg_cons=KWH2500-4999&tax=I_TAX&currency=EUR&geo=AL&geo=AT&geo=BA&geo=BE&geo=BG&geo=CY&geo=CZ&geo=DE&geo=DK&geo=EA&geo=EE&geo=EL&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=GE&geo=HR&geo=HU&geo=IE&geo=IS&geo=IT&geo=LI&geo=LT&geo=LU&geo=LV&geo=MD&geo=ME&geo=MK&geo=MT&geo=NL&geo=NO&geo=PL&geo=PT&geo=RO&geo=RS&geo=SE&geo=SI&geo=SK&geo=TR&geo=UA&geo=UK&geo=XK')
type(dataset)
df_eur = dataset.write('dataframe')
df_eur=df_eur.replace({'Bosnia and Herzegovina':'Bosnia and Herz.','Czechia':'Czech Rep.','Euro area (EA11-1999, EA12-2001, EA13-2007, EA15-2008, EA16-2009, EA17-2011, EA18-2014, EA19-2015, EA20-2023)':'Euro area', 'European Union - 27 countries (from 2020)':'EU27', 'Germany (until 1990 former territory of the FRG)':'Germany', 'Kosovo (under United Nations Security Council Resolution 1244/99)':'Kosovo','Türkiye':'Turkey'})
df_new_eur = df_eur.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new_eur.rename(columns={'2022-S2': 'EUR'},inplace=True)
df_new_eur.drop(index={'Ukraine','United Kingdom'}, inplace=True)
df_new=pd.concat([df_new_pps, df_new_eur], axis=1)
df_new.dropna(inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Electricity_Prices_Consumers_PPS_Europe.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Natural gas price consumers PPS Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_202?time=2022-S2&nrg_cons=GJ20-199&tax=I_TAX&unit=KWH&currency=PPS&geo=AL&geo=AT&geo=BA&geo=BE&geo=BG&geo=CY&geo=CZ&geo=DE&geo=DK&geo=EA&geo=EE&geo=EL&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=GE&geo=HR&geo=HU&geo=IE&geo=IS&geo=IT&geo=LI&geo=LT&geo=LU&geo=LV&geo=MD&geo=ME&geo=MK&geo=MT&geo=NL&geo=NO&geo=PL&geo=PT&geo=RO&geo=RS&geo=SE&geo=SI&geo=SK&geo=TR&geo=UA&geo=UK&geo=XK')
type(dataset)
df_pps = dataset.write('dataframe')
df_pps=df_pps.replace({'Bosnia and Herzegovina':'Bosnia and Herz.','Czechia':'Czech Rep.','Euro area (EA11-1999, EA12-2001, EA13-2007, EA15-2008, EA16-2009, EA17-2011, EA18-2014, EA19-2015, EA20-2023)':'Euro area', 'European Union - 27 countries (from 2020)':'EU27', 'Germany (until 1990 former territory of the FRG)':'Germany', 'Kosovo (under United Nations Security Council Resolution 1244/99)':'Kosovo','Türkiye':'Turkey'})
df_new_pps = df_pps.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new_pps.rename(columns={'2022-S2': 'PPS'},inplace=True)
df_new_pps.drop(index={'United Kingdom'}, inplace=True)
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_pc_202?time=2022-S2&nrg_cons=GJ20-199&tax=I_TAX&unit=KWH&currency=EUR&geo=AL&geo=AT&geo=BA&geo=BE&geo=BG&geo=CY&geo=CZ&geo=DE&geo=DK&geo=EA&geo=EE&geo=EL&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=GE&geo=HR&geo=HU&geo=IE&geo=IS&geo=IT&geo=LI&geo=LT&geo=LU&geo=LV&geo=MD&geo=ME&geo=MK&geo=MT&geo=NL&geo=NO&geo=PL&geo=PT&geo=RO&geo=RS&geo=SE&geo=SI&geo=SK&geo=TR&geo=UA&geo=UK&geo=XK')
type(dataset)
df_eur = dataset.write('dataframe')
df_eur=df_eur.replace({'Bosnia and Herzegovina':'Bosnia and Herz.','Czechia':'Czech Rep.','Euro area (EA11-1999, EA12-2001, EA13-2007, EA15-2008, EA16-2009, EA17-2011, EA18-2014, EA19-2015, EA20-2023)':'Euro area', 'European Union - 27 countries (from 2020)':'EU27', 'Germany (until 1990 former territory of the FRG)':'Germany', 'Kosovo (under United Nations Security Council Resolution 1244/99)':'Kosovo','Türkiye':'Turkey'})
df_new_eur = df_eur.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new_eur.rename(columns={'2022-S2': 'EUR'},inplace=True)
df_new_eur.drop(index={'Ukraine','United Kingdom'}, inplace=True)
df_new=pd.concat([df_new_pps, df_new_eur], axis=1)
df_new.dropna(inplace=True)
df_new.to_csv('data_Eurostat_Energy_Prices/Natural_Gas_Prices_Consumers_PPS_Europe.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
