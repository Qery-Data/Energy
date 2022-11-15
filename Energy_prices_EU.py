from pyjstat import pyjstat
import requests
import os
import json
from datetime import datetime
import locale
import io
import pandas as pd
os.makedirs('data', exist_ok=True)

#Energy price households 
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/nrg_pc_204?lastTimePeriod=1&consom=4161903&tax=I_TAX&currency=EUR&geo=AL&geo=AT&geo=BA&geo=BE&geo=BG&geo=CY&geo=CZ&geo=DE&geo=DK&geo=EA&geo=EE&geo=EL&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=GE&geo=HR&geo=HU&geo=IE&geo=IS&geo=IT&geo=LI&geo=LT&geo=LU&geo=LV&geo=MD&geo=ME&geo=MK&geo=MT&geo=NL&geo=NO&geo=PL&geo=PT&geo=RO&geo=RS&geo=SE&geo=SI&geo=SK&geo=TR&geo=UA&geo=UK&geo=XK')
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='time', columns='geo', values='value')
df_new.to_csv('data/Energy_prices_households_EU.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%d')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Energy price households Europe composition
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/nrg_pc_204?lastTimePeriod=1&consom=4161903&tax=I_TAX&tax=X_TAX&tax=X_VAT&currency=EUR&geo=AL&geo=AT&geo=BA&geo=BE&geo=BG&geo=CY&geo=CZ&geo=DE&geo=DK&geo=EA&geo=EE&geo=EL&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=GE&geo=HR&geo=HU&geo=IE&geo=IS&geo=IT&geo=LI&geo=LT&geo=LU&geo=LV&geo=MD&geo=ME&geo=MK&geo=MT&geo=NL&geo=NO&geo=PL&geo=PT&geo=RO&geo=RS&geo=SE&geo=SI&geo=SK&geo=TR&geo=UA&geo=UK&geo=XK')
type(dataset)
df = dataset.write('dataframe')
df=df.replace({'Bosnia and Herzegovina':'Bosnia and Herz.','Czechia':'Czech Rep.','Euro area (EA11-1999, EA12-2001, EA13-2007, EA15-2008, EA16-2009, EA17-2011, EA18-2014, EA19-2015)':'Euro area', 'European Union - 27 countries (from 2020)':'EU27', 'Germany (until 1990 former territory of the FRG)':'Germany', 'Kosovo (under United Nations Security Council Resolution 1244/99)':'Kosovo','Türkiye':'Turkey'})
df_new = df.pivot(index='geo', columns='tax', values='value')
df_new.drop(index={'Ukraine','United Kingdom'}, inplace=True)
df_new.to_csv('data/Energy_prices_households_composition_EU.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%d')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Energy price households PPS Europe
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/nrg_pc_204?lastTimePeriod=1&consom=4161903&tax=I_TAX&currency=PPS&geo=AL&geo=AT&geo=BA&geo=BE&geo=BG&geo=CY&geo=CZ&geo=DE&geo=DK&geo=EA&geo=EE&geo=EL&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=GE&geo=HR&geo=HU&geo=IE&geo=IS&geo=IT&geo=LI&geo=LT&geo=LU&geo=LV&geo=MD&geo=ME&geo=MK&geo=MT&geo=NL&geo=NO&geo=PL&geo=PT&geo=RO&geo=RS&geo=SE&geo=SI&geo=SK&geo=TR&geo=UA&geo=UK&geo=XK')
type(dataset)
df_pps = dataset.write('dataframe')
df_pps=df_pps.replace({'Bosnia and Herzegovina':'Bosnia and Herz.','Czechia':'Czech Rep.','Euro area (EA11-1999, EA12-2001, EA13-2007, EA15-2008, EA16-2009, EA17-2011, EA18-2014, EA19-2015)':'Euro area', 'European Union - 27 countries (from 2020)':'EU27', 'Germany (until 1990 former territory of the FRG)':'Germany', 'Kosovo (under United Nations Security Council Resolution 1244/99)':'Kosovo','Türkiye':'Turkey'})
df_new_pps = df_pps.pivot(index='geo', columns='time', values='value')
df_new_pps.rename(columns={'2022S1': 'PPS'},inplace=True)
df_new_pps.drop(index={'United Kingdom'}, inplace=True)
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/nrg_pc_204?lastTimePeriod=1&consom=4161903&tax=I_TAX&currency=EUR&geo=AL&geo=AT&geo=BA&geo=BE&geo=BG&geo=CY&geo=CZ&geo=DE&geo=DK&geo=EA&geo=EE&geo=EL&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=GE&geo=HR&geo=HU&geo=IE&geo=IS&geo=IT&geo=LI&geo=LT&geo=LU&geo=LV&geo=MD&geo=ME&geo=MK&geo=MT&geo=NL&geo=NO&geo=PL&geo=PT&geo=RO&geo=RS&geo=SE&geo=SI&geo=SK&geo=TR&geo=UA&geo=UK&geo=XK')
type(dataset)
df_eur = dataset.write('dataframe')
df_eur=df_eur.replace({'Bosnia and Herzegovina':'Bosnia and Herz.','Czechia':'Czech Rep.','Euro area (EA11-1999, EA12-2001, EA13-2007, EA15-2008, EA16-2009, EA17-2011, EA18-2014, EA19-2015)':'Euro area', 'European Union - 27 countries (from 2020)':'EU27', 'Germany (until 1990 former territory of the FRG)':'Germany', 'Kosovo (under United Nations Security Council Resolution 1244/99)':'Kosovo','Türkiye':'Turkey'})
df_new_eur = df_eur.pivot(index='geo', columns='time', values='value')
df_new_eur.rename(columns={'2022S1': 'EUR'},inplace=True)
df_new_eur.drop(index={'Ukraine','United Kingdom'}, inplace=True)
df_new=pd.concat([df_new_pps, df_new_eur], axis=1)
df_new.dropna(inplace=True)
df_new.to_csv('data/Energy_prices_households_PPS_EU.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%d')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')