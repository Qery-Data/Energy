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