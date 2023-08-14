from pyjstat import pyjstat
import requests
import os
from datetime import datetime
import pandas as pd
os.makedirs('data_IRENA', exist_ok=True)

#Data Gathering from IRENA API Renewable Energy Capacity
url = 'https://pxweb.irena.org:443/api/v1/en/IRENASTAT/Power Capacity and Generation/RECAP_2023_cycle2.px'
query = {
     "query": [
            {
            "code": "Technology",
            "selection": {
                "filter": "item",
                "values": [
                "0",
                "2",
                "4",
                "5",
                "6",
                "7",
                "8",
                "9",
                "10",
                "11",
                "18"
                ]
            }
            }
        ],
     "response": {
         "format": "json-stat2"
     }
 }

result = requests.post(url, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')

###TOTAL RENEWABLE TECHNOLOGY###

# Define technologies list
technologies = ['Renewable hydropower', 'Solar', 'Wind', 'Bioenergy', 'Geothermal', 'Marine']
regions = ['Africa', 'Asia', 'Central America and the Caribbean', 'Eurasia', 'Europe', 'Middle East', 'Oceania', 'South America', 'North America']

# Total Renewable Energy Capacity Cumulative World
filtered_world_df = df[(df['Region/country/area'] == 'World') & (df['Technology'] == 'Total renewable energy')]
pivot_world_df = filtered_world_df.pivot(index='Technology', columns='Year', values='value').round(2)
pivot_world_df.to_csv("data_IRENA_Renewable_Capacity/IRENA_Total_Renewable_Capacity_World.csv")

# Total Renewable Energy Capacity Net Additions World
net_additions_world_df = pivot_world_df.diff(axis=1).drop(columns='2000').round(2)
net_additions_world_df.to_csv("data_IRENA_Renewable_Capacity/IRENA_Total_Renewable_Capacity_World_Net_Additions.csv")

# Total Renewable Energy Capacity Cumulative World Per Technology
filtered_tech_df = df[(df['Region/country/area'] == 'World') & (df['Technology'].isin(technologies))]
pivot_tech_df = filtered_tech_df.pivot(index='Technology', columns='Year', values='value').round(2)
pivot_tech_df = pivot_tech_df.sort_values(by=pivot_tech_df.columns[-1], ascending=False)
pivot_tech_df.to_csv("data_IRENA_Renewable_Capacity/IRENA_Total_Renewable_Capacity_Per_Technology.csv")

# Total Renewable Energy Net Capacity Additions World Per Technology
net_additions_tech_df = pivot_tech_df.diff(axis=1).drop(columns='2000').sort_values(by=pivot_tech_df.columns[-1], ascending=False).round(2)
net_additions_tech_df.to_csv("data_IRENA_Renewable_Capacity/IRENA_Total_Renewable_Capacity_Per_Technology_Net_Additions.csv")

# Total Renewable Energy Capacity Cumulative World Per Technology Share
share_tech_df = (pivot_tech_df.divide(pivot_tech_df.sum(), axis=1) * 100).sort_values(by=pivot_tech_df.columns[-1], ascending=False).round(2)
share_tech_df.to_csv("data_IRENA_Renewable_Capacity/IRENA_Total_Renewable_Capacity_Per_Technology_Share.csv")

# Total Renewable Energy Capacity Cumulative Regions
filtered_regions_df = df[(df['Region/country/area'].isin(regions)) & (df['Technology'] == 'Total renewable energy')]
pivot_regions_df = filtered_regions_df.pivot(index='Region/country/area', columns='Year', values='value').round(2)
pivot_regions_df = pivot_regions_df.sort_values(by=pivot_regions_df.columns[-1], ascending=False)
pivot_regions_df.to_csv("data_IRENA_Renewable_Capacity/IRENA_Total_Renewable_Capacity_Regions.csv")

# Total Renewable Energy Net Capacity Additions Regions
net_additions_regions_df = pivot_regions_df.diff(axis=1).drop(columns='2000').sort_values(by=pivot_regions_df.columns[-1], ascending=False).round(2)
net_additions_regions_df.to_csv("data_IRENA_Renewable_Capacity/IRENA_Total_Renewable_Capacity_Regions_Net_Additions.csv")

# Total Renewable Energy Capacity Regions Share
share_regions_df = (pivot_regions_df.divide(pivot_regions_df.sum(), axis=1) * 100).sort_values(by=pivot_regions_df.columns[-1], ascending=False).round(2)
share_regions_df.to_csv("data_IRENA_Renewable_Capacity/IRENA_Total_Renewable_Capacity_Regions_Share.csv")

# Total Renewable Energy Capacity Countries
excluded_entities = regions + ['World', 'European Union']
filtered_countries_df = df[~df['Region/country/area'].isin(excluded_entities) & (df['Technology'] == 'Total renewable energy')]
pivot_countries_df = filtered_countries_df.pivot(index='Region/country/area', columns='Year', values='value').round(2)
pivot_countries_df = pivot_countries_df.sort_values(by=pivot_countries_df.columns[-1], ascending=False)
pivot_countries_df.to_csv("data_IRENA_Renewable_Capacity/IRENA_Total_Renewable_Capacity_Countries.csv")

# Total Renewable Energy Capacity Countries Latest Year
latest_year = pivot_countries_df.columns[-1]
latest_year_df = pivot_countries_df[[latest_year]].sort_values(by=latest_year, ascending=False)
latest_year_df = latest_year_df[latest_year_df[latest_year] != 0]
latest_year_df.to_csv(f"data_IRENA_Renewable_Capacity/IRENA_Total_Renewable_Capacity_Countries_Latest_Year.csv")

# Total Renewable Energy Net Capacity Additions Countries
net_additions_countries_df = pivot_countries_df.diff(axis=1).drop(columns='2000').sort_values(by=pivot_countries_df.columns[-1], ascending=False).round(2)
net_additions_countries_df.to_csv("data_IRENA_Renewable_Capacity/IRENA_Total_Renewable_Capacity_Countries_Net_Additions.csv")

# Total Renewable Energy Net Capacity Additions Countries Latest Year
latest_year_net_additions = net_additions_countries_df[[latest_year]].sort_values(by=latest_year, ascending=False)
latest_year_net_additions = latest_year_net_additions[latest_year_net_additions[latest_year] != 0]
latest_year_net_additions.to_csv(f"data_IRENA_Renewable_Capacity/IRENA_Total_Renewable_Capacity_Countries_Net_Additions_Latest_Year.csv")
