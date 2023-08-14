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

def generate_files_for_technology(technology, df, regions=['Africa', 'Asia', 'Central America and the Caribbean', 'Eurasia', 'Europe', 'Middle East', 'Oceania', 'South America', 'North America']):
    # Filename prefix
    filename_prefix = "data_IRENA_Renewable_Capacity/IRENA_" + technology.title().replace(" ", "_") + "_Capacity"
    
    # World Data
    filtered_world_df = df[(df['Region/country/area'] == 'World') & (df['Technology'] == technology)]
    pivot_world_df = filtered_world_df.pivot(index='Technology', columns='Year', values='value').round(2)
    pivot_world_df.to_csv(filename_prefix + "_World.csv")

    # World Net Additions
    net_additions_world_df = pivot_world_df.diff(axis=1).drop(columns='2000').round(2)
    net_additions_world_df.to_csv(filename_prefix + "_World_Net_Additions.csv")

    # World Share
    share_tech_df = (pivot_world_df.divide(pivot_world_df.sum(), axis=1) * 100).round(2)
    share_tech_df.to_csv(filename_prefix + "_World_Share.csv")

    # Regions Data
    filtered_regions_df = df[(df['Region/country/area'].isin(regions)) & (df['Technology'] == technology)]
    pivot_regions_df = filtered_regions_df.pivot(index='Region/country/area', columns='Year', values='value').round(2)
    pivot_regions_df = pivot_regions_df.sort_values(by=pivot_regions_df.columns[-1], ascending=False)
    pivot_regions_df.to_csv(filename_prefix + "_Regions.csv")

    # Regions Net Additions
    net_additions_regions_df = pivot_regions_df.diff(axis=1).drop(columns='2000').sort_values(by=pivot_regions_df.columns[-1], ascending=False).round(2)
    net_additions_regions_df.to_csv(filename_prefix + "_Regions_Net_Additions.csv")

    # Regions Share
    share_regions_df = (pivot_regions_df.divide(pivot_regions_df.sum(), axis=1) * 100).sort_values(by=pivot_regions_df.columns[-1], ascending=False).round(2)
    share_regions_df.to_csv(filename_prefix + "_Regions_Share.csv")

    # Countries Data
    excluded_entities = regions + ['World', 'European Union']
    filtered_countries_df = df[~df['Region/country/area'].isin(excluded_entities) & (df['Technology'] == technology)]
    pivot_countries_df = filtered_countries_df.pivot(index='Region/country/area', columns='Year', values='value').round(2)
    pivot_countries_df = pivot_countries_df.sort_values(by=pivot_countries_df.columns[-1], ascending=False)
    pivot_countries_df.to_csv(filename_prefix + "_Countries.csv")

    # Countries Net Additions
    net_additions_countries_df = pivot_countries_df.diff(axis=1).drop(columns='2000').sort_values(by=pivot_countries_df.columns[-1], ascending=False).round(2)
    net_additions_countries_df.to_csv(filename_prefix + "_Countries_Net_Additions.csv")

technologies = ["Bioenergy", "Geothermal", "Wind", "Solar", "Renewable hydropower", "Offshore wind energy", "Onshore wind energy", "Marine", "Total renewable energy"]
for tech in technologies:
    generate_files_for_technology(tech, df)

#ADDITIONAL FILES FOR TECHNOLOGIES AND WIND SUBTECHNOLOGIES

# Total Renewable Energy Capacity World Per Technology
specific_technologies = ["Bioenergy", "Geothermal", "Wind", "Solar", "Renewable hydropower", "Marine"]
filtered_tech_df = df[(df['Region/country/area'] == 'World') & (df['Technology'].isin(specific_technologies))]
pivot_tech_df = filtered_tech_df.pivot(index='Technology', columns='Year', values='value').round(2)
pivot_tech_df = pivot_tech_df.sort_values(by=pivot_tech_df.columns[-1], ascending=False)
pivot_tech_df.to_csv("data_IRENA_Renewable_Capacity/IRENA_Total_Renewable_Energy_Capacity_Per_Technology.csv")

# Total Renewable Energy Net Capacity Additions World Per Technology
net_additions_tech_df = pivot_tech_df.diff(axis=1).drop(columns='2000').sort_values(by=pivot_tech_df.columns[-1], ascending=False).round(2)
net_additions_tech_df.to_csv("data_IRENA_Renewable_Capacity/IRENA_Total_Renewable_Energy_Capacity_Per_Technology_Net_Additions.csv")

# Total Wind Sub-Technologies Capacity World Per Technology
wind_sub_technologies = ['Onshore wind energy', 'Offshore wind energy']
filtered_wind_subtech_df = df[(df['Region/country/area'] == 'World') & (df['Technology'].isin(wind_sub_technologies))]
pivot_wind_subtech_df = filtered_wind_subtech_df.pivot(index='Technology', columns='Year', values='value').round(2)
pivot_wind_subtech_df = pivot_wind_subtech_df.sort_values(by=pivot_wind_subtech_df.columns[-1], ascending=False)
pivot_wind_subtech_df.to_csv("data_IRENA_Renewable_Capacity/IRENA_Wind_Sub_Technologies_Capacity_Per_Technology.csv")

# Total Wind Sub-Technologies Net Capacity Additions World Per Technology
net_additions_wind_subtech_df = pivot_wind_subtech_df.diff(axis=1).drop(columns='2000').sort_values(by=pivot_wind_subtech_df.columns[-1], ascending=False).round(2)
net_additions_wind_subtech_df.to_csv("data_IRENA_Renewable_Capacity/IRENA_Wind_Sub_Technologies_Capacity_Per_Technology_Net_Additions.csv")