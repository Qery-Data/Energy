from pyjstat import pyjstat
import requests
import os
from datetime import datetime
import pandas as pd
os.makedirs('data_IRENA_Renewable_Capacity', exist_ok=True)

shorthand_dict = {
    'Bahamas (the)': 'Bahamas',
    'Bolivia (Plurinational State of)': 'Bolivia',
    'China, Hong Kong Special Administrative Region': 'Hong Kong SAR',
    'Congo (the)': 'Congo',
    'Democratic Republic of the Congo (the)': 'DR Congo',
    'Dominican Republic (the)': 'Dominican Republic',
    'Iran (Islamic Republic of)': 'Iran',
    "Lao People's Democratic Republic (the)": 'Lao PDR',
    'Netherlands (Kingdom of the)': 'Netherlands',
    'Philippines (the)': 'Philippines',
    'Republic of Korea (the)': 'Korea',
    'Republic of Moldova (the)': 'Moldova',
    'Russian Federation (the)': 'Russia',
    'State of Palestine (the)': 'State of Palestine',
    'Sudan (the)': 'Sudan',
    'Syrian Arab Republic (the)': 'Syria',
    'United Arab Emirates (the)': 'United Arab Emirates',
    'United Kingdom of Great Britain and Northern Ireland (the)': 'United Kingdom',
    'United Republic of Tanzania (the)': 'Tanzania',
    'United States of America (the)': 'United States',
    'Venezuela (Bolivarian Republic of)': 'Venezuela'
}
technology_name_mapping = {
    'Total Renewable': 'Total renewable energy',
    'Solar energy': 'Solar',
    'Wind energy': 'Wind',
    'Hydropower (excl. Pumped Storage)': 'Renewable hydropower',
    'Marine energy': 'Marine',
    'Bioenergy': 'Bioenergy',  
    'Geothermal energy': 'Geothermal'
}

#RENEWABLE CAPACITY FOR REGIONS AND COUNTRIES

#Regions
url_regions = 'https://pxweb.irena.org:443/api/v1/en/IRENASTAT/Power Capacity and Generation/Region_ELECSTAT_2024_H2.px'
query = {
  "query": [
    {
      "code": "Technology",
      "selection": {
        "filter": "item",
        "values": [
          "0",
          "1",
          "2",
          "3",
          "4",
          "5",
          "6"
        ]
      }
    },
    {
      "code": "Data Type",
      "selection": {
        "filter": "item",
        "values": [
          "1"
        ]
      }
    },
    {
      "code": "Year",
      "selection": {
        "filter": "item",
        "values": [
          "0",
          "1",
          "10",
          "11",
          "12",
          "13",
          "14",
          "15",
          "16",
          "17",
          "18",
          "19",
          "2",
          "20",
          "21",
          "22",
          "23",
          "3",
          "4",
          "5",
          "6",
          "7",
          "8",
          "9"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result_regions = requests.post(url_regions, json=query)
df_regions = pyjstat.Dataset.read(result_regions.text).write('dataframe')
df_regions['Technology'] = df_regions['Technology'].replace(technology_name_mapping)
df_regions.rename(columns={'Region': 'Region/country/area'}, inplace=True)


#Countries
url_countries = 'https://pxweb.irena.org:443/api/v1/en/IRENASTAT/Power Capacity and Generation/Country_ELECSTAT_2024_H2.px'
query = {
  "query": [
    {
      "code": "Technology",
      "selection": {
        "filter": "item",
        "values": [
          "0",
          "1",
          "2",
          "3",
          "4",
          "5",
          "8",
          "10",
          "11",
          "12"
        ]
      }
    },
    {
      "code": "Data Type",
      "selection": {
        "filter": "item",
        "values": [
          "1"
        ]
      }
    },
    {
      "code": "Grid connection",
      "selection": {
        "filter": "item",
        "values": [
          "0"
        ]
      }
    },
        {
      "code": "Year",
      "selection": {
        "filter": "item",
        "values": [
          "0",
          "1",
          "10",
          "11",
          "12",
          "13",
          "14",
          "15",
          "16",
          "17",
          "18",
          "19",
          "2",
          "20",
          "21",
          "22",
          "23",
          "3",
          "4",
          "5",
          "6",
          "7",
          "8",
          "9"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result_countries = requests.post(url_countries, json=query)
df_countries = pyjstat.Dataset.read(result_countries.text).write('dataframe')
solar_aggregated = df_countries[df_countries['Technology'].isin(['Solar photovoltaic', 'Solar thermal energy'])].groupby(['Country/area', 'Year']).agg({'value': 'sum'}).reset_index()
solar_aggregated['Technology'] = 'Solar'
wind_aggregated = df_countries[df_countries['Technology'].isin(['Onshore wind energy', 'Offshore wind energy'])].groupby(['Country/area', 'Year']).agg({'value': 'sum'}).reset_index()
wind_aggregated['Technology'] = 'Wind'
bioenergy_aggregated = df_countries[df_countries['Technology'].isin(['Solid biofuels', 'Liquid biofuels', 'Biogas'])].groupby(['Country/area', 'Year']).agg({'value': 'sum'}).reset_index()
bioenergy_aggregated['Technology'] = 'Bioenergy'
df_countries['Technology'] = df_countries['Technology'].replace(technology_name_mapping)
df_countries = pd.concat([df_countries, solar_aggregated, wind_aggregated, bioenergy_aggregated], ignore_index=True)
df_countries.rename(columns={'Country/area': 'Region/country/area'}, inplace=True)
df_countries['Region/country/area'] = df_countries['Region/country/area'].replace(shorthand_dict)

# Main function for regions
def generate_files_for_technology(technology, df_regions, regions=['Africa', 'Asia', 'Central America and the Caribbean', 'Eurasia', 'Europe', 'Middle East', 'Oceania', 'South America', 'North America']):
    # Filename prefix
    filename_prefix = "data_IRENA_Renewable_Capacity/IRENA_" + technology.title().replace(" ", "_") + "_Capacity"
    
    # World Data
    filtered_world_df = df_regions[(df_regions['Region/country/area'] == 'World') & (df_regions['Technology'] == technology)]
    pivot_world_df = filtered_world_df.pivot(index='Technology', columns='Year', values='value').round(2)
    pivot_world_df.to_csv(filename_prefix + "_World.csv")

    # World Net Additions
    net_additions_world_df = pivot_world_df.diff(axis=1).drop(columns='2000').round(2)
    net_additions_world_df.to_csv(filename_prefix + "_World_Net_Additions.csv")

    # Regions Data
    filtered_regions_df = df_regions[(df_regions['Region/country/area'].isin(regions)) & (df_regions['Technology'] == technology)]
    pivot_regions_df = filtered_regions_df.pivot(index='Region/country/area', columns='Year', values='value').round(2)
    pivot_regions_df = pivot_regions_df[pivot_regions_df.sum(axis=1) != 0]
    pivot_regions_df = pivot_regions_df.sort_values(by=[pivot_regions_df.columns[-1], 'Region/country/area'], ascending=[False, True])
    pivot_regions_df.to_csv(filename_prefix + "_Regions.csv")

    # Regions Net Additions
    net_additions_regions_df = pivot_regions_df.diff(axis=1).drop(columns='2000').sort_values(by=[pivot_regions_df.columns[-1], 'Region/country/area'], ascending=[False, True]).round(2)
    net_additions_regions_df.to_csv(filename_prefix + "_Regions_Net_Additions.csv")

    # Regions Share
    share_regions_df = (pivot_regions_df.divide(pivot_regions_df.sum(), axis=1) * 100).sort_values(by=pivot_regions_df.columns[-1], ascending=False).round(2)
    share_regions_df.to_csv(filename_prefix + "_Regions_Share.csv")

technologies = ['Total renewable energy', 'Solar', 'Wind',
       'Renewable hydropower', 'Marine', 'Bioenergy',
       'Geothermal']
for tech in technologies:
    generate_files_for_technology(tech, df_regions)

# Main function for countries
def generate_files_for_technology(technology, df_countries, regions=['Africa', 'Asia', 'Central America and the Caribbean', 'Eurasia', 'Europe', 'Middle East', 'Oceania', 'South America', 'North America']):
    # Filename prefix
    filename_prefix = "data_IRENA_Renewable_Capacity/IRENA_" + technology.title().replace(" ", "_") + "_Capacity"
    
    # Countries Data
    filtered_countries_df = df_countries[df_countries['Technology'] == technology]
    pivot_countries_df = filtered_countries_df.pivot(index='Region/country/area', columns='Year', values='value').round(2)
    pivot_countries_df.sort_index(inplace=True)
    pivot_countries_df.to_csv(filename_prefix + "_Countries.csv")

    # Countries Latest Year
    latest_year = pivot_countries_df.columns[-1]
    latest_year_df = pivot_countries_df[[latest_year]].sort_values(by=[latest_year, 'Region/country/area'], ascending=[False, True])
    latest_year_df = latest_year_df[latest_year_df[latest_year] != 0]
    latest_year_df.to_csv(filename_prefix + "_Countries_Latest_Year.csv")

    # Countries Net Additions
    net_additions_countries_df = pivot_countries_df.diff(axis=1).drop(columns='2000').round(2)
    net_additions_countries_df.sort_index(inplace=True)
    net_additions_countries_df.to_csv(filename_prefix + "_Countries_Net_Additions.csv")

    # Countries Net Additions Latest Year
    latest_year_net_additions = net_additions_countries_df[[latest_year]].sort_values(by=[latest_year, 'Region/country/area'], ascending=[False, True])
    latest_year_net_additions = net_additions_countries_df.loc[latest_year_df.index][[latest_year]].sort_values(by=[latest_year, 'Region/country/area'], ascending=[False, True])
    latest_year_net_additions.to_csv(filename_prefix + "_Countries_Net_Additions_Latest_Year.csv")

technologies = ['Total renewable energy','Solar','Wind','Onshore wind energy','Offshore wind energy','Renewable hydropower','Bioenergy','Geothermal']
for tech in technologies:
    generate_files_for_technology(tech, df_countries)

#ADDITIONAL FILES FOR TECHNOLOGIES AND WIND SUBTECHNOLOGIES

# Total Renewable Energy Capacity World Per Technology
specific_technologies = ["Bioenergy", "Geothermal", "Wind", "Solar", "Renewable hydropower", "Marine"]
filtered_tech_df = df_regions[(df_regions['Region/country/area'] == 'World') & (df_regions['Technology'].isin(specific_technologies))]
pivot_tech_df = filtered_tech_df.pivot(index='Technology', columns='Year', values='value').round(2)
pivot_tech_df = pivot_tech_df.sort_values(by=pivot_tech_df.columns[-1], ascending=False)
pivot_tech_df.to_csv("data_IRENA_Renewable_Capacity/IRENA_Total_Renewable_Energy_Capacity_Per_Technology.csv")

# Total Renewable Energy Net Capacity Additions World Per Technology
net_additions_tech_df = pivot_tech_df.diff(axis=1).drop(columns='2000').sort_values(by=pivot_tech_df.columns[-1], ascending=False).round(2)
net_additions_tech_df.to_csv("data_IRENA_Renewable_Capacity/IRENA_Total_Renewable_Energy_Capacity_Per_Technology_Net_Additions.csv")

#ADDITIONAL FILES FOR RENEWABLE ENERGY SHARES OF TOTAL CAPACITY

#Data Gathering from IRENA API
url = 'https://pxweb.irena.org:443/api/v1/en/IRENASTAT/Power Capacity and Generation/RESHARE_2024_H2.px'
query = {
     "query": [
    {
      "code": "Indicator",
      "selection": {
        "filter": "item",
        "values": [
          "1"
        ]
      }
    },
    {
      "code": "Year",
      "selection": {
        "filter": "item",
        "values": [
          "0",
          "1",
          "10",
          "11",
          "12",
          "13",
          "14",
          "15",
          "16",
          "17",
          "18",
          "19",
          "2",
          "20",
          "21",
          "22",
          "23",
          "3",
          "4",
          "5",
          "6",
          "7",
          "8",
          "9"
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
df['Region/country/area'] = df['Region/country/area'].replace(shorthand_dict)

# Renewable Energy Share of Electric Capacity World
filtered_world_df = df[(df['Region/country/area'] == 'World')]
pivot_world_df = filtered_world_df.pivot(index='Indicator', columns='Year', values='value').round(2)
pivot_world_df.to_csv("data_IRENA_Renewable_Capacity/IRENA_Renewable_Share_Electric_Capacity_World.csv")

# Renewable Energy Share of Electric Capacity Regions
regions = ['Africa', 'Asia', 'Central America and the Caribbean', 'Eurasia', 'Europe', 'Middle East', 'Oceania', 'South America', 'North America']
filtered_regions_df = df[(df['Region/country/area'].isin(regions))]
pivot_regions_df = filtered_regions_df.pivot(index='Region/country/area', columns='Year', values='value').round(2)
pivot_regions_df = pivot_regions_df.sort_values(by=pivot_regions_df.columns[-1], ascending=False)
pivot_regions_df.to_csv("data_IRENA_Renewable_Capacity/IRENA_Renewable_Share_Electric_Capacity_Regions.csv")

# Total Renewable Energy Capacity Countries
excluded_entities = regions + ['World', 'European Union']
filtered_countries_df = df[~df['Region/country/area'].isin(excluded_entities)]
pivot_countries_df = filtered_countries_df.pivot(index='Region/country/area', columns='Year', values='value').round(2)
pivot_countries_df = pivot_countries_df.sort_values(by=pivot_countries_df.columns[-1], ascending=False)
pivot_countries_df.to_csv("data_IRENA_Renewable_Capacity/IRENA_Renewable_Share_Electric_Capacity_Countries.csv")
