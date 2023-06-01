import pandas as pd
import requests
import io
import os
import time
os.makedirs('data_IRENA', exist_ok=True)

#IRENA Renewable Energy Capacity Data per Country/region/area
def process_data(url: str, filename_start: str) -> None:
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        return None
    except Exception as err:
        print(f"An error occurred: {err}")
        return None

    data = pd.read_csv(io.StringIO(response.text))
    # Get the list of years from the column names
    years = [col for col in data.columns if col.isdigit()]
    
    # Convert years columns to numeric
    for year in years:
        data[year] = pd.to_numeric(data[year], errors='coerce')

    # Define regions
    regions = ['World', 'Africa', 'Asia', 'C America + Carib', 'Eurasia', 'Europe', 'European Union (27)', 'Middle East', 'Oceania', 'S America', 'N America']

    # Separate regions and countries
    regions_data = data[data['Region/country/area'].isin(regions)].copy()
    countries_data = data[~data['Region/country/area'].isin(regions)].copy()

    # Save the total per year data for regions and countries
    regions_data['Region/country/area'] = regions_data['Region/country/area'].replace({'C America + Carib': 'Central America and the Caribbean','European Union (27)': 'EU27','S America': 'South America','N America': 'North America'})
    regions_to_drop = ['EU27', 'World']
    regions_data = regions_data[~regions_data['Region/country/area'].isin(regions_to_drop)]
    regions_data.drop('Technology', axis=1, inplace=True)
    regions_data = regions_data.loc[~(regions_data[years]==0).all(axis=1)]
    latest_year = str(regions_data.columns[1:].max())
    regions_data = regions_data.sort_values(by=latest_year, ascending=False)        
    regions_data.to_csv(filename_start + '_Regions_Total_Per_Year.csv', index=False)

    countries_data.drop('Technology', axis=1, inplace=True)
    countries_data.to_csv(filename_start + '_Countries_Total_Per_Year.csv', index=False)

    # Get the top 5 countries for the latest year
    latest_year = max(years)
    top5_countries = countries_data.nlargest(5, latest_year)

    # Get the other countries
    other_countries = countries_data.loc[~countries_data['Region/country/area'].isin(top5_countries['Region/country/area'])]

    # Sum the values for the other countries
    other_countries_sum = other_countries.sum(numeric_only=True)
    other_countries_sum_df = pd.DataFrame(other_countries_sum).transpose()
    other_countries_sum_df['Region/country/area'] = 'Others'

    # Concatenate the top 5 and other countries into a new DataFrame
    top5 = pd.concat([top5_countries, other_countries_sum_df], ignore_index=True)

    # Ensure the 'Region/country/area' column is not of type float
    top5['Region/country/area'] = top5['Region/country/area'].astype(str)

    # Write the DataFrame to a CSV file
    top5.to_csv(filename_start + '_Top5_Countries.csv', index=False)

    # Create an empty DataFrame for regions net additions
    regions_net_additions = pd.DataFrame()

    # Fill the net additions DataFrame for regions
    for i in range(1, len(years)):
        regions_net_additions[years[i]] = regions_data[years[i]] - regions_data[years[i - 1]]

    # Set the 'Region/country/area' column in the regions net additions DataFrame
    regions_net_additions['Region/country/area'] = regions_data['Region/country/area']

    # Rearrange the columns to put 'Region/country/area' at the start
    cols = regions_net_additions.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    regions_net_additions = regions_net_additions[cols]

    # Save the regions net additions data without 'Region/country/area' column
    years = [str(year) for year in range(int(regions_net_additions.columns[1:].min()), int(regions_net_additions.columns[1:].max()) + 1)]
    regions_net_additions = regions_net_additions.loc[~(regions_net_additions[years]==0).all(axis=1)]
    latest_year = str(regions_net_additions.columns[1:].max())
    regions_net_additions = regions_net_additions.sort_values(by=latest_year, ascending=False)        
    regions_net_additions.to_csv(filename_start + '_Regions_Net_Additions.csv', index=False)

    
    # Countries Net Additions
    countries_net_additions = pd.DataFrame()
    for i in range(1, len(years)):
        countries_net_additions[years[i]] = countries_data[years[i]] - countries_data[years[i - 1]]
    countries_net_additions['Region/country/area'] = countries_data['Region/country/area']
    cols = countries_net_additions.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    countries_net_additions = countries_net_additions[cols]
    countries_net_additions.to_csv(filename_start + '_Countries_Net_Additions_Per_Year.csv', index=False)

    # Save the latest year data and net additions for countries with non-zero values
    latest_countries_data = countries_data[['Region/country/area', latest_year]]
    latest_countries_data = latest_countries_data[latest_countries_data[latest_year] != 0]
    latest_countries_data.to_csv(filename_start + '_Countries_Total_Latest_Year.csv', index=False)
    
    latest_countries_net_additions = countries_net_additions[['Region/country/area', latest_year]]
    latest_countries_net_additions = latest_countries_net_additions[latest_countries_net_additions[latest_year] != 0]
    latest_countries_net_additions.to_csv(filename_start + '_Countries_Net_Additions_Latest_Year.csv', index=False)

    # Calculate the total for each year
    total_per_year = regions_data[years].sum()

    # Calculate the share of each region for each year
    regions_share_per_year = regions_data[years].divide(total_per_year)

    # Set the 'Region/country/area' column in the regions share per year DataFrame
    regions_share_per_year['Region/country/area'] = regions_data['Region/country/area']

    # Rearrange the columns to put 'Region/country/area' at the start
    cols = regions_share_per_year.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    regions_share_per_year = regions_share_per_year[cols]

    # Filter rows where the latest year share is less than 0.5%
    latest_year = max(years)
    regions_share_per_year = regions_share_per_year[regions_share_per_year[latest_year] >= 0.005] # Because the shares are in the range 0-1, 0.5% is 0.005 

    # Sort the DataFrame by the last year's share (last column) in descending order
    regions_share_per_year.sort_values(by=regions_share_per_year.columns[-1], ascending=False, inplace=True)

    # Write the DataFrame to a CSV file
    regions_share_per_year.to_csv(filename_start + '_Regions_Share_Per_Year.csv', index=False)

    return data
    
urls = [
    {"url": "https://pxweb.irena.org:443/sq/5f992bbc-6a3e-40ff-a803-50e3ee112875", "start filename": "data_IRENA/IRENA_Total_Renewable_Capacity"},
    {"url": "https://pxweb.irena.org:443/sq/d22cf986-2bcd-403f-ae41-982e80ff5234", "start filename": "data_IRENA/IRENA_Solar_Capacity"},
    {"url": "https://pxweb.irena.org:443/sq/9cd4cf9b-a150-4482-a122-9d0262884580", "start filename": "data_IRENA/IRENA_Solar_Photovoltaic_Capacity"},
    {"url": "https://pxweb.irena.org:443/sq/cb0d1458-fe7f-4acd-8a30-ead7056fa7b6", "start filename": "data_IRENA/IRENA_Solar_Concentrated_Power_Capacity"},    
    {"url": "https://pxweb.irena.org:443/sq/98abee2b-b13e-493c-9936-8fa7fb3bcd75", "start filename": "data_IRENA/IRENA_Wind_Capacity"},
    {"url": "https://pxweb.irena.org:443/sq/f1d4b002-b9c2-4f21-8a4c-9c162c6dbebe", "start filename": "data_IRENA/IRENA_Wind_Onshore_Capacity"},
    {"url": "https://pxweb.irena.org:443/sq/3a2a0d76-9ac0-4f12-b3ca-a0dc57817fe2", "start filename": "data_IRENA/IRENA_Wind_Offshore_Capacity"},         
    {"url": "https://pxweb.irena.org:443/sq/1eadead1-760e-48f1-a0b1-20da7ce57bc9", "start filename": "data_IRENA/IRENA_Hydropower_Capacity"},
    {"url": "https://pxweb.irena.org:443/sq/27476093-9aa2-4153-9f2d-29284f5b8606", "start filename": "data_IRENA/IRENA_Hydropower_Renewable_Hydro_Capacity"},        
    {"url": "https://pxweb.irena.org:443/sq/453382db-3986-4c4c-9914-449d0f0f880a", "start filename": "data_IRENA/IRENA_Pumped_Storage_Capacity"},    
    {"url": "https://pxweb.irena.org:443/sq/237533e3-e447-4d8d-9ff9-f7e66a0685d7", "start filename": "data_IRENA/IRENA_Bioenergy_Capacity"},
    {"url": "https://pxweb.irena.org:443/sq/09cb12b8-74d1-43c3-8c76-ff01453ab62f", "start filename": "data_IRENA/IRENA_Bioenergy_Biogas_Capacity"},
    {"url": "https://pxweb.irena.org:443/sq/5d09f55f-e1bc-432b-9498-e587b21980df", "start filename": "data_IRENA/IRENA_Bioenergy_Liquid_Biofuels_Capacity"},
    {"url": "https://pxweb.irena.org:443/sq/64cb91c7-4fa0-4b79-bee7-7b801b69b400", "start filename": "data_IRENA/IRENA_Bioenergy_Renewable_Municipal_Waste_Capacity"},
    {"url": "https://pxweb.irena.org:443/sq/c5c6492c-9167-4132-87e4-a549115a8812", "start filename": "data_IRENA/IRENA_Bioenergy_Solid_Biofuels_Capacity"},
    {"url": "https://pxweb.irena.org:443/sq/8c0f0fc6-6df5-4654-8427-4501ec19185e", "start filename": "data_IRENA/IRENA_Marine_Energy_Capacity"},
    {"url": "https://pxweb.irena.org:443/sq/6e165525-65c2-4a9f-9973-1d9cba36d89f", "start filename": "data_IRENA/IRENA_Geothermal_Capacity"}  
]

for url_info in urls:
    data = process_data(url_info['url'], url_info['start filename'])
    if data is None:
        continue
    time.sleep(1)


#IRENA Renewable Energy Capacity Data per Technology
def process_data(url, year_range):
    response = requests.get(url)
    if response.status_code == 200:
        data = pd.read_csv(io.StringIO(response.text), quotechar='"')

        # Convert years columns to numeric
        for year in year_range:
            data[str(year)] = pd.to_numeric(data[str(year)], errors='coerce')

        # Calculate net additions
        data_net_additions = data.copy()
        for year in year_range[1:]:
            data_net_additions[str(year)] = data[str(year)] - data[str(year - 1)]
        
        # Select technologies of interest
        technologies = ['Renewable hydropower', 'Wind', 'Solar', 'Geothermal', 'Bioenergy', 'Marine']
        data_per_technology = data[data['Technology'].isin(technologies)].copy()
        data_net_additions_per_technology = data_net_additions[data_net_additions['Technology'].isin(technologies)].copy()

        # Drop unwanted columns and save
        data_per_technology.drop('Region/country/area', axis=1, inplace=True)
        data_per_technology.sort_values(by=str(year_range[-1]), ascending=False).to_csv('data_IRENA/IRENA_Total_Renewable_Cumulative_Per_Technology.csv', index=False)
        data_net_additions_per_technology.sort_values(by=str(year_range[-1]), ascending=False).to_csv('data_IRENA/IRENA_Total_Renewable_Net_Additions_Per_Technology.csv', index=False)

        # Calculate share of each technology
        year_range_str = [str(year) for year in year_range]  # convert years to strings
        total_per_year = data_per_technology[year_range_str].sum()
        technology_share_per_year = data_per_technology[year_range_str].divide(total_per_year, axis=1)

        # Multiply by 100 to get percentages
        technology_share_per_year = technology_share_per_year * 100

        # Insert 'Technology' column to technology_share_per_year dataframe
        technology_share_per_year.insert(0, 'Technology', data_per_technology['Technology'])

        # Sort the dataframe based on the latest year
        latest_year = str(year_range[-1])
        technology_share_per_year = technology_share_per_year.sort_values(by=[latest_year], ascending=False)

        # Save the share data to CSV
        technology_share_per_year.to_csv('data_IRENA/IRENA_Total_Renewable_Cumulative_Per_Technology_Share.csv', index=False)

        # Process total renewable data
        total_renewable = data[data['Technology'] == 'Total renewable energy']
        total_renewable_net_additions = data_net_additions[data_net_additions['Technology'] == 'Total renewable energy']

        # Sort and Save the total renewable data to CSV files
        total_renewable.sort_values(by=str(year_range[-1]), ascending=False).to_csv('data_IRENA/IRENA_Total_Renewable_Cumulative_World.csv', index=False)
        total_renewable_net_additions.sort_values(by=str(year_range[-1]), ascending=False).to_csv('data_IRENA/IRENA_Total_Renewable_Net_Additions_World.csv', index=False)

        #Wind sub-technologies
        wind_sub_technologies = ['Onshore wind energy', 'Offshore wind energy']
        data_wind_sub_tech = data[data['Technology'].isin(wind_sub_technologies)].copy()
        data_wind_sub_tech.drop('Region/country/area', axis=1, inplace=True)
        data_wind_sub_tech.sort_values(by=str(year_range[-1]), ascending=False).to_csv('data_IRENA/IRENA_Wind_Sub_Technology_Cumulative_Capacity.csv', index=False)
        data_net_additions_wind_sub_tech = data_net_additions[data_net_additions['Technology'].isin(wind_sub_technologies)].copy()
        data_net_additions_wind_sub_tech.drop('Region/country/area', axis=1, inplace=True)
        data_net_additions_wind_sub_tech.sort_values(by=str(year_range[-1]), ascending=False).to_csv('data_IRENA/IRENA_Wind_Sub_Technology_Net_Additional_Capacity.csv', index=False)

        # Solar sub-technologies
        solar_sub_technologies = ['Solar photovoltaic', 'Concentrated solar power']
        data_solar_sub_tech = data[data['Technology'].isin(solar_sub_technologies)].copy()
        data_solar_sub_tech.drop('Region/country/area', axis=1, inplace=True)
        data_solar_sub_tech.sort_values(by=str(year_range[-1]), ascending=False).to_csv('data_IRENA/IRENA_Solar_Sub_Technology_Cumulative_Capacity.csv', index=False)
        data_net_additions_solar_sub_tech = data_net_additions[data_net_additions['Technology'].isin(solar_sub_technologies)].copy()
        data_net_additions_solar_sub_tech.drop('Region/country/area', axis=1, inplace=True)
        data_net_additions_solar_sub_tech.sort_values(by=str(year_range[-1]), ascending=False).to_csv('data_IRENA/IRENA_Solar_Sub_Technology_Net_Additional_Capacity.csv', index=False)

# Call the function with your URL and year range
url = "https://pxweb.irena.org:443/sq/704ded05-efd7-40cf-9aa0-317172b23265"
process_data(url, range(2000, 2023))