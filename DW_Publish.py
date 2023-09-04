import requests
import json
import os

# Recursive function to fetch chart IDs from a folder (including its subfolders)
def fetch_all_chart_ids(folder_id, access_token):
    url = f"https://api.datawrapper.de/v3/folders/{folder_id}"
    headers = {
        "Authorization": "Bearer " + access_token,
        "Accept": "*/*"
    }
    response = requests.get(url, headers=headers)
    json_object = json.loads(response.text)
    
    # Fetch chart IDs directly in the current folder
    chart_ids = [chart["id"] for chart in json_object.get("charts", [])]
    
    # Recursively fetch chart IDs from child folders
    for child in json_object.get("children", []):
        chart_ids.extend(fetch_all_chart_ids(child["id"], access_token))
        
    return chart_ids

# Function to publish charts
def publish_charts(chart_ids, access_token):
    for chart_id in chart_ids:
        url = f"https://api.datawrapper.de/v3/charts/{chart_id}/publish/"
        headers = {
            "Authorization": "Bearer " + access_token,
            "Accept": "*/*"
        }
        response = requests.post(url, headers=headers)

# Main execution
def main():
    access_token = os.getenv('DW_TOKEN')
    
    # List of folder IDs
    folder_ids = [162886]
    
    # Iterating over folder IDs
    for folder_id in folder_ids:
        all_chart_ids = fetch_all_chart_ids(folder_id, access_token)
        publish_charts(all_chart_ids, access_token)
if __name__ == "__main__":
    main()