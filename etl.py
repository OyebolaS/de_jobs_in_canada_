import requests
from bs4 import BeautifulSoup
import json

# Specify the API endpoint URL
url = "https://jsearch.p.rapidapi.com/search"

headers = {
	"X-RapidAPI-Key": "3ae9294cb9mshdf3f54aea99687cp1ed5d7jsnb9c17121fbd8",
	"X-RapidAPI-Host": "jsearch.p.rapidapi.com"
    }

querystring = {"query":"Data Engineer in Canada","page":"1","num_pages":"1","date_posted":"today","country":"ca"}

def extract_data():
    try:
        response = requests.get(url, headers=headers, params=querystring)

        # to Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the JSON data from the response
            data = response.json()

            # Print or process the extracted data
            print(json.dumps(data, indent=2))

            # to save the data to a json file
            with open('dataengineering_jobs.json', 'w') as json_file:
                json.dump(data, json_file, indent=2)

        else:
            # Print an error message if the request was not successful
            print(f"Error: {response.status_code}, {response.text}")

    except Exception as e:
        # Handle any exceptions that may occur during the request
        print(f"An error occurred: {e}")

# Call the function to extract data
extract_data()

# To transform the raw json file

