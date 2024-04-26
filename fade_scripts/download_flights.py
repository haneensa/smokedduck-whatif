import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import os
import zipfile

# URL of the webpage to scrape
url = 'https://www.bts.gov/browse-statistical-products-and-data/bts-publications/airline-service-quality-performance-234-time'

# Send a GET request to the URL
response = requests.get(url)

# Parse the HTML content of the webpage
soup = BeautifulSoup(response.text, 'html.parser')

# Find all anchor tags (links) in the webpage
links = soup.find_all('a')

# Extract the href attribute (link) from each anchor tag
for link in links:
    href = link.get('href')
    if href:
        # Parse the URL to extract the file extension
        parsed_url = urlparse(href)
        file_extension = parsed_url.path.split('.')[-1]

        # Check if the file extension is 'zip'
        if file_extension == 'zip':
            print(href)
            # Construct the absolute URL if the link is relative
            absolute_url = urljoin(url, href)
            print("Downloading:", absolute_url)

            try:
                # Download the file
                response = requests.get(absolute_url)
                with open(f"fade_data/flights/file_{link.text}.zip", 'wb') as f:
                    f.write(response.content)
            except Exception as e:
                print(f"Error downloading link {href}: {e}")

import os
import zipfile

def unzip_all_files(source_folder, destination_folder):
    # Create destination folder if it doesn't exist
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    # Iterate over files in source folder
    for root, dirs, files in os.walk(source_folder):
        for file in files:
            # Check if the file is a zip file
            if file.endswith('.zip'):
                zip_file_path = os.path.join(root, file)
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    zip_ref.extractall(destination_folder)

# Example usage:
source_folder = "fade_data/flights"
destination_folder = "fade_data/flights_unzip"
unzip_all_files(source_folder, destination_folder)
