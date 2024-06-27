import aiohttp
import asyncio
import logging
import json
from tenacity import retry, stop_after_attempt, wait_fixed
import re
import nest_asyncio
from bs4 import BeautifulSoup

nest_asyncio.apply()
logging.basicConfig(level=logging.INFO)

# Define the file path
SERVERS_FILE_PATH = 'servers.txt'
OUTPUT_FILE_PATH = 'all_server_responses.json'

def normalize_url(url):
    # Remove duplicate slashes but keep the "http://" or "https://"
    return re.sub(r'(?<!:)/{2,}', '/', url)

async def fetch(session, url):
    logging.info(f"Fetching URL: {url}")
    async with session.get(url) as response:
        if response.status != 200:
            logging.error(f"Failed to fetch {url}: {response.status}")
            raise aiohttp.ClientResponseError(
                status=response.status,
                message=f"Unexpected content type {response.content_type} at {url}",
                headers=response.headers,
                history=response.history
            )
        data = await response.json()
        logging.info(f"Data fetched from {url}: {data}")
        return data

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def get_folders_and_services(session, url):
    return await fetch(session, f"{url}?f=json")

async def get_layer_metadata(session, layer_url):
    url = normalize_url(f"{layer_url}?f=json")
    layer_metadata = await fetch(session, url)
    
    fields = layer_metadata.get('fields', [])
    description_html = layer_metadata.get('description', 'No description available')
    description = BeautifulSoup(description_html, 'html.parser').get_text()
    geometry_type = layer_metadata.get('geometryType', 'Unknown')
    layer_name = layer_metadata.get('name', 'No name available')
    
    return {
        'layer_name': layer_name,
        'fields': [field['name'] for field in fields],
        'description': description,
        'geometry_type': geometry_type,
        'url': layer_url
    }

async def get_service_details(session, base_url, service):
    service_name = service['name']
    service_type = service['type']
    url = normalize_url(f"{base_url}/{service_name}/{service_type}?f=json")
    service_metadata = await fetch(session, url)

    tasks = []
    for layer in service_metadata.get('layers', []):
        layer_url = normalize_url(f"{base_url}/{service_name}/{service_type}/{layer['id']}")
        tasks.append(get_layer_metadata(session, layer_url))
    
    details = await asyncio.gather(*tasks)
    return details

async def process_server(session, url, results):
    try:
        folders_and_services = await get_folders_and_services(session, normalize_url(url))
        logging.info(f"Fetched data: {folders_and_services}")

        if not folders_and_services:
            raise ValueError("No metadata fetched")
        
        services = folders_and_services.get('services', [])
        tasks = []
        for service in services:
            if service['type'] == 'FeatureServer':
                tasks.append(get_service_details(session, url, service))
        
        service_details_list = await asyncio.gather(*tasks)
        for service_details in service_details_list:
            results.extend(service_details)

    except Exception as e:
        logging.error(f"Error processing server {url}: {e}")

async def main():
    async with aiohttp.ClientSession() as session:
        with open(SERVERS_FILE_PATH, 'r') as file:
            servers = [normalize_url(line.strip()) for line in file if line.strip()]
        
        results = []
        tasks = [process_server(session, server, results) for server in servers]
        await asyncio.gather(*tasks)

        # Save the results list to a single JSON file
        with open(OUTPUT_FILE_PATH, 'w') as f:
            json.dump(results, f, indent=4)
        logging.info(f"Saved all responses to: {OUTPUT_FILE_PATH}")

if __name__ == '__main__':
    asyncio.run(main())
