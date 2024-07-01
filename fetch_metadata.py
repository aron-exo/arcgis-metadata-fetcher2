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

# Define the file paths
SERVERS_FILE_PATH = 'servers.txt'
OUTPUT_FILE_PATH = 'all_server_responses.json'

ALLOWED_GEOMETRY_TYPES = [
    'esriGeometryPolyline',
    'esriGeometryPoint',
    'esriGeometryMultipoint'
]

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
        logging.info(f"Data fetched from {url}")
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
    
    if geometry_type not in ALLOWED_GEOMETRY_TYPES:
        return None

    return {
        'layer_name': layer_name,
        'fields': [field['name'] for field in fields] if fields else [],
        'description': description,
        'geometry_type': geometry_type,
        'url': layer_url
    }

async def get_service_details(session, base_url, service):
    service_name = service['name']
    service_type = service['type']
    if service_type not in ["FeatureServer", "MapServer"]:
        return None
    
    service_url = normalize_url(f"{base_url}/{service_name}/{service_type}?f=json")
    service_metadata = await fetch(session, service_url)

    tasks = []
    for layer in service_metadata.get('layers', []):
        layer_url = normalize_url(f"{base_url}/{service_name}/{service_type}/{layer['id']}")
        tasks.append(get_layer_metadata(session, layer_url))
    
    details = await asyncio.gather(*tasks)
    details = [detail for detail in details if detail]  # Remove None values
    return {
        'service_name': service_name,
        'service_type': service_type,
        'layers': details
    }

async def process_folder(session, base_url, folder_path):
    folder_url = normalize_url(f"{base_url}/{folder_path}")
    folders_and_services = await get_folders_and_services(session, folder_url)
    
    results = {
        'folder_name': folder_path.split('/')[-1],  # Get the last part of the path as the folder name
        'services': [],
        'subfolders': []
    }
    
    # Process services
    services = folders_and_services.get('services', [])
    for service in services:
        service_details = await get_service_details(session, base_url, service)
        if service_details:
            results['services'].append(service_details)
    
    # Process subfolders
    subfolders = folders_and_services.get('folders', [])
    for subfolder in subfolders:
        subfolder_path = f"{folder_path}/{subfolder}"
        subfolder_details = await process_folder(session, base_url, subfolder_path)
        results['subfolders'].append(subfolder_details)
    
    return results

async def process_server(session, base_url):
    results = {
        'services': [],
        'folders': []
    }
    
    folders_and_services = await get_folders_and_services(session, base_url)
    
    # Process root services
    services = folders_and_services.get('services', [])
    for service in services:
        service_details = await get_service_details(session, base_url, service)
        if service_details:
            results['services'].append(service_details)
    
    # Process folders
    folders = folders_and_services.get('folders', [])
    tasks = [process_folder(session, base_url, folder) for folder in folders]
    folder_details = await asyncio.gather(*tasks)
    results['folders'].extend(folder_details)
    
    return results

async def main():
    async with aiohttp.ClientSession() as session:
        with open(SERVERS_FILE_PATH, 'r') as file:
            servers = [normalize_url(line.strip()) for line in file if line.strip()]
        
        all_results = {}
        for server in servers:
            try:
                server_results = await process_server(session, server)
                all_results[server] = server_results
            except Exception as e:
                logging.error(f"Error processing server {server}: {e}")

        # Save the results to a JSON file
        with open(OUTPUT_FILE_PATH, 'w') as f:
            json.dump(all_results, f, indent=4)
        logging.info(f"Saved all responses to: {OUTPUT_FILE_PATH}")

if __name__ == '__main__':
    asyncio.run(main())
