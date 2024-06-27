import asyncio
import aiohttp
import json
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from urllib.parse import urljoin

# Load unique servers from file
with open('servers_small.txt', 'r') as f:
    unique_servers = [line.strip() for line in f.readlines()]

# Function to get the list of folders and services
async def get_folders_and_services(session, base_url):
    url = urljoin(base_url, '?f=json')
    async with session.get(url) as response:
        if response.status != 200:
            print(f"Error fetching folders and services from {base_url}: {response.status}")
            return [], []
        data = await response.json()
        folders = data.get('folders', [])
        services = data.get('services', [])
        return folders, services

# Function to get metadata from a layer within a service
async def get_layer_metadata(session, layer_url):
    url = urljoin(layer_url, '?f=json')
    async with session.get(url) as response:
        if response.status != 200:
            print(f"Error fetching layer metadata from {layer_url}: {response.status}")
            return {
                'layer_name': layer_url.split('/')[-1],
                'fields': [],
                'description': 'No description available',
                'geometry_type': 'Unknown',
                'url': layer_url
            }
        metadata = await response.json()
        fields = metadata.get('fields', [])
        description = metadata.get('description', 'No description available')
        geometry_type = metadata.get('geometryType', 'Unknown')
        layer_name = metadata.get('name', 'No name available')

        return {
            'layer_name': layer_name,
            'fields': [field['name'] for field in fields],
            'description': description,
            'geometry_type': geometry_type,
            'url': layer_url
        }

# Function to get layers from a service
async def get_service_layers(session, service_url):
    url = urljoin(service_url, '?f=json')
    async with session.get(url) as response:
        if response.status != 200:
            print(f"Error fetching service layers from {service_url}: {response.status}")
            return []
        layers_json = await response.json()
        layers = layers_json.get('layers', [])
        tables = layers_json.get('tables', [])
        return layers + tables

# Recursive function to fetch all layers and tables
async def fetch_all_layers(session, service_url, layers):
    all_layers = []
    for layer in layers:
        layer_id = layer['id']
        full_layer_url = f"{service_url}/{layer_id}"
        all_layers.append(full_layer_url)
    return all_layers

# Function to download metadata for a single service
async def download_service_metadata(session, server, service):
    service_metadata = []
    service_name = service['name']
    service_type = service['type']
    service_url = urljoin(server, f"{service_name}/{service_type}")
    layers = await get_service_layers(session, service_url)
    all_layers = await fetch_all_layers(session, service_url, layers)
    for layer_url in all_layers:
        try:
            metadata = await get_layer_metadata(session, layer_url)
            service_metadata.append(metadata)
        except Exception as e:
            print(f"Error fetching metadata for layer {layer_url}: {e}")
            service_metadata.append({
                'layer_name': layer_url.split('/')[-1],
                'fields': [],
                'description': 'No description available',
                'geometry_type': 'Unknown',
                'url': layer_url
            })
    return service_metadata

# Recursive function to process all folders and services
async def process_folder(session, base_url):
    all_services_metadata = []
    folders, services = await get_folders_and_services(session, base_url)
    # Process services in the current folder
    tasks = [download_service_metadata(session, base_url, service) for service in services]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            print(f"Error during service metadata download: {result}")
        else:
            all_services_metadata.extend(result)
    # Recursively process subfolders
    for folder in folders:
        folder_url = urljoin(base_url, folder)
        subfolder_metadata = await process_folder(session, folder_url)
        all_services_metadata.extend(subfolder_metadata)
    return all_services_metadata

# Function to handle the download for each server using asyncio
async def process_server(server):
    async with aiohttp.ClientSession() as session:
        try:
            return await process_folder(session, server)
        except Exception as e:
            print(f"Error processing server {server}: {e}")
            return []

# Wrapper function to run the asyncio event loop for each server
def process_server_wrapper(server):
    return asyncio.run(process_server(server))

# Function to download all services metadata using multiprocessing
def download_metadata(servers):
    with Pool(cpu_count()) as pool:
        results = list(tqdm(pool.imap(process_server_wrapper, servers), total=len(servers), desc="Servers"))
        services_metadata = [item for sublist in results for item in sublist]  # Flatten the list
    return services_metadata

# Download and save the metadata
services_metadata = download_metadata(unique_servers)

# Save metadata to a file
with open('services_metadata.json', 'w') as f:
    json.dump(services_metadata, f)

print("Completed fetching all metadata.")
