import os
import aiohttp
import asyncio
import json
from tqdm import tqdm

# Load processed servers from file
processed_servers_file = 'processed_servers.txt'
if os.path.exists(processed_servers_file):
    with open(processed_servers_file, 'r') as f:
        processed_servers = set(line.strip() for line in f.readlines())
else:
    processed_servers = set()

async def fetch_metadata(session, url):
    """
    Fetch metadata from a given URL.
    
    Args:
        session (aiohttp.ClientSession): The client session to use for the request.
        url (str): The URL to fetch metadata from.
    
    Returns:
        dict or None: The fetched metadata as a dictionary, or None if an error occurred.
    """
    async with session.get(url) as response:
        if response.status == 200:
            try:
                data = await response.json()
                return data
            except aiohttp.ContentTypeError:
                print(f"Invalid content type at {url}")
                return None
        else:
            print(f"Failed to fetch data from {url}")
            return None

async def get_layers_metadata(session, service_url):
    """
    Get metadata for all layers within a service.
    
    Args:
        session (aiohttp.ClientSession): The client session to use for the request.
        service_url (str): The URL of the service to fetch layers metadata from.
    
    Returns:
        list: A list of dictionaries containing metadata for each layer.
    """
    layers_metadata = []
    service_metadata = await fetch_metadata(session, f"{service_url}?f=json")
    if not service_metadata:
        return layers_metadata

    for layer in tqdm(service_metadata.get('layers', []) + service_metadata.get('tables', []), desc=f"Fetching layers from {service_url}"):
        layer_url = f"{service_url}/{layer['id']}?f=json"
        layer_metadata = await fetch_metadata(session, layer_url)
        if layer_metadata:
            layers_metadata.append({
                'layer_name': layer_metadata.get('name', 'No name available'),
                'fields': [field['name'] for field in layer_metadata.get('fields', [])],
                'description': layer_metadata.get('description', 'No description available'),
                'geometry_type': layer_metadata.get('geometryType', 'Unknown'),
                'url': layer_url
            })
    return layers_metadata

async def process_server(session, server):
    """
    Process a single server to fetch metadata for all its services and layers.
    
    Args:
        session (aiohttp.ClientSession): The client session to use for the request.
        server (str): The base URL of the server to process.
    
    Returns:
        list: A list of dictionaries containing metadata for all services and layers.
    """
    all_metadata = []
    server_metadata = await fetch_metadata(session, f"{server}?f=json")
    if not server_metadata:
        return all_metadata

    services = server_metadata.get('services', [])
    for service in tqdm(services, desc=f"Processing services for {server}"):
        service_url = f"{server}{service['name']}/{service['type']}"
        layers_metadata = await get_layers_metadata(session, service_url)
        all_metadata.extend(layers_metadata)

    return all_metadata

async def main():
    """
    Main function to orchestrate fetching and saving metadata for multiple servers.
    """
    if os.path.exists('servers.txt'):
        with open('servers.txt', 'r') as f:
            servers = [line.strip() for line in f.readlines()]

        all_metadata = []
        async with aiohttp.ClientSession() as session:
            for server in tqdm(servers, desc="Servers"):
                if server in processed_servers:
                    print(f"Skipping already processed server: {server}")
                    continue
                
                server_metadata = await process_server(session, server)
                all_metadata.extend(server_metadata)
                
                # Save metadata after processing each server
                with open('services_metadata.json', 'w') as f:
                    json.dump(all_metadata, f, indent=4)

                # Mark server as processed
                processed_servers.add(server)
                with open(processed_servers_file, 'a') as f:
                    f.write(server + '\n')

if __name__ == "__main__":
    asyncio.run(main())
