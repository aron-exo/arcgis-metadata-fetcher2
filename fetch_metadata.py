import os
import aiohttp
import asyncio
import json
from tqdm import tqdm
from urllib.parse import urljoin

async def fetch_metadata(session, url):
    """
    Fetch metadata from a given URL.

    Args:
        session (aiohttp.ClientSession): The session to use for the request.
        url (str): The URL to fetch metadata from.

    Returns:
        dict: The metadata if fetched successfully, otherwise None.
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
            print(f"Failed to fetch data from {url}, status: {response.status}")
            return None

async def get_layer_metadata(session, layer_url):
    """
    Get metadata for a specific layer.

    Args:
        session (aiohttp.ClientSession): The session to use for the request.
        layer_url (str): The URL of the layer.

    Returns:
        dict: A dictionary containing layer metadata.
    """
    layer_metadata = await fetch_metadata(session, f"{layer_url}?f=json")
    if layer_metadata:
        return {
            'layer_name': layer_metadata.get('name', 'No name available'),
            'fields': [field['name'] for field in layer_metadata.get('fields', [])],
            'description': layer_metadata.get('description', 'No description available'),
            'geometry_type': layer_metadata.get('geometryType', 'Unknown'),
            'url': layer_url
        }
    else:
        return {
            'layer_name': layer_url.split('/')[-1],
            'fields': [],
            'description': 'No description available',
            'geometry_type': 'Unknown',
            'url': layer_url
        }

async def get_service_layers(session, service_url):
    """
    Get all layers from a service.

    Args:
        session (aiohttp.ClientSession): The session to use for the request.
        service_url (str): The URL of the service.

    Returns:
        list: A list of layer URLs.
    """
    service_metadata = await fetch_metadata(session, f"{service_url}?f=json")
    if service_metadata:
        layers = service_metadata.get('layers', []) + service_metadata.get('tables', [])
        return [f"{service_url}/{layer['id']}" for layer in layers]
    return []

async def process_service(session, server, service):
    """
    Process a single service to fetch all layers' metadata.

    Args:
        session (aiohttp.ClientSession): The session to use for the request.
        server (str): The server URL.
        service (dict): The service information.

    Returns:
        list: A list of metadata dictionaries for each layer.
    """
    service_metadata = []
    service_url = urljoin(server, f"services/{service['name']}/{service['type']}")
    print(f"Fetching service layers from: {service_url}")
    layers = await get_service_layers(session, service_url)
    if not layers:
        print(f"No layers found in service: {service_url}")
    for layer_url in tqdm(layers, desc=f"Fetching layers from {service_url}"):
        metadata = await get_layer_metadata(session, layer_url)
        service_metadata.append(metadata)
    return service_metadata

async def process_server(session, server):
    """
    Process a server to fetch and save its metadata.

    Args:
        session (aiohttp.ClientSession): The session to use for the request.
        server (str): The server URL.

    Returns:
        list: A list of all metadata fetched from the server.
    """
    all_metadata = []
    server_metadata = await fetch_metadata(session, f"{server}?f=json")
    if not server_metadata:
        return all_metadata

    services = server_metadata.get('services', [])
    for service in tqdm(services, desc=f"Processing services for {server}"):
        service_metadata = await process_service(session, server, service)
        all_metadata.extend(service_metadata)

    return all_metadata

async def main():
    """
    Main function to process all servers listed in 'servers.txt'.
    """
    if os.path.exists('servers_small.txt'):
        with open('servers.txt', 'r') as f:
            servers = [line.strip() for line in f.readlines()]

        all_metadata = []
        async with aiohttp.ClientSession() as session:
            for server in tqdm(servers, desc="Servers"):
                server_metadata = await process_server(session, server)
                all_metadata.extend(server_metadata)

        with open('services_metadata.json', 'w') as f:
            json.dump(all_metadata, f, indent=4)

if __name__ == "__main__":
    asyncio.run(main())
