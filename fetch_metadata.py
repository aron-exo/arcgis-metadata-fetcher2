import os
import aiohttp
import asyncio
import json
from tqdm import tqdm

async def fetch_metadata(session, url):
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
    if os.path.exists('servers.txt'):
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
