import os
import aiohttp
import asyncio
import json
from tqdm import tqdm

async def fetch_metadata(server):
    """Fetch metadata from a given server."""
    async with aiohttp.ClientSession() as client_session:
        url = f"{server}?f=json"
        async with client_session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data
            else:
                print(f"Failed to fetch data from {server}")
                return None

async def process_server(server):
    """Process a server to fetch and save its metadata."""
    metadata = await fetch_metadata(server)
    if metadata:
        with open('services_metadata.json', 'a') as f:
            f.write(json.dumps(metadata) + '\n')

    with open('processed_servers.txt', 'a') as f:
        f.write(server + '\n')

async def main():
    """Main function to process all servers listed in 'servers.txt'."""
    if os.path.exists('servers.txt'):
        with open('servers.txt', 'r') as f:
            servers = [line.strip() for line in f.readlines()]

        processed_servers = set()
        if os.path.exists('processed_servers.txt'):
            with open('processed_servers.txt', 'r') as f:
                processed_servers = set(line.strip() for line in f.readlines())

        servers_to_process = [server for server in servers if server not in processed_servers]

        for server in tqdm(servers_to_process):
            await process_server(server)

if __name__ == "__main__":
    asyncio.run(main())
