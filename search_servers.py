import os
import aiohttp
import asyncio
import json
from tqdm import tqdm

async def fetch_metadata(server):
    async with aiohttp.ClientSession() as client_session:
        url = f"{server}?f=json"
        async with client_session.get(url) as response:
            if response.status == 200:
                try:
                    data = await response.json()
                    return data
                except aiohttp.ContentTypeError:
                    print(f"Invalid content type at {server}")
                    return None
            else:
                print(f"Failed to fetch data from {server}")
                return None

async def process_server(server):
    metadata_file = f"metadata_{server.replace('/', '_')}.json"
    if os.path.exists(metadata_file):
        print(f"Metadata for {server} already exists. Skipping...")
        return

    metadata = await fetch_metadata(server)
    if metadata:
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=4)

async def main():
    if os.path.exists('servers.txt'):
        with open('servers.txt', 'r') as f:
            servers = [line.strip() for line in f.readlines()]
        for server in tqdm(servers):
            await process_server(server)

if __name__ == "__main__":
    asyncio.run(main())
