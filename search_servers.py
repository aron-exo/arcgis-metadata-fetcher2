import osmnx as ox
import geopandas as gpd
from arcgis.gis import GIS
from arcgis.geocoding import reverse_geocode
from urllib.parse import urlparse, urlunparse
import os

# Initialize the GIS
gis = GIS("https://www.arcgis.com", os.getenv('USERNAME'), os.getenv('PASSWORD'))

# Define the place of interest
county_name = "Los Angeles County, California, USA"

# Get the boundary of Los Angeles County
county_gdf = ox.geocode_to_gdf(county_name)
county_polygon = county_gdf.loc[0, 'geometry']

# Download place boundaries within Los Angeles County including cities, towns, villages, and hamlets
tags = {'place': ['city', 'town']}
places_gdf = ox.features_from_polygon(county_polygon, tags=tags)

# Ensure only valid strings are considered for place names and filter by geometry within the county polygon
places_gdf = places_gdf[places_gdf.geometry.within(county_polygon)]
place_names_script = set([name for name in places_gdf['name'] if isinstance(name, str)])

def get_root_url(service_url):
    """Extracts the root URL from a given service URL."""
    parsed_url = urlparse(service_url)
    path_segments = parsed_url.path.split('/')
    try:
        rest_index = path_segments.index('rest')
        services_index = path_segments.index('services', rest_index)
        new_path = '/' + '/'.join(path_segments[:services_index + 1]) + '/'
        new_parsed_url = parsed_url._replace(path=new_path)
        return urlunparse(new_parsed_url)
    except (ValueError, IndexError):
        return urlunparse(parsed_url._replace(path=''))

def search_for_servers(search_terms):
    """Searches for servers based on a list of search terms."""
    servers = set()
    for term in search_terms:
        print(f"Searching for: {term}")
        try:
            search_results = gis.content.advanced_search(query=term + " Los Angeles County", max_items=15)
            for item in search_results['results']:
                try:
                    url = item.url
                    if url:
                        root_url = get_root_url(url)
                        servers.add(root_url)
                except AttributeError:
                    continue  # Skip items that might not have a URL
        except Exception as e:
            print(f"Error searching for {term}: {str(e)}")
    return servers

# Perform individual searches for each place name
unique_servers = search_for_servers(place_names_script)

# Additional searches for broader terms
additional_search_terms = [
    "Los Angeles County, California, USA",
    "California, USA",
    "usa gas pipeline",
    "usa electricity transmission lines"
]

unique_servers.update(search_for_servers(additional_search_terms))

# Print or process the unique servers
print("Unique Root Servers Found:")
print(len(unique_servers))
for server in unique_servers:
    print(server)

# Save unique servers to a file
output_file = 'servers.txt'
with open(output_file, 'w') as f:
    for server in unique_servers:
        f.write(server + '\n')
