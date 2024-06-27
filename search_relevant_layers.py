import json
import re
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import osmnx as ox

# Example list of utility-related keywords
utility_keywords = [
    "diameter", "sewage", "sewer", "gas", "natural gas",
    "electric", "electricity", "power", "sanitation",
    "wastewater", "drainage", "fuel", "pipeline",
    "grid", "distribution", "transmission",
    "telecom", "telecommunications", "fiber",
    "broadband", "storm", "storm water", "waste water", "storm drain",
    "stormdrain", "drain", "pipes", "storm sewer",
    "catch basin", "manhole", "culvert", "outfall", "Hydrant",
    "Valve", "Booster", "Tank", "pipe",
    "Reducer", "Cross Fittings", "Cleanout", "Pump", "Lampholes",
    "Manholes", "Force main", "Junction Box", "SepticTank",
    "Gravity Sewer", "Ejection Line", "Water Main"
]

# List of desired geometry types
desired_geometry_types = ['esriGeometryPolyline', 'esriGeometryPoint', 'esriGeometryMultipoint', 'esriGeometryLine']

# Function to search for keywords in text
def contains_keywords(text, keywords):
    if not text:
        return False
    text = text.lower()
    for keyword in keywords:
        pattern = rf"{re.escape(keyword.lower())}"
        if re.search(pattern, text, re.IGNORECASE):
            return True
    return False

# Function to search the metadata for keywords and filter by geometry type
def search_metadata(service, keywords, geometry_types):
    if isinstance(service, dict) and (
        contains_keywords(service.get('layer_name', ''), keywords) or
        contains_keywords(service.get('description', ''), keywords) or
        any(contains_keywords(field, keywords) for field in service.get('fields', []))
    ) and service.get('geometry_type') in geometry_types:
        return service
    return None

# Load metadata from file
with open("all_server_responses.json", 'r') as f:
    services_metadata = json.load(f)

# Define the place of interest
county_name = "Los Angeles County, California, USA"

# Get the boundary of Los Angeles County
county_gdf = ox.geocode_to_gdf(county_name)
county_polygon = county_gdf.loc[0, 'geometry']

# Use tqdm to display progress when loading metadata
print("Loading metadata...")

# Search the downloaded metadata for utility-related keywords and filter by geometry type and extent
matching_services = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(search_metadata, service, utility_keywords, desired_geometry_types) for service in services_metadata]
    for future in tqdm(as_completed(futures), total=len(futures), desc="Searching metadata"):
        result = future.result()
        if result:
            matching_services.append(result)

# Use tqdm to display progress when saving layers
print("Saving matching layers...")
layers_for_webmap = []
for service in tqdm(matching_services, desc="Saving layers"):
    layer_info = {
        'title': service['layer_name'],
        'url': service['url'],
        'type': 'FeatureLayer'
    }
    layers_for_webmap.append(layer_info)

# Save the list of added layers with URLs
with open('added_layers.json', 'w') as f:
    json.dump(layers_for_webmap, f, indent=4)

print("Added layers saved to added_layers.json")
