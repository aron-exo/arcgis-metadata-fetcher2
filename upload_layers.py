import json
import re
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from arcgis.gis import GIS
from arcgis.geometry import Polygon
from arcgis.features import FeatureLayer
from arcgis.mapping import WebMap
from arcgis.geometry.filters import intersects
import os
import osmnx as ox
import geopandas as gpd


# Example list of utility-related keywords
utility_keywords = [
    "diameter", "sewage", "sewer", "gas", "natural gas",
    "electric", "electricity", "power", "sanitation",
    "wastewater", "drainage", "fuel", "pipeline",
    "grid", "distribution", "transmission",
    "telecom", "telecommunications", "fiber", "internet",
    "broadband", "storm", "storm water", "waste water", "storm drain",
    "stormdrain", "drain", "pipes", "storm sewer",
    "catch basin", "manhole", "culvert", "outfall", "Hydrant",
    "Valve",
    "Booster", "Tank", "pipe",
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
def search_metadata(service, keywords, geometry_types, extent_polygon):
    if isinstance(service, dict) and (
        contains_keywords(service.get('layer_name', ''), keywords) or
        contains_keywords(service.get('description', ''), keywords) or
        any(contains_keywords(field, keywords) for field in service.get('fields', []))
    ) and service.get('geometry_type') in geometry_types:
        return service
    return None

# Load metadata from file line by line
services_metadata = []
with open("services_metadata.json", 'r') as f:
    for line in f:
        line = line.strip()
        if line:
            try:
                services_metadata.append(json.loads(line))
            except json.JSONDecodeError:
                pass  # Skip lines that cannot be parsed

# Define the place of interest
county_name = "Los Angeles County, California, USA"

# Get the boundary of Los Angeles County
county_gdf = ox.geocode_to_gdf(county_name)
county_polygon = county_gdf.loc[0, 'geometry']
extent_polygon = Polygon(county_polygon.__geo_interface__)

# Use tqdm to display progress when loading metadata
print("Loading metadata...")
services_metadata = tqdm(services_metadata, desc="Loading metadata")

# Search the downloaded metadata for utility-related keywords and filter by geometry type and extent
matching_services = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(search_metadata, service, utility_keywords, desired_geometry_types, extent_polygon) for service in services_metadata]
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

# Display the matching services
print("Matching Services:")
for service in layers_for_webmap:
    print(service)

# Transform the list layers_for_webmap into a FeatureLayer list
print("Transforming to FeatureLayer list...")
feature_layers = []
for layer in tqdm(layers_for_webmap, desc="Creating FeatureLayer objects"):
    feature_layer = FeatureLayer(layer['url'])
    feature_layers.append(feature_layer)

# Now `feature_layers` contains the FeatureLayer objects
print("Feature Layers:")
for layer in feature_layers:
    print(layer)

# Replace the placeholders with your own variables
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')

# Initialize the GIS
gis = GIS("https://www.arcgis.com", username, password)

# Create a new WebMap
webmap_obj = WebMap()

# Query and display features within the polygon
for layer in tqdm(feature_layers, desc="Querying and adding layers"):
    try:
        result = layer.query(geometry_filter=intersects(extent_polygon))
        if result.features:
            webmap_obj.add_layer(layer)
        else:
            print(f"No features found in layer {layer.properties['name']}")
    except Exception as e:
        print(f"Error querying layer {layer.properties['name']}: {e}")

# Save the WebMap
webmap_item = webmap_obj.save({'title': 'Utility Layers WebMap', 'tags': 'utility, layers', 'snippet': 'WebMap containing utility layers'})
print(f"WebMap created with ID: {webmap_item.id}")
