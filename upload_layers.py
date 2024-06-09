import json
import re
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from arcgis.features import FeatureLayer
from arcgis.geometry import Geometry, Polygon, project
from arcgis.gis import GIS
import os
import osmnx as ox

# Example list of utility-related keywords
utility_keywords = [
    "diameter", "sewage", "sewer", "gas", "natural gas","electric", "electricity", "power", "sanitation",
    "wastewater", "drainage", "fuel", "pipeline",
    "grid", "distribution", "transmission",
    "telecom", "telecommunications", "fiber", "internet",
    "broadband", "storm", "storm water", "waste water", "storm drain",
    "stormdrain", "drain", "pipes", "storm sewer",
    "catch basin", "manhole", "culvert", "outfall", "Hydrant",
    "Valve", "Booster", "Tank", "pipe", "Reducer", "Cross Fittings", "Cleanout", "Pump", "Lampholes",
    "Manholes", "Force main", "Junction Box", "SepticTank", "Gravity Sewer", "Ejection Line", "Water Main"
]

# List of desired geometry types
desired_geometry_types = ['esriGeometryPolyline', 'esriGeometryPoint', 'esriGeometryMultipoint', 'esriGeometryLine']

def contains_keywords(text, keywords):
    """
    Check if the given text contains any of the specified keywords.

    Args:
        text (str): The text to search.
        keywords (list): A list of keywords to search for.

    Returns:
        bool: True if any keyword is found in the text, False otherwise.
    """
    if not text:
        return False
    text = text.lower()
    for keyword in keywords:
        pattern = rf"{re.escape(keyword.lower())}"
        if re.search(pattern, text, re.IGNORECASE):
            return True
    return False

def search_metadata(service, keywords, geometry_types, extent_polygon):
    """
    Search the metadata for utility-related keywords and filter by geometry type.

    Args:
        service (dict): The service metadata.
        keywords (list): A list of utility-related keywords.
        geometry_types (list): A list of desired geometry types.
        extent_polygon (Polygon): The extent polygon for filtering.

    Returns:
        dict or None: The filtered service metadata if it matches the criteria, None otherwise.
    """
    if (contains_keywords(service.get('layer_name', ''), keywords) or
        contains_keywords(service.get('description', ''), keywords) or
        any(contains_keywords(field, keywords) for field in service.get('fields', []))) and \
        service.get('geometry_type') in geometry_types:
        return service
    return None

# Load metadata from file
with open('services_metadata.json', 'r') as f:
    services_metadata = json.load(f)

# Assuming your polygon and spatial reference are defined as LA County boundaries
county_name = "Los Angeles County, California, USA"
# Initialize the GIS
gis = GIS("https://www.arcgis.com", os.getenv('USERNAME'), os.getenv('PASSWORD'))
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

# Save metadata as it goes along
with open('filtered_metadata.json', 'w') as f:
    json.dump([], f)  # Initialize the file with an empty list

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
    with open('filtered_metadata.json', 'r+') as f:
        data = json.load(f)
        data.append(service)
        f.seek(0)
        json.dump(data, f, indent=4)

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

# Remove non-Sketch layers from the webmap
layers_to_remove = [webmap_obj.layers[i] for i in range(len(webmap_obj.layers)) if (webmap_obj.layers[i]['title'] not in ['Sketch'])]
for i in tqdm(range(len(layers_to_remove))):
    webmap_obj.remove_layer(layers_to_remove[i])

# Query and display features within the polygon
for layer in tqdm(feature_layers):
    try:
        result = layer.query(geometry_filter=intersects(extent_polygon))
        if result.features:
            webmap_obj.add_layer(layer)
        else:
            print(f"No features found in layer {layer.properties['name']}")
    except Exception as e:
        print(f"Error querying layer {layer.properties['name']}: {e}")

webmap_obj.update()
