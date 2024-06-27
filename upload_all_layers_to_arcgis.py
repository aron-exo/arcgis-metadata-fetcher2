import json
from tqdm import tqdm
from arcgis.gis import GIS
from arcgis.geometry import Polygon
from arcgis.features import FeatureLayer
from arcgis.mapping import WebMap
from arcgis.geometry.filters import intersects
import os
import osmnx as ox

# Load the list of layers from the JSON file
with open('added_layers.json', 'r') as f:
    layers_for_webmap = json.load(f)

# Define the place of interest
county_name = "Los Angeles County, California, USA"

# Get the boundary of Los Angeles County
county_gdf = ox.geocode_to_gdf(county_name)
county_polygon = county_gdf.loc[0, 'geometry']
extent_polygon = Polygon(county_polygon.__geo_interface__)

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

# Check if the WebMap already exists
existing_maps = gis.content.search(query=f'title:{county_name}', item_type='Web Map')
if existing_maps and existing_maps[0].owner == gis.users.me.username:
    webmap_item = existing_maps[0]
    webmap_obj = WebMap(webmap_item)
    # Remove all existing layers
    webmap_obj.remove_layers()
else:
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
if existing_maps and existing_maps[0].owner == gis.users.me.username:
    webmap_item = webmap_obj.update({'tags': 'utility, layers', 'snippet': 'Updated WebMap containing utility layers'})
else:
    webmap_item = webmap_obj.save({'title': county_name, 'tags': 'utility, layers', 'snippet': 'WebMap containing utility layers'})

print(f"WebMap created or updated with ID: {webmap_item.id}")
