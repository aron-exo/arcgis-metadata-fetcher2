import os
import psycopg2
import json
import pandas as pd
import geopandas as gpd
from arcgis.features import FeatureLayer
import re

def connect_to_database():
    conn = psycopg2.connect(
        host=os.getenv('COCKROACH_DB_HOST'),
        database=os.getenv('COCKROACH_DB_DATABASE'),
        user=os.getenv('COCKROACH_DB_USER'),
        password=os.getenv('COCKROACH_DB_PASSWORD'),
        port=os.getenv('COCKROACH_DB_PORT')
    )
    return conn

conn = connect_to_database()
cur = conn.cursor()

def sanitize_table_name(name):
    # Remove or replace special characters to ensure valid SQL identifiers
    name = re.sub(r'\W+', '_', name)
    if name[0].isdigit():
        name = '_' + name
    return name.lower()

def check_table_exists(table_name):
    cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}');")
    return cur.fetchone()[0]

def create_table_from_dataframe(table_name, dataframe):
    # Dynamically create table structure based on dataframe columns
    columns = []
    
    for column_name in dataframe.columns:
        escaped_column_name = f'"{column_name}"'
        if column_name.lower() == 'shape':
            columns.append(f"{escaped_column_name} JSONB")
        elif dataframe[column_name].dtype == 'int64':
            columns.append(f"{escaped_column_name} INTEGER")
        elif dataframe[column_name].dtype == 'float64':
            columns.append(f"{escaped_column_name} FLOAT")
        elif pd.api.types.is_datetime64_any_dtype(dataframe[column_name]):
            columns.append(f"{escaped_column_name} TIMESTAMP")
        else:
            columns.append(f"{escaped_column_name} TEXT")
    
    if not columns:
        raise ValueError("No columns defined for the table.")
    
    columns_query = ", ".join(columns)
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        {columns_query},
        srid INTEGER,
        drawing_info JSONB,
        UNIQUE ("{dataframe.columns[0]}")
    )
    """
    print(f"Creating table with query: {create_table_query}")  # Debug print
    cur.execute(create_table_query)
    conn.commit()

def convert_geometry_to_json(geometry):
    if geometry is None:
        return None
    return json.dumps(geometry.__geo_interface__)

def sanitize_value(value):
    if pd.isna(value):
        return None
    return value

def insert_dataframe_to_supabase(table_name, dataframe, srid, drawing_info):
    # Convert the SHAPE column to JSONB if it's a GeoDataFrame or has geospatial data
    if 'SHAPE' in dataframe.columns:
        dataframe['SHAPE'] = dataframe['SHAPE'].apply(convert_geometry_to_json)
    
    # Ensure drawing_info is a serializable dictionary
    drawing_info_dict = dict(drawing_info)
    
    for _, row in dataframe.iterrows():
        row = row.apply(sanitize_value)
        row['srid'] = srid
        row['drawing_info'] = json.dumps(drawing_info_dict)
        columns = ', '.join([f'"{col}"' for col in row.index])
        values = ', '.join(['%s'] * len(row))
        update_set = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in row.index if col != 'id'])
        insert_query = f"""
        INSERT INTO {table_name} ({columns}) 
        VALUES ({values}) 
        ON CONFLICT ("{dataframe.columns[0]}") DO UPDATE SET {update_set}
        """
        print(f"Inserting row with query: {insert_query}")  # Debug print
        cur.execute(insert_query, tuple(row))
    
    conn.commit()

def process_and_store_layers(layers_json_path):
    with open(layers_json_path, 'r') as file:
        layers_data = json.load(file)
    
    for layer in layers_data:
        layer_name = layer['title']
        layer_url = layer['url']
        
        # Fetch data for the layer using FeatureLayer
        feature_layer = FeatureLayer(layer_url)
        sdf = feature_layer.query().sdf
        
        # Handle cases where the spatial reference is not available
        try:
            srid = feature_layer.properties.extent['spatialReference']['latestWkid']
            drawing_info = feature_layer.properties.drawingInfo
        except (TypeError, KeyError):
            print(f"Spatial reference or drawing info not available for layer: {layer_name}. Skipping.")
            continue
        
        print(f"Processing layer: {layer_name}")  # Debug print
        print(sdf.head())  # Debug print to show dataframe structure
        
        if sdf.empty:
            print(f"No data found for layer: {layer_name}")
            continue
        
        table_name = sanitize_table_name(layer_name)  # Sanitize table name
        
        if not check_table_exists(table_name):
            create_table_from_dataframe(table_name, sdf)
        
        insert_dataframe_to_supabase(table_name, sdf, srid, drawing_info)

# Example usage
process_and_store_layers("added_layers.json")

# Close the cursor and connection
cur.close()
conn.close()
