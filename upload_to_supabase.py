import os
import psycopg2
import json
import pandas as pd
import geopandas as gpd
from arcgis.features import FeatureLayer

# Environment variables for Supabase
SUPABASE_DB_HOST = os.getenv('SUPABASE_DB_HOST')
SUPABASE_DB_NAME = os.getenv('SUPABASE_DB_NAME')
SUPABASE_DB_USER = os.getenv('SUPABASE_DB_USER')
SUPABASE_DB_PASSWORD = os.getenv('SUPABASE_DB_PASSWORD')
SUPABASE_DB_PORT = os.getenv('SUPABASE_DB_PORT')

# Connect to Supabase
conn = psycopg2.connect(
    host=SUPABASE_DB_HOST,
    database=SUPABASE_DB_NAME,
    user=SUPABASE_DB_USER,
    password=SUPABASE_DB_PASSWORD,
    port=SUPABASE_DB_PORT
)

cur = conn.cursor()

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
        else:
            columns.append(f"{escaped_column_name} TEXT")
    
    columns_query = ", ".join(columns)
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        {columns_query}
    )
    """
    cur.execute(create_table_query)
    conn.commit()

def insert_dataframe_to_supabase(table_name, dataframe):
    # Convert the SHAPE column to JSONB if it's a GeoDataFrame or has geospatial data
    if 'SHAPE' in dataframe.columns:
        dataframe['SHAPE'] = dataframe['SHAPE'].apply(json.loads)
    
    for _, row in dataframe.iterrows():
        columns = ', '.join([f'"{col}"' for col in row.index])
        values = ', '.join(['%s'] * len(row))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
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
        
        table_name = layer_name.replace(" ", "_").lower()  # Create a suitable table name
        create_table_from_dataframe(table_name, sdf)
        insert_dataframe_to_supabase(table_name, sdf)

# Example usage
process_and_store_layers("path_to_your_added_layers.json")

# Close the cursor and connection
cur.close()
conn.close()