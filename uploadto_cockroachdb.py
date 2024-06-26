import os
import psycopg2
import json
import pandas as pd
import geopandas as gpd
from arcgis.features import FeatureLayer
import re

def connect_to_database():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    return conn

def sanitize_table_name(name):
    # Remove or replace special characters to ensure valid SQL identifiers
    name = re.sub(r'\W+', '_', name)
    if name[0].isdigit():
        name = '_' + name
    return name.lower()

def create_metadata_table(cur):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS metadata (
        id SERIAL PRIMARY KEY,
        layer_name TEXT UNIQUE,
        srid INTEGER,
        drawing_info JSONB
    )
    """
    print(f"Creating metadata table with query: {create_table_query}")  # Debug print
    cur.execute(create_table_query)
    cur.connection.commit()
    print(f"Metadata table created successfully.")

def insert_metadata(cur, layer_name, srid, drawing_info):
    # Convert PropertyMap to dictionary if necessary
    if hasattr(drawing_info, 'to_dict'):
        drawing_info = drawing_info.to_dict()
    else:
        drawing_info = dict(drawing_info)

    insert_query = """
    INSERT INTO metadata (layer_name, srid, drawing_info)
    VALUES (%s, %s, %s)
    ON CONFLICT (layer_name) DO UPDATE
    SET srid = EXCLUDED.srid,
        drawing_info = EXCLUDED.drawing_info
    RETURNING id
    """
    print(f"Inserting metadata with query: {insert_query}")  # Debug print
    cur.execute(insert_query, (layer_name, srid, json.dumps(drawing_info)))
    metadata_id = cur.fetchone()[0]
    cur.connection.commit()
    print(f"Metadata inserted successfully with id {metadata_id}.")
    return metadata_id

def create_table_from_dataframe(cur, table_name, dataframe, metadata_id):
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
        metadata_id INTEGER REFERENCES metadata(id)
    )
    """
    print(f"Creating table with query: {create_table_query}")  # Debug print
    cur.execute(create_table_query)
    cur.connection.commit()
    print(f"Table {table_name} created successfully.")

def convert_geometry_to_json(geometry):
    if geometry is None:
        return None
    return json.dumps(geometry.__geo_interface__)

def sanitize_value(value):
    if pd.isna(value):
        return None
    return value

def insert_dataframe_to_supabase(cur, table_name, dataframe, metadata_id):
    # Convert the SHAPE column to JSONB if it's a GeoDataFrame or has geospatial data
    if 'SHAPE' in dataframe.columns:
        dataframe['SHAPE'] = dataframe['SHAPE'].apply(convert_geometry_to_json)
    
    for _, row in dataframe.iterrows():
        row = row.apply(sanitize_value)
        row['metadata_id'] = metadata_id
        columns = ', '.join([f'"{col}"' for col in row.index])
        values = ', '.join(['%s'] * len(row))
        update_set = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in row.index])
        insert_query = f"""
        INSERT INTO {table_name} ({columns}) 
        VALUES ({values}) 
        ON CONFLICT (id) DO UPDATE SET {update_set}
        """
        print(f"Inserting row with query: {insert_query}")  # Debug print
        cur.execute(insert_query, tuple(row))
    
    cur.connection.commit()
    print(f"Data inserted into {table_name} successfully.")

def check_table_exists(cur, table_name):
    cur.execute("""
    SELECT EXISTS (
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_name = %s
    );
    """, (table_name,))
    return cur.fetchone()[0]

def process_and_store_layers(layers_json_path):
    conn = connect_to_database()
    cur = conn.cursor()
    
    # Create metadata table if not exists
    create_metadata_table(cur)
    
    with open(layers_json_path, 'r') as file:
        layers_data = json.load(file)
    
    for layer in layers_data:
        layer_name = layer['title']
        table_name = sanitize_table_name(layer_name)
        
        # Skip if table already exists
        if check_table_exists(cur, table_name):
            print(f"Table {table_name} already exists. Skipping.")
            continue

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
        
        metadata_id = insert_metadata(cur, layer_name, srid, drawing_info)
        create_table_from_dataframe(cur, table_name, sdf, metadata_id)
        insert_dataframe_to_supabase(cur, table_name, sdf, metadata_id)
    
    # Close the cursor and connection
    cur.close()
    conn.close()
    print("Processing complete. Connection closed.")

# Example usage
process_and_store_layers("added_layers.json")
