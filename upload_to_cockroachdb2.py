import os
import psycopg2
import json
import pandas as pd
from arcgis.features import FeatureLayer
import re
from shapely.geometry import shape
from shapely.wkb import dumps


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
    name = re.sub(r'\W+', '_', name)
    if name[0].isdigit():
        name = '_' + name
    return name.lower()

def create_table_from_dataframe(table_name, dataframe):
    columns = []
    for column_name in dataframe.columns:
        escaped_column_name = f'"{column_name}"'
        if column_name.lower() == 'shape':
            columns.append(f"{escaped_column_name} JSONB")
            columns.append(f"{escaped_column_name}_ewkb BYTEA")
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
        drawing_info JSONB
    )
    """
    print(f"Creating table with query: {create_table_query}")
    cur.execute(create_table_query)
    conn.commit()

def convert_geometry_to_ewkb(geometry, srid):
    if geometry is None:
        return None
    return dumps(geometry, srid=srid, hex=True)

def sanitize_value(value):
    if pd.isna(value):
        return None
    return value

def insert_dataframe_to_database(table_name, dataframe, srid, drawing_info):
    drawing_info_dict = dict(drawing_info)
    
    for _, row in dataframe.iterrows():
        row = row.apply(sanitize_value)
        original_geometry = json.loads(row['SHAPE'])
        ewkb_geometry = convert_geometry_to_ewkb(shape(original_geometry), srid)
        row['srid'] = srid
        row['drawing_info'] = json.dumps(drawing_info_dict)
        row['SHAPE_EWKB'] = ewkb_geometry
        
        columns = ', '.join([f'"{col}"' for col in row.index])
        values = ', '.join(['%s'] * len(row))
        update_set = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in row.index])
        insert_query = f"""
        INSERT INTO {table_name} ({columns}) 
        VALUES ({values}) 
        ON CONFLICT (id) DO UPDATE SET {update_set}
        """
        print(f"Inserting row with query: {insert_query}")
        cur.execute(insert_query, tuple(row))
    
    conn.commit()

def process_and_store_layers(layers_json_path):
    with open(layers_json_path, 'r') as file:
        layers_data = json.load(file)
    
    for layer in layers_data:
        layer_name = layer['title']
        layer_url = layer['url']
        
        feature_layer = FeatureLayer(layer_url)
        sdf = feature_layer.query().sdf
        
        try:
            srid = feature_layer.properties.extent['spatialReference']['latestWkid']
            drawing_info = feature_layer.properties.drawingInfo
        except (TypeError, KeyError):
            print(f"Spatial reference or drawing info not available for layer: {layer_name}. Skipping.")
            continue
        
        print(f"Processing layer: {layer_name}")
        print(sdf.head())
        
        if sdf.empty:
            print(f"No data found for layer: {layer_name}")
            continue
        
        table_name = sanitize_table_name(layer_name)
        create_table_from_dataframe(table_name, sdf)
        insert_dataframe_to_database(table_name, sdf, srid, drawing_info)

# Example usage
process_and_store_layers("added_layers(small).json")

# Close the cursor and connection
cur.close()
conn.close()
