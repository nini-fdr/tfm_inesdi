#!/usr/bin/env python3
"""
Script to download data from INE (Instituto Nacional de Estadística) API
Table: Número de parejas por comunidades y ciudades autónomas según nacionalidad de la pareja y tipo de unión
Source: https://www.ine.es/jaxi/Tabla.htm?path=/t20/p274/serie/def/p02/&file=02017.px
"""

import requests
import pandas as pd
import json
from datetime import datetime
import sys
import os

def download_ine_data():
    """
    Download data from INE API for the couples table
    """
    
    # API endpoint for the table
    # For PC-Axis tables, we concatenate path and file parameters
    table_id = "t20/p274/serie/def/p02/02017.px"
    api_url = f"https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/{table_id}?tip=A&det=2"
    
    print(f"Downloading data from: {api_url}")
    
    try:
        # Make the API request
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        
        # Parse the JSON response
        data = response.json()
        
        print(f"Successfully retrieved data. Found {len(data)} series.")
        
        # Process the data - include all couple types but exclude Total Nacional
        all_data = []
        
        for series in data:
            series_id = series.get('COD', 'Unknown')
            series_name = series.get('Nombre', 'Unknown')
            series_data = series.get('Data', [])
            
            # Skip Total Nacional data
            if "Total Nacional" in series_name:
                print(f"Skipping series: {series_name} (Total Nacional)")
                continue
            
            # Process all other series (all couple types by community)
            print(f"Processing series: {series_name} (ID: {series_id})")
            
            # Process each data point in the series
            for data_point in series_data:
                if isinstance(data_point, dict):
                    year = data_point.get('NombrePeriodo', '')
                    value = data_point.get('Valor', None)
                    
                    # Only include non-null values
                    if value is not None:
                        all_data.append({
                            'series_id': series_id,
                            'series_name': series_name,
                            'year': year,
                            'value': value
                        })
        
        # Create DataFrame
        df = pd.DataFrame(all_data)
        
        if df.empty:
            print("Warning: No data found in the response.")
            return None
        
        # Define output directory and filename
        output_dir = "../../extraction_folder"
        filename = "ine_parejas_por_nacionalidad_y_tipo_union.csv"
        filepath = os.path.join(output_dir, filename)
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save to CSV
        df.to_csv(filepath, index=False, encoding='utf-8')
        
        print(f"Data saved to: {filepath}")
        print(f"Total records: {len(df)}")
        print(f"Series count: {df['series_id'].nunique()}")
        print(f"Years: {df['year'].min()} to {df['year'].max()}")
        
        # Display sample data
        print("\nSample data:")
        print(df.head(10))
        
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

def main():
    """
    Main function to run the data download
    """
    print("INE Data Download Script")
    print("=" * 50)
    print("Table: Número de parejas por comunidades y ciudades autónomas")
    print("según nacionalidad de la pareja y tipo de unión")
    print("=" * 50)
    
    # Download the data
    df = download_ine_data()
    
    if df is not None:
        print("\n✅ Data download completed successfully!")
    else:
        print("\n❌ Data download failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
