#!/usr/bin/env python3
"""
Script to download data from INE (Instituto Nacional de Estadística) API
Table: Delitos según tipo por comunidades y ciudades autónomas
Source: https://www.ine.es/jaxi/Tabla.htm?tpx=62327
"""

import requests
import pandas as pd
import json
import sys
import os

def download_ine_data():
    """
    Download data from INE API for the crimes table
    """
    
    # API endpoint for the table
    # For TPX tables, we use the 'tpx' parameter
    table_id = "62327"
    api_url = f"https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/{table_id}?tip=A&det=2"
    
    print(f"Downloading data from: {api_url}")
    
    try:
        # Make the API request
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        
        # Parse the JSON response
        data = response.json()
        
        print(f"Successfully retrieved data. Found {len(data)} series.")
        
        # Define the crime types related to family and sexuality that we want to include
        family_sexuality_crimes = [
            "8 Contra la libertad e indemnidad sexuales",
            "12 Contra las relaciones familiares",
            "12.3 Contra los derechos y deberes familiares",
            "12.3.1 Quebrantamiento de los deberes de custodia",
            "12.3.2 Sustracción de menores",
            "12.3.3 Abandono de familia",
            "12.99 Otros delitos contra las relaciones familiares"
        ]
        
        # Process the data - only include family and sexuality related crimes, exclude Total Nacional
        all_data = []
        
        for series in data:
            series_id = series.get('COD', 'Unknown')
            series_name = series.get('Nombre', 'Unknown')
            series_data = series.get('Data', [])
            
            # Check if this series contains family/sexuality crimes and exclude Total Nacional
            is_family_sexuality_crime = any(crime_type in series_name for crime_type in family_sexuality_crimes)
            is_not_national_total = 'Total Nacional' not in series_name
            
            if is_family_sexuality_crime and is_not_national_total:
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
            else:
                print(f"Skipping series: {series_name} (not family/sexuality crime or Total Nacional)")
        
        # Create DataFrame
        df = pd.DataFrame(all_data)
        
        if df.empty:
            print("Warning: No data found in the response.")
            return None
        
        # Define output directory and filename
        output_dir = "../../extraction_folder"
        filename = "ine_delitos_familia_sexualidad.csv"
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
    print("Table: Delitos según tipo por comunidades y ciudades autónomas")
    print("Focus: Family and sexuality related crimes")
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
