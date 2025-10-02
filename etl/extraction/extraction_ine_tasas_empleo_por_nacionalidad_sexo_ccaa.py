#!/usr/bin/env python3
"""
Script to download data from INE (Instituto Nacional de Estadística) API
Table: Tasas de empleo por nacionalidad, sexo y comunidad autónoma
Source: https://www.ine.es/jaxiT3/Tabla.htm?t=65310&L=0
"""

import requests
import pandas as pd
import json
import sys
import os

def download_ine_data():
    """
    Download data from INE API for the employment rates table
    """
    
    # API endpoint for the table
    # For Tempus3 tables, we use the 't' parameter
    table_id = "65310"
    api_url = f"https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/{table_id}?tip=A&det=2"
    
    try:
        # Make the API request
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        
        # Parse the JSON response
        data = response.json()
        
        # Process the data - only include Hombres and Mujeres, exclude Total Nacional, exclude Española and Extranjera
        all_data = []
        
        for series in data:
            series_id = series.get('COD', 'Unknown')
            series_name = series.get('Nombre', 'Unknown')
            series_data = series.get('Data', [])
            
            # Filter: Hombres or Mujeres, exclude Total Nacional, exclude Española and Extranjera
            if (series_name.startswith('Tasa de empleo de la población. Hombres.') or 
                series_name.startswith('Tasa de empleo de la población. Mujeres.')) and \
               'Total Nacional' not in series_name and \
               'Española' not in series_name and \
               'Extranjera:' not in series_name:
                
                # Process each data point in the series
                for data_point in series_data:
                    if isinstance(data_point, dict):
                        year = data_point.get('Anyo', '')
                        quarter = data_point.get('Periodo', '') # For EPA data, 'Periodo' contains the quarter (e.g., T1, T2)
                        period = data_point.get('NombrePeriodo', '') # Full period like 2023T4
                        value = data_point.get('Valor', None)
                        
                        # Only include non-null values
                        if value is not None:
                            all_data.append({
                                'series_id': series_id,
                                'series_name': series_name,
                                'year': year,
                                'quarter': quarter,
                                'period': period,
                                'value': value
                            })
            else:
                        
        # Create DataFrame
        df = pd.DataFrame(all_data)
        
        if df.empty:
                        return None
        
        # Define output directory and filename
        output_dir = "../../extraction_folder"
        filename = "ine_tasas_empleo_por_nacionalidad_sexo_ccaa.csv"
        filepath = os.path.join(output_dir, filename)
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save to CSV
        df.to_csv(filepath, index=False, encoding='utf-8')

        # Display sample data
                        
        return df
        
    except requests.exceptions.RequestException as e:
                return None
    except json.JSONDecodeError as e:
                return None
    except Exception as e:
                return None

def main():
    """
    Main function to run the data download
    """
                    
    # Download the data
    df = download_ine_data()
    
    if df is not None:
            else:
                sys.exit(1)

if __name__ == "__main__":
    main()