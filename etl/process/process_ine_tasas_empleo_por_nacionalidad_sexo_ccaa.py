#!/usr/bin/env python3
"""
Script to process INE employment rates data (Tasas de empleo por nacionalidad, sexo y comunidad autónoma) 
and transform it into a clean format with annual averages
Input: extraction_folder/ine_tasas_empleo_por_nacionalidad_sexo_ccaa.csv
Output: processed_folder/ine_tasas_empleo_por_nacionalidad_sexo_ccaa_processed.csv
Key structure: (year, comunidad_autonoma, genero)
"""

import pandas as pd
import os
import sys
import re
import ast
from datetime import datetime
from utils import apply_community_mapping

def extract_quarter_info(quarter_str):
    """
    Extract quarter information from the quarter string
    Format: "{'Codigo': 'II', 'Nombre': 'T2'}"
    """
    try:
        # Parse the string as a dictionary
        quarter_dict = ast.literal_eval(quarter_str)
        codigo = quarter_dict.get('Codigo', '')
        nombre = quarter_dict.get('Nombre', '')
        return codigo, nombre
    except:
        return None, None

def extract_community_and_gender(series_name):
    """
    Extract comunidad_autonoma and genero from series name
    Format: "Tasa de empleo de la población. Hombres. Andalucía. Total. "
    """
    try:
        # Split by periods and clean
        parts = [part.strip() for part in series_name.split('.')]
        
        if len(parts) >= 3:
            genero = parts[1].strip()  # Hombres or Mujeres
            comunidad_autonoma = parts[2].strip()  # Community name
            
            return genero, comunidad_autonoma
        return None, None
    except Exception as e:
                return None, None

def process_data():
    """
    Main processing function for INE employment rates data.
    Reads raw data from extraction_folder, calculates annual averages by quarter,
    and saves to processed_folder.
    """
    # Define file paths
    input_file = "../../extraction_folder/ine_tasas_empleo_por_nacionalidad_sexo_ccaa.csv"
    output_dir = "../../processed_folder"
    output_file = "ine_tasas_empleo_por_nacionalidad_sexo_ccaa_processed.csv"
    output_path = os.path.join(output_dir, output_file)

    # 1. Read raw data
        try:
        df_raw = pd.read_csv(input_file)
            except FileNotFoundError:
                        return None

    # 2. Extract quarter information
        df_raw[['quarter_codigo', 'quarter_nombre']] = df_raw['quarter'].apply(
        lambda x: pd.Series(extract_quarter_info(x))
    )

    # 3. Extract community and gender information
        df_raw[['genero', 'comunidad_autonoma']] = df_raw['series_name'].apply(
        lambda x: pd.Series(extract_community_and_gender(x))
    )

    # Filter out rows where extraction failed
    df_filtered = df_raw.dropna(subset=['genero', 'comunidad_autonoma', 'quarter_codigo']).copy()
    
    # 4. Standardize community names using the mapping
        df_filtered = apply_community_mapping(df_filtered, 'comunidad_autonoma')
    
    # 5. Ensure 'year' is integer type
    df_filtered['year'] = pd.to_numeric(df_filtered['year'], errors='coerce').astype('Int64')

    # 6. Calculate annual averages by grouping by year, community, and gender
        df_annual = df_filtered.groupby(['year', 'comunidad_autonoma', 'genero'])['value'].mean().reset_index()
    df_annual.rename(columns={'value': 'tasa_promedio_empleo'}, inplace=True)
    
    # Round to 2 decimal places
    df_annual['tasa_promedio_empleo'] = df_annual['tasa_promedio_empleo'].round(2)

    # 6. Sort by year, community, and gender
    df_annual = df_annual.sort_values(['year', 'comunidad_autonoma', 'genero'])

    # 7. Ensure output directory exists and save processed data
        os.makedirs(output_dir, exist_ok=True)
    df_annual.to_csv(output_path, index=False, encoding='utf-8')

        for col in df_annual.columns:
            
    # Display summary by community for the latest year
        latest_year = df_annual['year'].max()
    summary = df_annual[df_annual['year'] == latest_year].sort_values(
        by='tasa_promedio_empleo', ascending=False
    )
    
    return df_annual

def main():
    """
    Main function to run the data processing
    """
    df = process_data()
    if df is not None:
            else:
                sys.exit(1)

if __name__ == "__main__":
    main()
