#!/usr/bin/env python3
"""
Script to process INE crimes data and transform it into a clean format
Input: extraction_folder/ine_delitos_familia_sexualidad.csv
Output: processed_folder/ine_delitos_familia_sexualidad_processed.csv
Key structure: (year, comunidad_autonoma, tipo_delito, numero_delitos)
"""

import pandas as pd
import os
import sys
import re
from utils import apply_community_mapping

def extract_community_crime_type(series_name):
    """
    Extract community and crime type from series_name.
    Expected formats: 
    - "01 Andalucía, 8 Contra la libertad e indemnidad sexuales"
    - "03 Asturias, Principado de, 8 Contra la libertad e indemnidad sexuales"
    """
    # Split by comma to separate parts
    parts = series_name.split(', ')
    
    if len(parts) >= 2:
        # Extract community (remove the number prefix from first part)
        community_part = parts[0].strip()
        community = re.sub(r'^\d+\s+', '', community_part).strip()
        
        # Check if there's a second part that's a community qualifier
        if len(parts) >= 3:
            # Format: "03 Asturias, Principado de, 8 Contra la libertad..."
            qualifier = parts[1].strip()
            crime_type = ', '.join(parts[2:]).strip()
            
            # Combine community with qualifier
            community = f"{qualifier} {community}"
        else:
            # Format: "01 Andalucía, 8 Contra la libertad..."
            crime_type = ', '.join(parts[1:]).strip()
        
        # Community mapping will be handled by apply_community_mapping from utils.py
        
        return community, crime_type
    return None, None

def process_data():
    """
    Main processing function for INE crimes data.
    Reads raw data from extraction_folder, extracts relevant information,
    and saves to processed_folder.
    """
    # Define file paths
    input_file = "../../extraction_folder/ine_delitos_familia_sexualidad.csv"
    output_dir = "../../processed_folder"
    output_file = "ine_delitos_familia_sexualidad_processed.csv"
    output_path = os.path.join(output_dir, output_file)

    # 1. Read raw data
        try:
        df_raw = pd.read_csv(input_file)
            except FileNotFoundError:
                        return None
    
    # 2. Extract community and crime type from 'series_name'
        df_raw[['comunidad_autonoma', 'tipo_delito']] = df_raw['series_name'].apply(
        lambda x: pd.Series(extract_community_crime_type(x))
    )

    # Filter out rows where extraction failed
    df_processed = df_raw.dropna(subset=['comunidad_autonoma', 'tipo_delito', 'year', 'value']).copy()
    
    # Ensure 'year' is integer type
    df_processed['year'] = pd.to_numeric(df_processed['year'], errors='coerce').astype('Int64')

    # 3. Standardize community names using the mapping
        df_processed = apply_community_mapping(df_processed, 'comunidad_autonoma')
    
    # 4. Create final structure with key (year, comunidad_autonoma, tipo_delito, numero_delitos)
        df_final = df_processed[['year', 'comunidad_autonoma', 'tipo_delito', 'value']].copy()
    df_final.rename(columns={'value': 'numero_delitos'}, inplace=True)
    
    # Sort by year, community, and crime type for better organization
    df_final = df_final.sort_values(['year', 'comunidad_autonoma', 'tipo_delito']).reset_index(drop=True)

    # 5. Ensure output directory exists and save processed data
        os.makedirs(output_dir, exist_ok=True)
    df_final.to_csv(output_path, index=False, encoding='utf-8')

        for col in df_final.columns:
            
    # Display summary by community for the latest year
        latest_year_data = df_final[df_final['year'] == df_final['year'].max()]
    
    # Display crime type distribution
        crime_summary = df_final.groupby('tipo_delito')['numero_delitos'].sum().sort_values(ascending=False)
    
    return df_final

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
