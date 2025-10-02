#!/usr/bin/env python3
"""
Script to process INE salaries data and transform it into a clean format
Input: extraction_folder/ine_salarios_medias_percentiles.csv
Output: processed_folder/ine_salarios_medias_percentiles_processed.csv
Key structure: (year, comunidad_autonoma, sexo, medida)
"""

import pandas as pd
import os
import sys
from utils import apply_community_mapping
import re
from datetime import datetime

def extract_information(series_name):
    """
    Extract sexo, comunidad_autonoma, and medida from series name
    Format: "Mujeres. Andalucía. Dato base. Media." or "Hombres. Madrid, Comunidad de. Dato base. 25."
    """
    try:
        # Split by periods and clean
        parts = [part.strip() for part in series_name.split('.')]
        
        if len(parts) >= 4:
            sexo = parts[0].strip()
            comunidad_autonoma = parts[1].strip()
            medida = parts[3].strip()
            
            # Clean medida names
            medida_clean = medida.replace(' ', '_').lower()
            if medida_clean == 'media':
                medida_clean = 'media'
            elif medida_clean == '25':
                medida_clean = 'cuartil_25'
            elif medida_clean == '50':
                medida_clean = 'mediana'
            elif medida_clean == '75':
                medida_clean = 'cuartil_75'
            
            return sexo, comunidad_autonoma, medida_clean
        else:
            return None, None, None
            
    except Exception as e:
        return None, None, None

def process_data():
    """
    Main processing function
    """
    # Define file paths
    input_file = "../../extraction_folder/ine_salarios_medias_percentiles.csv"
    output_dir = "../../processed_folder"
    output_file = "ine_salarios_medias_percentiles_processed.csv"
    output_path = os.path.join(output_dir, output_file)

    try:
        # Read raw data
        df_raw = pd.read_csv(input_file)
        
        # Extract information from series_name
        df_raw[['sexo', 'comunidad_autonoma', 'medida']] = df_raw['series_name'].apply(
            lambda x: pd.Series(extract_information(x))
        )

        # Filter out rows where extraction failed
        df_filtered = df_raw.dropna(subset=['sexo', 'comunidad_autonoma', 'medida']).copy()
        
        # Standardize community names using the mapping
        df_filtered = apply_community_mapping(df_filtered, 'comunidad_autonoma')
        
        # Create the final structure
        df_processed = df_filtered[['year', 'comunidad_autonoma', 'sexo', 'medida', 'value']].copy()
        
        # Ensure year is integer type
        df_processed['year'] = pd.to_numeric(df_processed['year'], errors='coerce').astype('Int64')
        
        # Sort by year, comunidad, sexo, medida
        df_processed = df_processed.sort_values(['year', 'comunidad_autonoma', 'sexo', 'medida']).reset_index(drop=True)
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save processed data
        df_processed.to_csv(output_path, index=False, encoding='utf-8')
        
        return df_processed
        
    except FileNotFoundError:
        print(f"Error: Input file not found: {input_file}")
        print("Please make sure the extraction script has been run first.")
        return None
    except Exception as e:
        print(f"Error processing data: {e}")
        return None

def main():
    """
    Main function to run the data processing
    """
    processed_df = process_data()
    
    if processed_df is None:
        sys.exit(1)

if __name__ == "__main__":
    main()