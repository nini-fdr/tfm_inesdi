#!/usr/bin/env python3
"""
Script to process INE poverty risk data and transform it into a clean format
Input: extraction_folder/ine_riesgo_pobreza_exclusion_social.csv
Output: processed_folder/ine_riesgo_pobreza_exclusion_social_processed.csv
Key structure: (year, comunidad_autonoma)
"""

import pandas as pd
import os
import sys
from utils import apply_community_mapping
from datetime import datetime

def extract_community_name(series_name):
    """
    Extract community name from series name
    Example: 'Andalucía. Todas las edades. Tasa de riesgo de pobreza o exclusión social (indicador AROPE). Base 2013.' -> 'Andalucía'
    Example: 'Madrid, Comunidad de. Todas las edades. Tasa de riesgo de pobreza o exclusión social (indicador AROPE). Base 2013.' -> 'Madrid, Comunidad de'
    """
    # Remove quotes if present and split by period
    clean_name = series_name.strip('"')
    parts = [part.strip() for part in clean_name.split('.')]
    
    if len(parts) >= 1:
        return parts[0]
    else:
        return None

def process_data():
    """
    Main processing function
    """
    
    # Define file paths
    input_file = "../../extraction_folder/ine_riesgo_pobreza_exclusion_social.csv"
    output_dir = "../../processed_folder"
    output_file = "ine_riesgo_pobreza_exclusion_social_processed.csv"
    output_path = os.path.join(output_dir, output_file)

    try:
        # Read the raw data
                df = pd.read_csv(input_file)
                
        # Extract community name from series_name
                df['comunidad_autonoma'] = df['series_name'].apply(extract_community_name)
        
        # Filter out records where we couldn't extract community name
        initial_count = len(df)
        df = df.dropna(subset=['comunidad_autonoma'])
        filtered_count = len(df)
                
        # Standardize community names using the mapping
                df = apply_community_mapping(df, 'comunidad_autonoma')
                
        # Create the final processed DataFrame
                processed_df = df[['year', 'comunidad_autonoma', 'value']].copy()
        
        # Rename the value column to be more descriptive
        processed_df.rename(columns={'value': 'tasa_arope'}, inplace=True)
        
        # Convert year to integer
        processed_df['year'] = processed_df['year'].astype(int)
        
        # Sort by year and comunidad_autonoma
        processed_df = processed_df.sort_values(['year', 'comunidad_autonoma'])
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save processed data
                processed_df.to_csv(output_path, index=False, encoding='utf-8')
        
        # Display summary statistics
                                                
        # Display sample data
                        
        # Display column information
                        
        # Display summary by community (2024 data)
                df_2024 = processed_df[processed_df['year'] == 2024].sort_values('tasa_arope', ascending=False)
                
        # Display summary statistics
                                                
        return processed_df
        
    except FileNotFoundError:
                        return None
    except Exception as e:
                return None

def main():
    """
    Main function to run the data processing
    """
    processed_df = process_data()
    
    if processed_df is not None:
                    else:
                sys.exit(1)

if __name__ == "__main__":
    main()
