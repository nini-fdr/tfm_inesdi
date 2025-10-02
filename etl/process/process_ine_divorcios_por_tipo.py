#!/usr/bin/env python3
"""
Script to process INE divorces data and transform it into a clean format
Input: extraction_folder/ine_divorcios_por_tipo.csv
Output: processed_folder/ine_divorcios_por_tipo_processed.csv
Key structure: (year, comunidad_autonoma, tipo_divorcio)
"""

import pandas as pd
import os
import sys
from utils import apply_community_mapping
from datetime import datetime

def extract_community_and_type(series_name):
    """
    Extract community and divorce type from series name
    Example: 'Divorcios. Andalucía. Dato base. Total.' -> ('Andalucía', 'Total')
    """
    parts = series_name.split('. ')
    
    if len(parts) >= 4:
        community = parts[1].strip()
        divorce_type = parts[3].strip()
        return community, divorce_type
    else:
        return None, None

def process_divorces_data():
    """
    Process the raw divorces data and transform it into a clean format
    """
    
    # Define file paths
    input_file = "../../extraction_folder/ine_divorcios_por_tipo.csv"
    output_dir = "../../processed_folder"
    output_file = "ine_divorcios_por_tipo_processed.csv"
    output_path = os.path.join(output_dir, output_file)

    try:
        # Read the raw data
                df = pd.read_csv(input_file)
                
        # Extract community and divorce type from series_name
                df[['comunidad_autonoma', 'tipo_divorcio']] = df['series_name'].apply(
            lambda x: pd.Series(extract_community_and_type(x))
        )
        
        # Filter out records where we couldn't extract community/type
        initial_count = len(df)
        df = df.dropna(subset=['comunidad_autonoma', 'tipo_divorcio'])
        filtered_count = len(df)
                
        # Standardize community names using the mapping
                df = apply_community_mapping(df, 'comunidad_autonoma')
                
        # Create the final processed DataFrame with long format
                processed_df = df[['year', 'comunidad_autonoma', 'tipo_divorcio', 'value']].copy()
        
        # Rename the value column to be more descriptive
        processed_df.rename(columns={'value': 'numero_divorcios'}, inplace=True)
        
        # Convert year to integer
        processed_df['year'] = processed_df['year'].astype(int)
        
        # Sort by year, comunidad_autonoma, and tipo_divorcio
        processed_df = processed_df.sort_values(['year', 'comunidad_autonoma', 'tipo_divorcio'])
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save processed data
                processed_df.to_csv(output_path, index=False, encoding='utf-8')
        
        # Display summary statistics
                                                
        # Display sample data
                        
        # Display column information
                        
        # Display summary by divorce type
                df_2020 = processed_df[processed_df['year'] == 2020]
        divorce_summary = df_2020.groupby('tipo_divorcio')['numero_divorcios'].sum().sort_values(ascending=False)
                
        # Display top communities by total divorces
                df_2020_total = df_2020[df_2020['tipo_divorcio'] == 'Total'].sort_values('numero_divorcios', ascending=False)
                
        return processed_df
        
    except FileNotFoundError:
                        return None
    except Exception as e:
                return None

def main():
    """
    Main function to run the data processing
    """
    processed_df = process_divorces_data()
    
    if processed_df is not None:
                    else:
                sys.exit(1)

if __name__ == "__main__":
    main()
