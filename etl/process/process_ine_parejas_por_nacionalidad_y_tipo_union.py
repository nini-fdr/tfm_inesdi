#!/usr/bin/env python3
"""
Script to process INE couples data and transform it into a clean format
Input: extraction_folder/ine_parejas_por_nacionalidad_y_tipo_union.csv
Output: processed_folder/ine_parejas_por_nacionalidad_y_tipo_union_processed.csv
Key structure: (year, comunidad_autonoma, tipo_union, nacionalidad)
"""

import pandas as pd
import os
import sys
from utils import apply_community_mapping
from datetime import datetime

def extract_couple_info(series_name):
    """
    Extract community, union type, and nationality from series name
    Example: 'Andalucía, Total (Parejas), Total (Parejas)' -> ('Andalucía', 'Total (Parejas)', 'Total (Parejas)')
    Example: 'Madrid, Comunidad de, Pareja casada, Ambos españoles' -> ('Madrid, Comunidad de', 'Pareja casada', 'Ambos españoles')
    """
    # Remove quotes if present and split by comma
    clean_name = series_name.strip('"')
    parts = [part.strip() for part in clean_name.split(',')]
    
    if len(parts) >= 3:
        # Handle cases where community name has multiple parts (e.g., "Madrid, Comunidad de")
        if len(parts) == 3:
            community = parts[0]
            union_type = parts[1]
            nationality = parts[2]
        elif len(parts) == 4:
            # Community name has two parts
            community = f"{parts[0]}, {parts[1]}"
            union_type = parts[2]
            nationality = parts[3]
        else:
            # More complex cases - take first part as community, last two as union and nationality
            community = parts[0]
            union_type = parts[-2]
            nationality = parts[-1]
        
        return community, union_type, nationality
    else:
        return None, None, None

def process_data():
    """
    Main processing function
    """
    # Define file paths
    input_file = "../../extraction_folder/ine_parejas_por_nacionalidad_y_tipo_union.csv"
    output_dir = "../../processed_folder"
    output_file = "ine_parejas_por_nacionalidad_y_tipo_union_processed.csv"
    output_path = os.path.join(output_dir, output_file)

    try:
        # Read data
                df = pd.read_csv(input_file)
                
        # Extract couple information from series_name
                couple_info = df['series_name'].apply(extract_couple_info)
        df['comunidad_autonoma'] = [info[0] for info in couple_info]
        df['tipo_union'] = [info[1] for info in couple_info]
        df['nacionalidad'] = [info[2] for info in couple_info]
        
        # Filter out records where we couldn't extract complete information
        initial_count = len(df)
        df = df.dropna(subset=['comunidad_autonoma', 'tipo_union', 'nacionalidad'])
        filtered_count = len(df)
                
        # Standardize community names using the mapping
                df = apply_community_mapping(df, 'comunidad_autonoma')
                
        # Create the final processed DataFrame
                processed_df = df[['year', 'comunidad_autonoma', 'tipo_union', 'nacionalidad', 'value']].copy()
        
        # Rename the value column to be more descriptive
        processed_df.rename(columns={'value': 'numero_parejas'}, inplace=True)
        
        # Convert year to integer
        processed_df['year'] = processed_df['year'].astype(int)
        
        # Sort by year, comunidad_autonoma, tipo_union, and nacionalidad
        processed_df = processed_df.sort_values(['year', 'comunidad_autonoma', 'tipo_union', 'nacionalidad'])
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save processed data
                processed_df.to_csv(output_path, index=False, encoding='utf-8')
        
        # Display summary statistics
                                                
        # Display sample data
                        
        # Display column information
                        
        # Display summary by community and union type
                df_2020 = processed_df[processed_df['year'] == 2020]
        df_2020_total = df_2020[df_2020['tipo_union'] == 'Total (Parejas)'].sort_values('numero_parejas', ascending=False)
                
        # Display union type breakdown
                union_summary = df_2020.groupby('tipo_union')['numero_parejas'].sum().sort_values(ascending=False)
                
        # Display nationality breakdown
                nationality_summary = df_2020.groupby('nacionalidad')['numero_parejas'].sum().sort_values(ascending=False)
                
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