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
        print(f"Error extracting info from '{series_name}': {e}")
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

    print("INE Employment Rates Data Processing Script")
    print("=" * 50)
    print(f"Input file: {input_file}")
    print(f"Output file: {output_path}")
    print("=" * 50)

    # 1. Read raw data
    print("Reading raw data...")
    try:
        df_raw = pd.read_csv(input_file)
        print(f"Loaded {len(df_raw)} records")
    except FileNotFoundError:
        print(f"❌ Error: Input file not found: {input_file}")
        print("Please ensure the corresponding extraction script has been run first.")
        return None

    # 2. Extract quarter information
    print("Extracting quarter information...")
    df_raw[['quarter_codigo', 'quarter_nombre']] = df_raw['quarter'].apply(
        lambda x: pd.Series(extract_quarter_info(x))
    )

    # 3. Extract community and gender information
    print("Extracting community and gender information...")
    df_raw[['genero', 'comunidad_autonoma']] = df_raw['series_name'].apply(
        lambda x: pd.Series(extract_community_and_gender(x))
    )

    # Filter out rows where extraction failed
    df_filtered = df_raw.dropna(subset=['genero', 'comunidad_autonoma', 'quarter_codigo']).copy()
    print(f"Filtered out {len(df_raw) - len(df_filtered)} records with missing info")

    # 4. Standardize community names using the mapping
    print("Standardizing community names...")
    df_filtered = apply_community_mapping(df_filtered, 'comunidad_autonoma')
    print(f"After standardization: {len(df_filtered)} records")

    # 5. Ensure 'year' is integer type
    df_filtered['year'] = pd.to_numeric(df_filtered['year'], errors='coerce').astype('Int64')

    # 6. Calculate annual averages by grouping by year, community, and gender
    print("Calculating annual averages...")
    df_annual = df_filtered.groupby(['year', 'comunidad_autonoma', 'genero'])['value'].mean().reset_index()
    df_annual.rename(columns={'value': 'tasa_promedio_empleo'}, inplace=True)
    
    # Round to 2 decimal places
    df_annual['tasa_promedio_empleo'] = df_annual['tasa_promedio_empleo'].round(2)

    # 6. Sort by year, community, and gender
    df_annual = df_annual.sort_values(['year', 'comunidad_autonoma', 'genero'])

    # 7. Ensure output directory exists and save processed data
    print("Saving processed data...")
    os.makedirs(output_dir, exist_ok=True)
    df_annual.to_csv(output_path, index=False, encoding='utf-8')

    print("\n✅ Processing completed successfully!")
    print(f"Processed records: {len(df_annual)}")
    print(f"Years covered: {df_annual['year'].min()} to {df_annual['year'].max()}")
    print(f"Communities: {df_annual['comunidad_autonoma'].nunique()}")
    print(f"Genders: {df_annual['genero'].nunique()}")
    print(f"Output saved to: {output_path}")

    print("\nSample processed data:")
    print(df_annual.head(10))

    print("\nColumns in processed data:")
    for col in df_annual.columns:
        print(f"  - {col}")
    
    # Display summary by community for the latest year
    print(f"\nSummary by community and gender ({df_annual['year'].max()} data):")
    latest_year = df_annual['year'].max()
    summary = df_annual[df_annual['year'] == latest_year].sort_values(
        by='tasa_promedio_empleo', ascending=False
    )
    print(summary.head(10))

    return df_annual

def main():
    """
    Main function to run the data processing
    """
    df = process_data()
    if df is not None:
        print("\n🎉 Data processing completed successfully!")
    else:
        print("\n💥 Data processing failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
