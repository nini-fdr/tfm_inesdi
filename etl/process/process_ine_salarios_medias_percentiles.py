#!/usr/bin/env python3
"""
Script to process INE salaries data (Medias y percentiles por sexo y CCAA) and transform it into a clean format
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
        print(f"Error parsing series name '{series_name}': {e}")
        return None, None, None

def process_data():
    """
    Main processing function for INE salaries data
    """
    # Define file paths
    input_file = "../../extraction_folder/ine_salarios_medias_percentiles.csv"
    output_dir = "../../processed_folder"
    output_file = "ine_salarios_medias_percentiles_processed.csv"
    output_path = os.path.join(output_dir, output_file)

    print("INE Salaries Data Processing Script")
    print("=" * 50)
    print(f"Input file: {input_file}")
    print(f"Output file: {output_path}")
    print("=" * 50)

    try:
        # 1. Read raw data
        print("Reading raw data...")
        df_raw = pd.read_csv(input_file)
        print(f"Loaded {len(df_raw)} records")

        # 2. Extract information from series_name
        print("Extracting information from series names...")
        df_raw[['sexo', 'comunidad_autonoma', 'medida']] = df_raw['series_name'].apply(
            lambda x: pd.Series(extract_information(x))
        )

        # 3. Filter out rows where extraction failed
        df_filtered = df_raw.dropna(subset=['sexo', 'comunidad_autonoma', 'medida']).copy()
        print(f"Filtered out {len(df_raw) - len(df_filtered)} records with missing info")

        # 4. Standardize community names using the mapping
        print("Standardizing community names...")
        df_filtered = apply_community_mapping(df_filtered, 'comunidad_autonoma')
        print(f"After standardization: {len(df_filtered)} records")

        # 5. Create the final structure
        print("Creating processed data structure...")
        df_processed = df_filtered[['year', 'comunidad_autonoma', 'sexo', 'medida', 'value']].copy()
        
        # 5. Ensure year is integer type
        df_processed['year'] = pd.to_numeric(df_processed['year'], errors='coerce').astype('Int64')
        
        # 6. Sort by year, comunidad, sexo, medida
        df_processed = df_processed.sort_values(['year', 'comunidad_autonoma', 'sexo', 'medida']).reset_index(drop=True)

        # 7. Ensure output directory exists and save processed data
        print("Saving processed data...")
        os.makedirs(output_dir, exist_ok=True)
        df_processed.to_csv(output_path, index=False, encoding='utf-8')

        print("\n✅ Processing completed successfully!")
        print(f"Processed records: {len(df_processed)}")
        print(f"Years covered: {df_processed['year'].min()} to {df_processed['year'].max()}")
        print(f"Communities: {df_processed['comunidad_autonoma'].nunique()}")
        print(f"Sexos: {df_processed['sexo'].nunique()}")
        print(f"Medidas: {df_processed['medida'].nunique()}")

        print("\nSample processed data:")
        print(df_processed.head(10))

        print("\nColumns in processed data:")
        for col in df_processed.columns:
            print(f"  - {col}")

        print("\nUnique values:")
        print(f"  - Sexos: {sorted(df_processed['sexo'].unique())}")
        print(f"  - Medidas: {sorted(df_processed['medida'].unique())}")
        print(f"  - Communities: {df_processed['comunidad_autonoma'].nunique()}")

        # Display summary by sexo and medida for the latest year
        print("\nSummary by sexo and medida (2023 data):")
        latest_year = df_processed['year'].max()
        summary_2023 = df_processed[df_processed['year'] == latest_year].groupby(['sexo', 'medida'])['value'].agg(['count', 'mean', 'min', 'max']).round(2)
        print(summary_2023)

        return df_processed

    except FileNotFoundError:
        print(f"❌ Error: Input file not found: {input_file}")
        print("Please ensure the corresponding extraction script has been run first.")
        return None
    except Exception as e:
        print(f"❌ Error processing data: {e}")
        return None

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
