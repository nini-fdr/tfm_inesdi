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
        
        # Clean up community names
        community_mapping = {
            'Comunidad de Madrid': 'Comunidad de Madrid',
            'Región de Murcia': 'Región de Murcia', 
            'Principado de Asturias': 'Principado de Asturias',
            'Illes Balears': 'Illes Balears',
            'Comunidad Foral de Navarra': 'Comunidad Foral de Navarra',
            'La Rioja': 'La Rioja'
        }
        
        # Apply mapping if needed
        if community in community_mapping:
            community = community_mapping[community]
        
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

    print("INE Crimes Data Processing Script")
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
    
    # 2. Extract community and crime type from 'series_name'
    print("Extracting community and crime type information...")
    df_raw[['comunidad_autonoma', 'tipo_delito']] = df_raw['series_name'].apply(
        lambda x: pd.Series(extract_community_crime_type(x))
    )

    # Filter out rows where extraction failed
    df_processed = df_raw.dropna(subset=['comunidad_autonoma', 'tipo_delito', 'year', 'value']).copy()
    print(f"Filtered out {len(df_raw) - len(df_processed)} records with missing info")

    # Ensure 'year' is integer type
    df_processed['year'] = pd.to_numeric(df_processed['year'], errors='coerce').astype('Int64')

    # 3. Standardize community names using the mapping
    print("Standardizing community names...")
    df_processed = apply_community_mapping(df_processed, 'comunidad_autonoma')
    print(f"After standardization: {len(df_processed)} records")

    # 4. Create final structure with key (year, comunidad_autonoma, tipo_delito, numero_delitos)
    print("Creating final data structure...")
    df_final = df_processed[['year', 'comunidad_autonoma', 'tipo_delito', 'value']].copy()
    df_final.rename(columns={'value': 'numero_delitos'}, inplace=True)
    
    # Sort by year, community, and crime type for better organization
    df_final = df_final.sort_values(['year', 'comunidad_autonoma', 'tipo_delito']).reset_index(drop=True)

    # 5. Ensure output directory exists and save processed data
    print("Saving processed data...")
    os.makedirs(output_dir, exist_ok=True)
    df_final.to_csv(output_path, index=False, encoding='utf-8')

    print("\n✅ Processing completed successfully!")
    print(f"Processed records: {len(df_final)}")
    print(f"Years covered: {df_final['year'].min()} to {df_final['year'].max()}")
    print(f"Communities: {df_final['comunidad_autonoma'].nunique()}")
    print(f"Crime types: {df_final['tipo_delito'].nunique()}")
    print(f"Output saved to: {output_path}")

    print("\nSample processed data:")
    print(df_final.head(10))

    print("\nColumns in processed data:")
    for col in df_final.columns:
        print(f"  - {col}")
    
    # Display summary by community for the latest year
    print(f"\nSummary by community and crime type ({df_final['year'].max()} data):")
    latest_year_data = df_final[df_final['year'] == df_final['year'].max()]
    print(latest_year_data.groupby(['comunidad_autonoma', 'tipo_delito'])['numero_delitos'].sum().head(15))

    # Display crime type distribution
    print(f"\nCrime type distribution (total across all years):")
    crime_summary = df_final.groupby('tipo_delito')['numero_delitos'].sum().sort_values(ascending=False)
    print(crime_summary)

    return df_final

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
