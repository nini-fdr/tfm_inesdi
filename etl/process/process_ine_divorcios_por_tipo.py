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
    
    print("INE Divorces Data Processing Script")
    print("=" * 50)
    print(f"Input file: {input_file}")
    print(f"Output file: {output_path}")
    print("=" * 50)
    
    try:
        # Read the raw data
        print("Reading raw data...")
        df = pd.read_csv(input_file)
        print(f"Loaded {len(df)} records")
        
        # Extract community and divorce type from series_name
        print("Extracting community and divorce type information...")
        df[['comunidad_autonoma', 'tipo_divorcio']] = df['series_name'].apply(
            lambda x: pd.Series(extract_community_and_type(x))
        )
        
        # Filter out records where we couldn't extract community/type
        initial_count = len(df)
        df = df.dropna(subset=['comunidad_autonoma', 'tipo_divorcio'])
        filtered_count = len(df)
        print(f"Filtered out {initial_count - filtered_count} records with missing community/type info")
        
        # Standardize community names using the mapping
        print("Standardizing community names...")
        df = apply_community_mapping(df, 'comunidad_autonoma')
        print(f"After standardization: {len(df)} records")
        
        # Create the final processed DataFrame with long format
        print("Creating processed data...")
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
        print("Saving processed data...")
        processed_df.to_csv(output_path, index=False, encoding='utf-8')
        
        # Display summary statistics
        print(f"\n✅ Processing completed successfully!")
        print(f"Processed records: {len(processed_df)}")
        print(f"Years covered: {processed_df['year'].min()} to {processed_df['year'].max()}")
        print(f"Communities: {processed_df['comunidad_autonoma'].nunique()}")
        print(f"Output saved to: {output_path}")
        
        # Display sample data
        print("\nSample processed data:")
        print(processed_df.head(10))
        
        # Display column information
        print(f"\nColumns in processed data:")
        for col in processed_df.columns:
            print(f"  - {col}")
        
        # Display summary by divorce type
        print(f"\nSummary by divorce type (2020 data):")
        df_2020 = processed_df[processed_df['year'] == 2020]
        divorce_summary = df_2020.groupby('tipo_divorcio')['numero_divorcios'].sum().sort_values(ascending=False)
        print(divorce_summary)
        
        # Display top communities by total divorces
        print(f"\nTop communities by total divorces (2020 data):")
        df_2020_total = df_2020[df_2020['tipo_divorcio'] == 'Total'].sort_values('numero_divorcios', ascending=False)
        print(df_2020_total[['comunidad_autonoma', 'numero_divorcios']].head(10))
        
        return processed_df
        
    except FileNotFoundError:
        print(f"❌ Error: Input file not found: {input_file}")
        print("Please make sure the extraction script has been run first.")
        return None
    except Exception as e:
        print(f"❌ Error processing data: {e}")
        return None

def main():
    """
    Main function to run the data processing
    """
    processed_df = process_divorces_data()
    
    if processed_df is not None:
        print("\n🎉 Data processing completed successfully!")
        print("The processed data is now ready for analysis with key structure: (year, comunidad_autonoma, tipo_divorcio)")
    else:
        print("\n💥 Data processing failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
