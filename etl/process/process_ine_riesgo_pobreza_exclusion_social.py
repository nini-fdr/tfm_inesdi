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
    
    print("INE Poverty Risk Data Processing Script")
    print("=" * 50)
    print(f"Input file: {input_file}")
    print(f"Output file: {output_path}")
    print("=" * 50)
    
    try:
        # Read the raw data
        print("Reading raw data...")
        df = pd.read_csv(input_file)
        print(f"Loaded {len(df)} records")
        
        # Extract community name from series_name
        print("Extracting community names...")
        df['comunidad_autonoma'] = df['series_name'].apply(extract_community_name)
        
        # Filter out records where we couldn't extract community name
        initial_count = len(df)
        df = df.dropna(subset=['comunidad_autonoma'])
        filtered_count = len(df)
        print(f"Filtered out {initial_count - filtered_count} records with missing community info")
        
        # Standardize community names using the mapping
        print("Standardizing community names...")
        df = apply_community_mapping(df, 'comunidad_autonoma')
        print(f"After standardization: {len(df)} records")
        
        # Create the final processed DataFrame
        print("Creating processed data...")
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
        
        # Display summary by community (2024 data)
        print(f"\nSummary by community (2024 data):")
        df_2024 = processed_df[processed_df['year'] == 2024].sort_values('tasa_arope', ascending=False)
        print(df_2024[['comunidad_autonoma', 'tasa_arope']].head(10))
        
        # Display summary statistics
        print(f"\nAROPE Statistics (2024 data):")
        print(f"  - Average: {df_2024['tasa_arope'].mean():.2f}%")
        print(f"  - Median: {df_2024['tasa_arope'].median():.2f}%")
        print(f"  - Min: {df_2024['tasa_arope'].min():.2f}%")
        print(f"  - Max: {df_2024['tasa_arope'].max():.2f}%")
        
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
    processed_df = process_data()
    
    if processed_df is not None:
        print("\n🎉 Data processing completed successfully!")
        print("The processed data is now ready for analysis with key structure: (year, comunidad_autonoma)")
    else:
        print("\n💥 Data processing failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
