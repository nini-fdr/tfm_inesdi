#!/usr/bin/env python3
"""
Utilities for ETL processing scripts
Contains standardized mappings and helper functions
"""

import numpy as np
import pandas as pd

# Standardized mapping for Spanish Autonomous Communities
CCAA_MAP = {
    # Andalucía / Aragón
    'Andalucía': 'Andalucía',
    'Aragón': 'Aragón',

    # Asturias
    'Asturias': 'Asturias',
    'Principado de Asturias': 'Asturias',
    'Asturias, Principado de': 'Asturias',

    # Illes Balears
    'Illes Balears': 'Illes Balears',
    'Balears, Illes': 'Illes Balears',
    'Balears': 'Illes Balears',

    # Canarias / Cantabria
    'Canarias': 'Canarias',
    'Cantabria': 'Cantabria',

    # Castilla-La Mancha (varios tipos de guion)
    'Castilla - La Mancha': 'Castilla-La Mancha',
    'Castilla – La Mancha': 'Castilla-La Mancha',  # guion en
    'Castilla — La Mancha': 'Castilla-La Mancha',  # em dash

    # Castilla y León
    'Castilla y León': 'Castilla y León',

    # Cataluña
    'Cataluña': 'Cataluña',

    # Ceuta / Melilla
    'Ceuta': 'Ceuta',
    'Melilla': 'Melilla',

    # Comunidad de Madrid (formas invertidas o abreviadas)
    'Comunidad de Madrid': 'Comunidad de Madrid',
    'Madrid, Comunidad de': 'Comunidad de Madrid',
    'Madrid': 'Comunidad de Madrid',

    # Comunitat Valenciana
    'Comunitat Valenciana': 'Comunitat Valenciana',

    # Extremadura / Galicia
    'Extremadura': 'Extremadura',
    'Galicia': 'Galicia',

    # La Rioja (invertida/abreviada)
    'La Rioja': 'La Rioja',
    'Rioja, La': 'La Rioja',
    'Rioja': 'La Rioja',

    # Región de Murcia (formas)
    'Región de Murcia': 'Región de Murcia',
    'Murcia, Región de': 'Región de Murcia',
    'Murcia': 'Región de Murcia',

    # Navarra (Comunidad Foral)
    'Comunidad Foral de Navarra': 'Navarra',
    'Navarra, Comunidad Foral de': 'Navarra',
    'Navarra': 'Navarra',

    # País Vasco
    'País Vasco': 'País Vasco',

    # Total nacional (si lo quieres excluir, lo dejamos en NaN)
    'Total Nacional': np.nan,
}

def standardize_community_name(community_name):
    """
    Standardize community name using the CCAA_MAP dictionary.
    
    Args:
        community_name (str): Original community name
        
    Returns:
        str or np.nan: Standardized community name or NaN if Total Nacional
    """
    if pd.isna(community_name):
        return np.nan
    
    # Try exact match first
    if community_name in CCAA_MAP:
        return CCAA_MAP[community_name]
    
    # Try case-insensitive match
    for key, value in CCAA_MAP.items():
        if community_name.lower() == key.lower():
            return value
    
    # If no match found, return original name (for debugging)
    print(f"Warning: Community name '{community_name}' not found in CCAA_MAP")
    return community_name

def apply_community_mapping(df, community_column='comunidad_autonoma'):
    """
    Apply community name standardization to a DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame with community names
        community_column (str): Name of the column containing community names
        
    Returns:
        pd.DataFrame: DataFrame with standardized community names
    """
    df_copy = df.copy()
    df_copy[community_column] = df_copy[community_column].apply(standardize_community_name)
    
    # Remove rows with NaN community names (Total Nacional, etc.)
    df_copy = df_copy.dropna(subset=[community_column])
    
    return df_copy
