# ETL Processing Guide

## Overview
This guide provides step-by-step instructions for creating ETL (Extract, Transform, Load) scripts that move data from `extraction_folder` to `processed_folder` with clean, structured formats.

## Project Structure
```
TFM/
├── etl/
│   ├── extraction_etls/          # Scripts for data extraction from INE API
│   └── process/                  # Scripts for data transformation
├── extraction_folder/            # Raw data from INE API
├── processed_folder/             # Clean, structured data
└── .cursor/rules/               # Documentation and rules
```

## Step-by-Step ETL Process

### 1. Data Extraction (Already Done)
- Scripts in `etl/extraction_etls/` download data from INE API
- Raw data saved to `extraction_folder/` as CSV files
- Format: `ine_{table_description}.csv`

### 2. Data Processing (New Scripts)
When creating a new processing script, follow these steps:

#### Step 1: Create Processing Script
- **Location**: `etl/process/process_ine_{table_name}.py`
- **Naming convention**: `process_ine_{table_name}.py`

#### Step 2: Define File Paths
```python
# Define file paths
input_file = "../../extraction_folder/ine_{table_name}.csv"
output_dir = "../../processed_folder"
output_file = "ine_{table_name}_processed.csv"
output_path = os.path.join(output_dir, output_file)
```

#### Step 3: Data Transformation Logic
The script should include:

1. **Data Reading**:
   ```python
   df = pd.read_csv(input_file)
   ```

2. **Data Parsing**:
   - Extract relevant information from `series_name` column
   - Parse community names, categories, types, etc.
   - Handle missing or malformed data

3. **Data Restructuring**:
   - Create pivot tables with appropriate keys
   - **Standard Key Structure**: `(year, comunidad_autonoma)`
   - Separate columns for different categories/types

4. **Calculated Fields**:
   - Add percentages, ratios, or derived metrics
   - Ensure data consistency and validation

5. **Data Output**:
   ```python
   processed_df.to_csv(output_path, index=False, encoding='utf-8')
   ```

#### Step 4: Standard Output Format
The processed CSV should have:
- **Primary Key**: `(year, comunidad_autonoma)`
- **Columns**: Descriptive names with consistent naming
- **Data Types**: Appropriate numeric types for calculations
- **Encoding**: UTF-8 for Spanish characters

### 3. Script Template Structure

```python
#!/usr/bin/env python3
"""
Script to process INE {table_description} data and transform it into a clean format
Input: extraction_folder/ine_{table_name}.csv
Output: processed_folder/ine_{table_name}_processed.csv
Key structure: (year, comunidad_autonoma)
"""

import pandas as pd
import os
import sys
from datetime import datetime

def extract_information(series_name):
    """
    Extract relevant information from series name
    Customize based on the specific data structure
    """
    # Implementation specific to the data
    pass

def process_data():
    """
    Main processing function
    """
    # Define file paths
    input_file = "../../extraction_folder/ine_{table_name}.csv"
    output_dir = "../../processed_folder"
    output_file = "ine_{table_name}_processed.csv"
    output_path = os.path.join(output_dir, output_file)
    
    try:
        # Read data
        df = pd.read_csv(input_file)
        
        # Transform data
        # ... transformation logic ...
        
        # Save processed data
        os.makedirs(output_dir, exist_ok=True)
        processed_df.to_csv(output_path, index=False, encoding='utf-8')
        
        return processed_df
        
    except Exception as e:
        print(f"❌ Error processing data: {e}")
        return None

def main():
    """
    Main function to run the data processing
    """
    processed_df = process_data()
    
    if processed_df is not None:
        print("🎉 Data processing completed successfully!")
    else:
        print("💥 Data processing failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
```

### 4. Quality Assurance Checklist

Before finalizing any processing script, ensure:

- [ ] **Input validation**: Script handles missing files gracefully
- [ ] **Data integrity**: No data loss during transformation
- [ ] **Output format**: Consistent with project standards
- [ ] **Error handling**: Proper exception handling and user feedback
- [ ] **Documentation**: Clear comments and docstrings
- [ ] **Testing**: Script runs successfully with sample data
- [ ] **File paths**: Correct relative paths from `etl/process/` directory

### 5. Common Transformation Patterns

#### Pattern 1: Pivot by Categories
```python
# For data with multiple categories in series_name
pivot_df = df.pivot_table(
    index=['year', 'comunidad_autonoma'],
    columns='category',
    values='value',
    aggfunc='sum',
    fill_value=0
)
```

#### Pattern 2: Extract from Series Name
```python
def extract_info(series_name):
    parts = series_name.split('. ')
    if len(parts) >= 4:
        community = parts[1].strip()
        category = parts[3].strip()
        return community, category
    return None, None
```

#### Pattern 3: Calculate Percentages
```python
if 'total' in processed_df.columns:
    processed_df['pct_category'] = (
        processed_df['category_value'] / processed_df['total'] * 100
    ).round(2)
```

### 6. Execution Instructions

To run any processing script:

```bash
# Navigate to the process directory
cd etl/process

# Run the specific processing script
python process_ine_{table_name}.py
```

### 7. Expected Output

Each processing script should:
- Print progress information during execution
- Display summary statistics (records, years, communities)
- Show sample data preview
- Confirm successful completion
- Save data to `processed_folder/` with descriptive filename

### 8. Integration with Existing Workflow

1. **Extraction**: Run extraction script first to get raw data
2. **Processing**: Run processing script to transform data
3. **Analysis**: Use processed data for analysis and visualization
4. **Documentation**: Update README.md with the new ETL information

This ensures a clean separation of concerns and maintainable ETL pipeline.

### 9. Update README.md Documentation

After creating any new ETL (extraction or processing), **ALWAYS** update the `README.md` file:

#### For New Extraction ETLs:
- Add a new section in "ETLs Implementados"
- Include: source table, processing description, final structure, period, and records count
- Follow the existing format and numbering

#### For New Processing ETLs:
- Update the corresponding ETL section in "ETLs Implementados"
- Add processing details, transformation logic, and final structure
- Update record counts and any new information

#### Documentation Template:
```markdown
### X. **[ETL Name]**

**Archivo de extracción:** `etl/extraction/extraction_ine_{table_name}.py`
**Archivo de procesamiento:** `etl/process/process_ine_{table_name}.py`

**Fuente:** [Table description and source]

**Procesamiento:**
- **Extracción:** [Extraction details and filters]
- **Transformación:** [Transformation logic and steps]
- **Estructura final:** [Final data structure]
- **Período:** [Year range]
- **Registros:** [Number of records]

**Uso:** [Use cases and applications]
```

---

## Notes
- Always test scripts with sample data before finalizing
- Maintain consistent naming conventions across all scripts
- Document any special handling or assumptions in the code
- Consider data validation and quality checks in processing scripts
