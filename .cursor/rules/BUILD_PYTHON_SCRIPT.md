# Cursor Rules

## INE API Documentation (Instituto Nacional de Estadística)

### Base URL
```
https://servicios.ine.es/wstempus/js/{idioma}/{función}/{input}[?parámetros]
```
- `{idioma}`: Language code (ES: Spanish, EN: English)
- `{función}`: API function (e.g., DATOS_TABLA, DATOS_SERIE)
- `{input}`: Table/series/operation identifier
- `[?parámetros]`: Optional parameters (e.g., nult, det, tip, date)

### Main Endpoints
- **DATOS_TABLA**: Get data for a table. Example: `/DATOS_TABLA/50902?nult=1&tip=AM`
- **DATOS_SERIE**: Get data for a series. Example: `/DATOS_SERIE/IPC251856?nult=1&tip=AM`
- **DATOS_METADATAOPERACION**: Get data for all series in an operation with filters.
- **PERIODICIDADES**: List available periodicities.
- **PUBLICACIONES**: List available publications.
- **CLASIFICACIONES**: List available classifications.
- **VALORES_HIJOS**: Get child values for a variable/value.

### Parameters
- `nult`: Number of last periods
- `det`: Detail level (0, 1, 2)
- `tip`: Output format ('A' = friendly, 'M' = metadata, 'AM' = both)
- `date`: Date range (e.g., `date=20240101:20241231`)

### How to Obtain Table/Series Identifiers
- **Tempus3 Tables**: Use the `t` parameter from the table URL (e.g., `t=50902`)
- **PC-Axis Tables**: Concatenate `path` and `file` from the URL (e.g., `t20/e245/p08/l0/01001.px`)
- **TPX Tables**: Use the `tpx` parameter from the URL (e.g., `tpx=33387`)
- **Series Identifiers**: Only for Tempus3; found in the table data view

### Example: Downloading Data
To download all data for a table (e.g., 24082):
```
GET https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/24082?tip=A&det=2
```

### Notes
- All dates are in YYYYMMDD format
- Multiple parameters are separated by `&`
- Some endpoints require specific codes, which can be found via INEbase navigation

---

## Step-by-step: From INE Table URL to Python Download Script

If a user provides an INE table URL (e.g., https://www.ine.es/jaxiT3/Tabla.htm?t=24082&L=0), follow these steps:

1. **Extract the Table Identifier**
   - For Tempus3 tables, get the value of the `t` parameter from the URL (e.g., `t=24082`).
   - For PC-Axis tables, concatenate `path` and `file` parameters (e.g., `t20/p274/serie/def/p02/02017.px`).
2. **Build the API Endpoint**
   - Use the identifier in the API endpoint: `https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/{table_id}?tip=A&det=2`
3. **Generate Python Code**
   - Write a Python script in the `etl/extraction_etls/` directory that:
     - Fetches the data from the API endpoint
     - Processes the response (list of series, each with its own data array)
     - Flattens the data into a DataFrame
     - Saves the data as a CSV file in the `extraction_folder/` directory
4. **File Organization**
   - Scripts go in: `etl/extraction_etls/extraction_{table_name}.py`
   - CSV outputs go in: `extraction_folder/{table_name}.csv`
   - Use fixed filenames (no timestamps) to override existing files
5. **Dependencies**
   - The script should use `requests` and `pandas` (add to `requirements.txt` if needed).
6. **Instructions**
   - Provide instructions to install dependencies and run the script from the `etl/extraction_etls/` directory.
7. **Update Documentation**
   - After creating a new extraction ETL, update the `README.md` file to include the new ETL in the "ETLs Implementados" section.
   - Follow the existing format with: source, processing description, final structure, period, and records count.

--- 