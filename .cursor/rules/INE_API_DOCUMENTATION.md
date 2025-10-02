# INE (Instituto Nacional de Estadística) API Documentation

## Base URL
```
https://servicios.ine.es/wstempus/js/{idioma}/{función}/{input}[?parámetros]
```

## Parameters
- `{idioma}`: Language code (ES: Spanish, EN: English)
- `{función}`: Function to be called
- `{input}`: Input identifiers for the function
- `[?parámetros]`: Optional parameters (separated by & if multiple)

## Available Functions

### 1. DATOS_TABLA
Get data for a specific table.

**Input**: Table identifier code

**Parameters**:
- `nult`: Return the last n data points or periods
- `det`: Detail level (0, 1, or 2)
- `tip`: Response format ('A' for friendly, 'M' for metadata, 'AM' for both)
- `tv`: Filter parameter (format: _tv=id_variable:id_valor_)
- `date`: Get data between dates (format: date=aaaammdd:aaaammdd)

**Example**:
```
https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/50902?nult=1&tip=AM
```

### 2. DATOS_SERIE
Get data for a specific series.

**Input**: Series identifier code

**Parameters**:
- `nult`: Return the last n data points or periods
- `det`: Detail level (0, 1, or 2)
- `tip`: Response format ('A' for friendly, 'M' for metadata, 'AM' for both)
- `date`: Get data between dates (format: date=aaaammdd:aaaammdd)

**Example**:
```
https://servicios.ine.es/wstempus/js/ES/DATOS_SERIE/IPC251856?nult=1&tip=AM
```

### 3. DATOS_METADATAOPERACION
Get data for series belonging to a specific operation using filters.

**Input**: Operation identifier code

**Parameters**:
- `p`: Periodicity ID of the series
- `g1`, `g2`, `g3`: Group filters
- `tip`: Response format ('A' for friendly, 'M' for metadata, 'AM' for both)

**Example**:
```
https://servicios.ine.es/wstempus/js/ES/DATOS_METADATAOPERACION/IPC?g1=115:29&g2=3:84&g3=762:&p=1&tip=AM
```

### 4. PERIODICIDADES
Get available periodicities.

**Input**: None

**Parameters**: None

**Example**:
```
https://servicios.ine.es/wstempus/js/ES/PERIODICIDADES
```

### 5. PUBLICACIONES
Get available publications.

**Input**: None

**Parameters**:
- `det`: Detail level (0, 1, or 2)
- `tip`: Response format ('A' for friendly)

**Example**:
```
https://servicios.ine.es/wstempus/js/ES/PUBLICACIONES?det=2&tip=A
```

### 6. PUBLICACIONES_OPERACION
Get all publications for a specific operation.

**Input**: Operation identifier code

**Parameters**:
- `det`: Detail level (0, 1, or 2)
- `tip`: Response format ('A' for friendly)

**Example**:
```
https://servicios.ine.es/wstempus/js/ES/PUBLICACIONES_OPERACION/IPC?det=2&tip=A
```

### 7. PUBLICACIONFECHA_PUBLICACION
Get publication dates for a specific publication.

**Input**: Publication identifier code

**Parameters**:
- `det`: Detail level (0, 1, or 2)
- `tip`: Response format ('A' for friendly)

**Example**:
```
https://servicios.ine.es/wstempus/js/ES/PUBLICACIONFECHA_PUBLICACION/8?det=2&tip=A
```

### 8. CLASIFICACIONES
Get all available classifications.

**Input**: None

**Parameters**: None

**Example**:
```
https://servicios.ine.es/wstempus/js/ES/CLASIFICACIONES
```

### 9. CLASIFICACIONES_OPERACION
Get all classifications for a specific operation.

**Input**: Operation identifier code

**Parameters**: None

**Example**:
```
https://servicios.ine.es/wstempus/js/ES/CLASIFICACIONES_OPERACION/25
```

### 10. VALORES_HIJOS
Get child values of a parent value within a hierarchical structure.

**Input**: Variable and value identifier codes

**Parameters**:
- `det`: Detail level (0, 1, or 2)

**Example**:
```
https://servicios.ine.es/wstempus/js/ES/VALORES_HIJOS/70/8997?det=2
```

## Response Formats
- Friendly format ('A'): More readable output
- Metadata format ('M'): Includes metadata information
- Combined format ('AM'): Both friendly and metadata formats

## Detail Levels
- 0: Basic information
- 1: Standard detail
- 2: Maximum detail

## Notes
- All dates should be in the format YYYYMMDD
- Multiple parameters should be separated by &
- The API supports both Spanish (ES) and English (EN) languages
- Some endpoints may require specific identifier codes that can be obtained from other API calls

## Obtaining Table and Series Identifiers

### Table Identifiers
Table identifiers can be obtained by navigating through INEbase. The identifier format depends on the data source:

1. **Tempus3 Tables**
   - Example: "Índices nacionales: general y de grupos ECOICOP"
   - URL: https://www.ine.es/jaxiT3/Tabla.htm?t=50902
   - Identifier: Use the `t` parameter from the URL (e.g., 50902)

2. **PC-Axis Tables**
   - Example: "Población por edad (3 grupos de edad), Españoles/Extranjeros, Sexo y Año"
   - URL: https://www.ine.es/jaxi/Tabla.htm?path=/t20/e245/p08/l0/&file=01001.px
   - Identifier: Concatenate the `path` and `file` parameters (e.g., t20/e245/p08/l0/01001.px)

3. **TPX Tables**
   - Example: "Extracción nacional por tipo de material y años"
   - URL: https://www.ine.es/jaxi/Tabla.htm?tpx=33387&L=0
   - Identifier: Use the `tpx` parameter from the URL (e.g., 33387)

### Series Identifiers (Tempus3 Only)
Series identifiers are only available for data from Tempus3 sources. To obtain a series identifier:

1. Navigate to the table containing the desired series in INEbase
2. Example: For "variación anual del Índice de Precios de Consumo general"
   - Go to table: "Índices nacionales: general y de grupos ECOICOP"
   - URL: https://www.ine.es/jaxiT3/Datos.htm?t=50902
   - The series identifier will be available in the table data

Note: Series identifiers are only available for Tempus3 data sources, not for PC-Axis or TPX tables. 