# ETL Pipeline - Instituto Nacional de Estadística (INE)

## Descripción General

Este proyecto implementa un pipeline ETL (Extract, Transform, Load) para la extracción y procesamiento de datos del Instituto Nacional de Estadística de España (INE). El pipeline está diseñado para automatizar la descarga de datos desde la API del INE y transformarlos en formatos estructurados para análisis.

## Estructura del Proyecto

```
TFM/
├── etl/
│   ├── extraction/                    # Scripts de extracción de datos
│   └── process/                       # Scripts de transformación de datos
├── extraction_folder/                 # Datos brutos extraídos de la API
├── processed_folder/                  # Datos procesados y estructurados
├── .cursor/rules/                     # Documentación y reglas del proyecto
└── requirements.txt                   # Dependencias de Python
```

## ETLs Implementados

**Total:** 6 ETLs completos con extracción y procesamiento de datos del INE

### 1. **Parejas por Nacionalidad y Tipo de Unión**

**Archivo de extracción:** `etl/extraction/extraction_ine_parejas_por_nacionalidad_y_tipo_union.py`
**Archivo de procesamiento:** `etl/process/process_ine_parejas_por_nacionalidad_y_tipo_union.py`

**Fuente:** Tabla PC-Axis del INE - Número de parejas por comunidades y ciudades autónomas según nacionalidad de la pareja y tipo de unión

**Procesamiento:**
- **Extracción:** 
  - Descarga todos los tipos de parejas por comunidad autónoma (excluye "Total Nacional")
  - Incluye desglose por **tipo de unión**: "Total (Parejas)", "Pareja casada", "Pareja de hecho"
  - Incluye desglose por **nacionalidad**: "Total (Parejas)", "Ambos españoles", "Ambos extranjeros", "Español con pareja extranjera", "Española con pareja extranjera"
- **Transformación:** 
  - Extrae `comunidad_autonoma`, `tipo_union` y `nacionalidad` del campo `series_name`
  - Aplica mapeo estandarizado de nombres de comunidades usando `utils.py`
  - Estructura final con clave `(year, comunidad_autonoma, tipo_union, nacionalidad, numero_parejas)`
- **Estructura final:** `(year, comunidad_autonoma, tipo_union, nacionalidad, numero_parejas)`
- **Período:** 2014-2020
- **Registros:** 1,987 registros (19 comunidades × 3 tipos de unión × 5 nacionalidades × ~7 años)

**Uso:** Análisis detallado de patrones de formación de parejas por tipo de unión y nacionalidad, comparación entre comunidades autónomas.

---

### 2. **Divorcios por Tipo**

**Archivo de extracción:** `etl/extraction/extraction_ine_divorcios_por_tipo.py`
**Archivo de procesamiento:** `etl/process/process_ine_divorcios_por_tipo.py`

**Fuente:** Tabla Tempus3 del INE - Divorcios según tipo de divorcio

**Procesamiento:**
- **Extracción:** Descarga todos los datos de divorcios por tipo (Total, No Contencioso, Contencioso) por comunidad autónoma
- **Transformación:** 
  - Extrae `comunidad_autonoma` y `tipo_divorcio` del campo `series_name` usando expresiones regulares
  - Mantiene formato largo (long format) con una sola columna para el tipo de divorcio
  - Aplica mapeo estandarizado de nombres de comunidades usando `utils.py`
- **Estructura final:** `(year, comunidad_autonoma, tipo_divorcio, numero_divorcios)`
- **Período:** 2013-2024
- **Registros:** 681 registros (19 comunidades × 3 tipos de divorcio × ~12 años)

**Uso:** Análisis de patrones de divorcio y comparación entre tipos de divorcio por región.

---

### 3. **Salarios - Medias y Percentiles por Sexo y CCAA**

**Archivo de extracción:** `etl/extraction/extraction_ine_salarios_medias_percentiles.py`
**Archivo de procesamiento:** `etl/process/process_ine_salarios_medias_percentiles.py`

**Fuente:** Tabla Tempus3 del INE - Medias y percentiles por sexo y CCAA

**Procesamiento:**
- **Extracción:** 
  - Filtra únicamente datos de "Mujeres" y "Hombres" (excluye "Total")
  - Incluye solo percentiles específicos: Media, Mediana (50), Cuartil 25, Cuartil 75
  - Excluye datos de "Total Nacional"
- **Transformación:**
  - Extrae `sexo`, `comunidad_autonoma` y `medida` del campo `series_name`
  - Estandariza nombres de medidas: Media → media, 50 → mediana, 25 → cuartil_25, 75 → cuartil_75
  - Mantiene formato largo (long format) para facilitar análisis
- **Estructura final:** `(year, comunidad_autonoma, sexo, medida, value)`
- **Período:** 2008-2020
- **Registros:** 2,178 registros (19 comunidades × 2 sexos × 4 medidas × ~14 años)

**Uso:** Análisis de desigualdad salarial por género y región, comparación de distribuciones salariales.

---

### 4. **Tasas de Empleo por Nacionalidad, Sexo y CCAA**

**Archivo de extracción:** `etl/extraction/extraction_ine_tasas_empleo_por_nacionalidad_sexo_ccaa.py`
**Archivo de procesamiento:** `etl/process/process_ine_tasas_empleo_por_nacionalidad_sexo_ccaa.py`

**Fuente:** Tabla Tempus3 del INE - Tasas de empleo por nacionalidad, sexo y comunidad autónoma

**Procesamiento:**
- **Extracción:**
  - Filtra únicamente datos de "Hombres" y "Mujeres" (excluye "Ambos sexos")
  - Excluye datos de "Total Nacional"
  - Excluye desglose por nacionalidad ("Española" y "Extranjera")
  - Incluye solo datos "Total" por comunidad
- **Transformación:**
  - Extrae `genero` y `comunidad_autonoma` del campo `series_name`
  - Procesa información trimestral del campo `quarter` (formato: `{'Codigo': 'II', 'Nombre': 'T2'}`)
  - **Calcula promedio anual** de los 4 trimestres para cada combinación año-comunidad-género
  - Redondea a 2 decimales
- **Estructura final:** `(year, comunidad_autonoma, genero, tasa_promedio_empleo)`
- **Período:** 2002-2025
- **Registros:** 912 registros (19 comunidades × 2 géneros × 24 años)

**Uso:** Análisis de tendencias de empleo por género y región, comparación de tasas de empleo entre comunidades.

---

### 5. **Delitos de Familia y Sexualidad**

**Archivo de extracción:** `etl/extraction/extraction_ine_delitos_familia_sexualidad.py`
**Archivo de procesamiento:** `etl/process/process_ine_delitos_familia_sexualidad.py`

**Fuente:** Tabla TPX del INE - Delitos según tipo por comunidades y ciudades autónomas

**Procesamiento:**
- **Extracción:** 
  - Filtra únicamente delitos relacionados con familia y sexualidad:
    - "8 Contra la libertad e indemnidad sexuales"
    - "12 Contra las relaciones familiares"
    - "12.3 Contra los derechos y deberes familiares"
    - "12.3.1 Quebrantamiento de los deberes de custodia"
    - "12.3.2 Sustracción de menores"
    - "12.3.3 Abandono de familia"
    - "12.99 Otros delitos contra las relaciones familiares"
  - Excluye datos de "Total Nacional"
  - Incluye solo datos por comunidad autónoma
- **Transformación:**
  - Extrae comunidad autónoma y tipo de delito del campo `series_name`
  - Maneja diferentes formatos de nombres de comunidades (con y sin calificadores)
  - Crea estructura final con clave `(year, comunidad_autonoma, tipo_delito, numero_delitos)`
  - Ordena datos por año, comunidad y tipo de delito
- **Estructura final:** `(year, comunidad_autonoma, tipo_delito, numero_delitos)`
- **Período:** 2013-2022
- **Registros:** 1,330 registros (19 comunidades × 7 tipos de delito × 10 años)

**Uso:** Análisis de delitos relacionados con violencia familiar y sexual por región, tendencias temporales de criminalidad familiar.

---

### 6. **Tasa de Riesgo de Pobreza o Exclusión Social (AROPE)**

**Archivo de extracción:** `etl/extraction/extraction_ine_riesgo_pobreza_exclusion_social.py`
**Archivo de procesamiento:** `etl/process/process_ine_riesgo_pobreza_exclusion_social.py`

**Fuente:** Tabla Tempus3 del INE - Tasa de riesgo de pobreza o exclusión social (indicador AROPE) por CCAA

**Procesamiento:**
- **Extracción:** 
  - Descarga únicamente el indicador AROPE por comunidad autónoma (excluye "Total Nacional")
  - Filtra específicamente "Tasa de riesgo de pobreza o exclusión social (indicador AROPE)"
- **Transformación:** 
  - Extrae `comunidad_autonoma` del campo `series_name`
  - Aplica mapeo estandarizado de nombres de comunidades usando `utils.py`
  - Estructura final con clave `(year, comunidad_autonoma, tasa_arope)`
- **Estructura final:** `(year, comunidad_autonoma, tasa_arope)`
- **Período:** 2014-2024
- **Registros:** 209 registros (19 comunidades × ~11 años)

**Uso:** Análisis de desigualdad social y riesgo de pobreza por región, seguimiento del objetivo Europa 2030, comparación regional de exclusión social.

---

## Metodología ETL

### Extracción (Extract)
- **API del INE:** Utiliza el endpoint `https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/`
- **Identificadores de tabla:** 
  - Tempus3: Parámetro `t` (ej: `t=28191`)
  - PC-Axis: Concatenación de `path` y `file` (ej: `t20/p274/serie/def/p02/02017.px`)
- **Filtrado:** Aplicación de filtros específicos durante la extracción para optimizar el procesamiento
- **Formato de salida:** CSV con estructura estándar: `series_id`, `series_name`, `year`, `value`

### Transformación (Transform)
- **Parsing de series:** Extracción de información estructurada del campo `series_name`
- **Limpieza de datos:** Manejo de valores nulos y validación de tipos de datos
- **Agregaciones:** Cálculo de promedios anuales para datos trimestrales
- **Estandarización:** Nombres de columnas consistentes y formatos de datos uniformes
- **Estructura de claves:** Implementación de claves compuestas `(year, comunidad_autonoma)` como estándar

### Carga (Load)
- **Formato de salida:** CSV con codificación UTF-8
- **Nomenclatura:** `{tabla}_processed.csv`
- **Validación:** Verificación de integridad de datos y estadísticas de resumen

## Dependencias Técnicas

```python
requests    # Para llamadas HTTP a la API del INE
pandas      # Para manipulación y transformación de datos
```

## Instrucciones de Uso

### Ejecutar Extracción
```bash
cd etl/extraction
python extraction_ine_{nombre_tabla}.py
```

### Ejecutar Procesamiento
```bash
cd etl/process
python process_ine_{nombre_tabla}.py
```

## Características Técnicas

- **Manejo de errores:** Validación de archivos de entrada y manejo de excepciones
- **Logging:** Información detallada del progreso y estadísticas de procesamiento
- **Flexibilidad:** Estructura modular que permite agregar nuevos ETLs fácilmente
- **Documentación:** Código autodocumentado con docstrings y comentarios explicativos

## Resultados y Aplicaciones

Los datos procesados están listos para:
- **Análisis estadístico:** Comparaciones entre comunidades autónomas y tendencias temporales
- **Visualizaciones:** Creación de gráficos y dashboards
- **Modelado:** Desarrollo de modelos predictivos o de clasificación
- **Reportes:** Generación de informes automatizados

## Notas de Implementación

- Todos los scripts implementan el patrón de manejo de errores estándar
- Los archivos de salida sobrescriben versiones anteriores (sin timestamps)
- La estructura de carpetas facilita la organización y mantenimiento del código
- Los filtros aplicados durante la extracción optimizan el rendimiento y reducen el ruido en los datos
