# Energy Data ETL Pipeline

## Overview
This project implements an ETL (Extract, Transform, Load) pipeline to automate the processing of electricity sales and capability data for an energy company. Previously, analysts manually retrieved and cleaned data quarterly, a time-consuming and error-prone process. This pipeline automates data extraction, transformation, and loading on a monthly basis, enabling rapid insights into electricity sales for the residential and transportation sectors.

The pipeline processes two datasets:
- `electricity_sales.csv`: Contains electricity sales data with fields like `period`, `stateid`, `sectorName`, `price`, and `price-units`.
- `electricity_capability_nested.json`: Contains nested JSON data on electricity capability, flattened for further analysis.

The pipeline is built using Python and the pandas library, designed to run in environments like DataCamp’s DataLab or any Python-enabled system.

## Prerequisites
- **Python**: Version 3.8 or higher
- **Libraries**:
  - `pandas` (for data processing)
  - `pyarrow` (for Parquet file support)
- **Input Files**:
  - `electricity_sales.csv`
  - `electricity_capability_nested.json`
- **Environment**: Compatible with DataCamp DataLab or local Python environments (e.g., Jupyter Notebook, VS Code).

Install dependencies:
```bash
pip install pandas pyarrow
```

## Project Structure
- `etl_pipeline.py`: Core Python script containing ETL functions.
- `electricity_sales.csv`: Input CSV file with electricity sales data.
- `electricity_capability_nested.json`: Input JSON file with capability data.
- `loaded__electricity_sales.csv`: Output file for transformed sales data.
- `loaded__electricity_capability.parquet`: Output file for flattened capability data.
- `README.md`: This file.

## Usage
1. **Place Input Files**:
   - Ensure `electricity_sales.csv` and `electricity_capability_nested.json` are in the working directory.

2. **Run the Pipeline**:
   Execute the following Python script to process the data:
   ```python
   import etl_pipeline

   # Extract
   raw_electricity_capability_df = etl_pipeline.extract_json_data("electricity_capability_nested.json")
   raw_electricity_sales_df = etl_pipeline.extract_tabular_data("electricity_sales.csv")

   # Transform
   cleaned_electricity_sales_df = etl_pipeline.transform_electricity_sales_data(raw_electricity_sales_df)

   # Load
   etl_pipeline.load(raw_electricity_capability_df, "loaded__electricity_capability.parquet")
   etl_pipeline.load(cleaned_electricity_sales_df, "loaded__electricity_sales.csv")
   ```

3. **Outputs**:
   - `loaded__electricity_sales.csv`: Transformed sales data with columns `year`, `month`, `stateid`, `price`, `price-units`.
   - `loaded__electricity_capability.parquet`: Flattened capability data in Parquet format.

## Functions
- **`extract_tabular_data(file_path: str) -> pd.DataFrame`**:
  - Extracts data from `.csv` or `.parquet` files using pandas.
  - Raises an error for invalid file extensions.

- **`extract_json_data(file_path: str) -> pd.DataFrame`**:
  - Reads and flattens nested JSON data using `pandas.json_normalize()`.
  - Returns a DataFrame with flattened structure.

- **`transform_electricity_sales_data(raw_data: pd.DataFrame) -> pd.DataFrame`**:
  - Processes `electricity_sales.csv` by:
    - Dropping rows with NA values in the `price` column.
    - Filtering for `sectorName` values of "residential" or "transportation".
    - Creating `month` (first 4 characters of `period`) and `year` (last 2 characters of `period`) columns.
    - Returning a DataFrame with `year`, `month`, `stateid`, `price`, `price-units`.

- **`load(dataframe: pd.DataFrame, file_path: str) -> None`**:
  - Saves a DataFrame to `.csv` or `.parquet` files.
  - Raises an error for invalid file extensions.

## Data Dictionary
### electricity_sales.csv
| Field            | Data Type | Description                              |
|------------------|-----------|------------------------------------------|
| period           | str       | Time period (e.g., `202301` for Jan 2023)|
| stateid          | str       | State identifier (e.g., `CA`, `NY`)      |
| stateDescription | str       | Full state name                          |
| sectorid         | str       | Sector identifier                        |
| sectorName       | str       | Sector name (e.g., `residential`)        |
| price            | float     | Electricity price                        |
| price-units      | str       | Units of price (e.g., `cents per kWh`)   |

### electricity_capability_nested.json
- Nested JSON structure, flattened into a DataFrame using `json_normalize()`.
- Specific fields depend on the JSON structure (not provided).

## Notes
- **Error Handling**: The pipeline includes robust checks for file extensions, input types, and required columns to prevent errors like `NoneType` returns.
- **Scalability**: Designed for monthly execution, potentially integrable with scheduling tools like Apache Airflow.
- **DataLab**: Developed in DataCamp’s DataLab, which supports AI-assisted coding and data analysis.[](https://datalab-docs.datacamp.com)

## Troubleshooting
- **FileNotFoundError**: Ensure input files are in the working directory.
- **KeyError**: Verify `electricity_sales.csv` contains required columns (`period`, `stateid`, `sectorName`, `price`, `price-units`).
- **JSON Errors**: Ensure `electricity_capability_nested.json` is valid JSON.
- **Contact**: For issues, reach out via DataCamp’s support channels or your project supervisor.

## Future Improvements
- Add transformations for `electricity_capability_nested.json` if specific requirements are provided.
- Integrate with a database for querying (e.g., SQL Server, supported by DataLab).[](https://datalab-docs.datacamp.com/connect-to-data/connect-your-data-to-workspace)
- Schedule monthly runs using a workflow orchestrator.