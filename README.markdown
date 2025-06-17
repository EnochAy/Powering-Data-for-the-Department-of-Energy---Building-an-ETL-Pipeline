Energy Data ETL Pipeline
Overview
This project implements an ETL (Extract, Transform, Load) pipeline to automate the processing of electricity sales and capability data for an energy company. The pipeline replaces a manual quarterly process, enabling monthly data processing to provide rapid insights into electricity sales for the residential and transportation sectors. Built with Python and pandas, it processes two datasets: electricity_sales.csv and electricity_capability_nested.json. The pipeline is designed for DataCamp’s DataLab but can run in any Python environment.
Prerequisites

Python: Version 3.8 or higher
Libraries:
pandas (for data processing)
pyarrow (for Parquet file support)


Input Files (must be in the working directory):
electricity_sales.csv: CSV file with electricity sales data.
electricity_capability_nested.json: JSON file with nested capability data.


Environment: Compatible with DataCamp DataLab or local Python environments (e.g., Jupyter Notebook, VS Code).

Install dependencies:
pip install pandas pyarrow

Project Structure

etl_pipeline.py: Core ETL pipeline script with extraction, transformation, and loading functions.
electricity_sales.csv: Input CSV file (required).
electricity_capability_nested.json: Input JSON file (required).
loaded__electricity_sales.csv: Output file for transformed sales data.
loaded__electricity_capability.parquet: Output file for flattened capability data.
README.md: This file.

Usage

Ensure Input Files:

Place electricity_sales.csv and electricity_capability_nested.json in the working directory (e.g., C:\Users\ayomi\Documents\Powering Data for the Department of Energy - Building an ETL Pipeline\ for local execution, or the DataLab project directory).
In DataCamp DataLab, verify that these files are uploaded to your project workspace.


Run the Pipeline:Execute the script in etl_pipeline.py:
import etl_pipeline

# Extract
raw_electricity_capability_df = etl_pipeline.extract_json_data("electricity_capability_nested.json")
raw_electricity_sales_df = etl_pipeline.extract_tabular_data("electricity_sales.csv")

# Transform
cleaned_electricity_sales_df = etl_pipeline.transform_electricity_sales_data(raw_electricity_sales_df)

# Load
etl_pipeline.load(raw_electricity_capability_df, "loaded__electricity_capability.parquet")
etl_pipeline.load(cleaned_electricity_sales_df, "loaded__electricity_sales.csv")


Outputs:

loaded__electricity_sales.csv: Transformed sales data with columns year, month, stateid, price, price-units.
loaded__electricity_capability.parquet: Flattened capability data in Parquet format.



Functions

extract_tabular_data(file_path: str) -> pd.DataFrame:
Reads .csv or .parquet files.
Validates file extensions and input type.


extract_json_data(file_path: str) -> pd.DataFrame:
Reads and flattens nested JSON using json_normalize().
Handles JSON parsing errors.


transform_electricity_sales_data(raw_data: pd.DataFrame) -> pd.DataFrame:
Drops rows with NA price values.
Filters for sectorName = "residential" or "transportation".
Creates month (first 4 chars of period) and year (last 2 chars of period) columns.
Returns DataFrame with year, month, stateid, price, price-units.


load(dataframe: pd.DataFrame, file_path: str) -> None:
Saves DataFrames to .csv or .parquet.
Validates file extensions and input types.



Data Dictionary
electricity_sales.csv



Field
Data Type
Description



period
str
Time period (e.g., 202301 for Jan 2023)


stateid
str
State identifier (e.g., CA, NY)


stateDescription
str
Full state name


sectorid
str
Sector identifier


sectorName
str
Sector name (e.g., residential)


price
float
Electricity price


price-units
str
Units of price (e.g., cents per kWh)


electricity_capability_nested.json

Nested JSON, flattened into a DataFrame.
Specific fields depend on the JSON structure (not provided).

Troubleshooting

FileNotFoundError (e.g., No such file or directory: 'electricity_capability_nested.json'):
Cause: The file is missing or not in the working directory.
Solution:
Verify electricity_capability_nested.json and electricity_sales.csv are in the correct directory (C:\Users\ayomi\Documents\Powering Data for the Department of Energy - Building an ETL Pipeline\ for local runs, or DataLab’s workspace).
In DataCamp DataLab, upload files via the "Data" tab in your project (https://www.datacamp.com/datalab/w/af61b4ce-0db9-45df-919d-4a899177dae2/edit).
Check file names for typos or case sensitivity.




KeyError: Ensure electricity_sales.csv has required columns (period, stateid, sectorName, price, price-units).
JSON Errors: Verify electricity_capability_nested.json is valid JSON.
Contact: For DataLab issues, use DataCamp’s support. For local issues, check file paths and permissions.

Notes

Error Handling: The pipeline validates inputs, file extensions, and columns to prevent errors like NoneType.
DataLab: Optimized for DataCamp DataLab but portable to local environments.
Scalability: Suitable for monthly runs, with potential for scheduling via Airflow.

Future Improvements

Add transformations for electricity_capability_nested.json.
Integrate with a database for querying (e.g., SQL in DataLab).
Automate monthly execution with a scheduler.

