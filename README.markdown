# Energy Data ETL Pipeline

## Overview
This project implements an ETL (Extract, Transform, Load) pipeline to automate the processing of electricity sales and capability data for an energy company. It replaces a manual quarterly process, enabling monthly data processing for insights into residential and transportation sector electricity sales. Built with Python and pandas, it processes `electricity_sales.csv` and `electricity_capability_nested.json`. The pipeline is designed for DataCamp’s DataLab but runs locally with proper file setup.

## Prerequisites
- **Python**: Version 3.8 or higher
- **Libraries**:
  - `pandas` (for data processing)
  - `pyarrow` (for Parquet file support)
- **Input Files** (must be in the working directory):
  - `electricity_sales.csv`: CSV file with electricity sales data.
  - `electricity_capability_nested.json`: JSON file with nested capability data.
- **Environment**: DataCamp DataLab or local Python environments (e.g., Jupyter Notebook, VS Code).

Install dependencies:
```bash
pip install pandas pyarrow
```

## Verifying and Handling Incorrect Datasets
If you encounter errors related to missing or incorrect files:
1. **Check File Presence**:
   - Ensure `electricity_sales.csv` and `electricity_capability_nested.json` are in `C:\Users\ayomi\Documents\Powering Data for the Department of Energy - Building an ETL Pipeline\`.
   - Download from DataLab’s "Data" tab: https://www.datacamp.com/datalab/w/af61b4ce-0db9-45df-919d-4a899177dae2/edit
2. **Verify `electricity_sales.csv` Columns**:
   - Run:
     ```python
     import pandas as pd
     df = pd.read_csv("electricity_sales.csv")
     print(df.columns)
     ```
   - Expected columns: `period`, `stateid`, `stateDescription`, `sectorid`, `sectorName`, `price`, `price-units`.
   - If columns differ (e.g., `order_number`, `date`), replace with the correct file or use the sample below.
3. **Create Sample Files**:
   - **electricity_sales.csv**:
     ```csv
     period,stateid,stateDescription,sectorid,sectorName,price,price-units
     202301,CA,California,RES,residential,15.5,cents per kWh
     202301,NY,New York,TRA,transportation,14.2,cents per kWh
     202302,CA,California,COM,commercial,16.0,cents per kWh
     202302,NY,New York,RES,residential,,cents per kWh
     202303,TX,Texas,TRA,transportation,13.8,cents per kWh
     ```
     ```python
     import pandas as pd
     data = {
         "period": ["202301", "202301", "202302", "202302", "202303"],
         "stateid": ["CA", "NY", "CA", "NY", "TX"],
         "stateDescription": ["California", "New York", "California", "New York", "Texas"],
         "sectorid": ["RES", "TRA", "COM", "RES", "TRA"],
         "sectorName": ["residential", "transportation", "commercial", "residential", "transportation"],
         "price": [15.5, 14.2, 16.0, None, 13.8],
         "price-units": ["cents per kWh"] * 5
     }
     pd.DataFrame(data).to_csv("electricity_sales.csv", index=False)
     ```
   - **electricity_capability_nested.json**:
     ```json
     [
         {"plant_id": "P001", "location": {"state": "CA", "city": "Los Angeles"}, "capacity_mw": 100.5, "fuel_type": "Solar"},
         {"plant_id": "P002", "location": {"state": "NY", "city": "Albany"}, "capacity_mw": 200.0, "fuel_type": "Wind"}
     ]
     ```
     ```python
     import json
     data = [
         {"plant_id": "P001", "location": {"state": "CA", "city": "Los Angeles"}, "capacity_mw": 100.5, "fuel_type": "Solar"},
         {"plant_id": "P002", "location": {"state": "NY", "city": "Albany"}, "capacity_mw": 200.0, "fuel_type": "Wind"}
     ]
     with open("electricity_capability_nested.json", "w") as f:
         json.dump(data, f)
     ```

## Handling Column Mismatches
If you encounter a `KeyError` (e.g., `Missing required columns`):
1. **Inspect Columns**: Use the above script to check CSV columns.
2. **Fix Column Names**:
   - The pipeline maps common variations (e.g., `StateID` to `stateid`).
   - If columns are unrelated (e.g., `order_number`), replace the CSV with the correct file or sample.
3. **Contact DataCamp Support**: If the correct `electricity_sales.csv` is missing in DataLab, request assistance.

## Project Structure
- `etl_pipeline.py`: Core ETL script.
- `electricity_sales.csv`: Input CSV file (required).
- `electricity_capability_nested.json`: Input JSON file (required).
- `loaded__electricity_sales.csv`: Output transformed sales data.
- `loaded__electricity_capability.parquet`: Output flattened capability data.
- `README.md`: This file.

## Usage
1. **Place Input Files**:
   - Ensure files are in the working directory.
2. **Run the Pipeline**:
   ```bash
   cd "C:\Users\ayomi\Documents\Powering Data for the Department of Energy - Building an ETL Pipeline"
   python etl_pipeline.py
   ```
   Or in DataLab:
   ```python
   import etl_pipeline
   raw_electricity_capability_df = etl_pipeline.extract_json_data("electricity_capability_nested.json")
   raw_electricity_sales_df = etl_pipeline.extract_tabular_data("electricity_sales.csv")
   cleaned_electricity_sales_df = etl_pipeline.transform_electricity_sales_data(raw_electricity_sales_df)
   etl_pipeline.load(raw_electricity_capability_df, "loaded__electricity_capability.parquet")
   etl_pipeline.load(cleaned_electricity_sales_df, "loaded__electricity_sales.csv")
   ```
3. **Outputs**:
   - `loaded__electricity_sales.csv`: Transformed sales data.
   - `loaded__electricity_capability.parquet`: Flattened capability data.

## Functions
- **`extract_tabular_data(file_path: str) -> pd.DataFrame`**: Reads `.csv` or `.parquet`, validates file existence.
- **`extract_json_data(file_path: str) -> pd.DataFrame`**: Flattens JSON, validates file existence.
- **`transform_electricity_sales_data(raw_data: pd.DataFrame) -> pd.DataFrame`**: Filters sales data, maps column names, creates `month` and `year`.
- **`load(dataframe: pd.DataFrame, file_path: str) -> None`**: Saves to `.csv` or `.parquet`.

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
- Nested JSON, flattened into a DataFrame (structure assumed).

## Troubleshooting
- **FileNotFoundError**: Ensure files are in the working directory or upload to DataLab’s "Data" tab.
- **KeyError**: Verify `electricity_sales.csv` columns; replace with sample if incorrect.
- **JSON Errors**: Ensure `electricity_capability_nested.json` is valid JSON.
- **Contact**: Use DataCamp support for DataLab issues or check local file permissions.

## Notes
- **Error Handling**: Validates files and columns with detailed error messages.
- **DataLab**: Optimized for DataLab but portable.
- **Scalability**: Suitable for monthly runs.

## Future Improvements
- Add JSON transformations.
- Integrate with SQL databases.
- Schedule with Airflow.