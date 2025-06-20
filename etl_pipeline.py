# ETL Pipeline for Electricity Sales Data
import pandas as pd
import json
from pandas import json_normalize
import os

def extract_tabular_data(file_path: str) -> pd.DataFrame:
    """
    Extract data from a tabular file format, with pandas.
    
    Parameters:
    file_path (str): Path to the input file.
    
    Returns:
    pandas.DataFrame: Data extracted from the file.
    
    Raises:
    Exception: If the file extension is not .csv or .parquet.
    TypeError: If file_path is not a string.
    """
    if not isinstance(file_path, str):
        raise TypeError("file_path must be a string")
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith('.parquet'):
        return pd.read_parquet(file_path)
    else:
        raise Exception("Warning: Invalid file extension. Please try with .csv or .parquet!")

def extract_json_data(file_path: str) -> pd.DataFrame:
    """
    Extract and flatten data from a JSON file.
    
    Parameters:
    file_path (str): Path to the JSON file.
    
    Returns:
    pandas.DataFrame: Flattened DataFrame from JSON data.
    
    Raises:
    Exception: If the file cannot be read or is not valid JSON.
    TypeError: If file_path is not a string.
    """
    if not isinstance(file_path, str):
        raise TypeError("file_path must be a string")
    try:
        with open(file_path, 'r') as file:
            data = pd.read_json(file)
        return json_normalize(data.to_dict(orient='records'))
    except Exception as e:
        raise Exception(f"Error reading JSON file: {str(e)}")

def transform_electricity_sales_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    """
    Transform electricity sales to find the total amount of electricity sold
    in the residential and transportation sectors.
    
    Requirements:
    - Drop any records with NA values in the `price` column. Do this inplace.
    - Only keep records with a `sectorName` of "residential" or "transportation".
    - Create a `month` column using the first 4 characters of the values in `period`.
    - Create a `year` column using the last 2 characters of the values in `period`.
    - Return the transformed DataFrame, keeping only the columns `year`, `month`, `stateid`, `price`, and `price-units`.
    
    Parameters:
    raw_data (pandas.DataFrame): Input DataFrame with electricity sales data.
    
    Returns:
    pandas.DataFrame: Transformed DataFrame with specified columns.
    
    Raises:
    KeyError: If required columns are missing.
    TypeError: If input is not a pandas DataFrame.
    """
    if not isinstance(raw_data, pd.DataFrame):
        raise TypeError("raw_data must be a pandas DataFrame")
    
    required_columns = ['period', 'stateid', 'sectorName', 'price', 'price-units']
    if not all(col in raw_data.columns for col in required_columns):
        raise KeyError(f"Missing required columns: {', '.join(set(required_columns) - set(raw_data.columns))}")
    
    # Create a copy to avoid modifying the input DataFrame
    transformed_data = raw_data.copy()
    
    # Drop rows with NA values in the 'price' column inplace
    transformed_data.dropna(subset=['price'], inplace=True)
    
    # Filter for 'residential' or 'transportation' in sectorName
    transformed_data = transformed_data[transformed_data['sectorName'].isin(['residential', 'transportation'])]
    
    # Create 'month' column (first 4 characters of 'period')
    transformed_data['month'] = transformed_data['period'].str[:4]
    
    # Create 'year' column (last 2 characters of 'period')
    transformed_data['year'] = transformed_data['period'].str[-2:]
    
    # Select only the required columns
    transformed_data = transformed_data[['year', 'month', 'stateid', 'price', 'price-units']]
    
    return transformed_data

def load(dataframe: pd.DataFrame, file_path: str) -> None:
    """
    Load a DataFrame to a file in either CSV or Parquet format.
    
    Parameters:
    dataframe (pandas.DataFrame): DataFrame to be saved.
    file_path (str): Path to the output file.
    
    Raises:
    Exception: If the file extension is not .csv or .parquet.
    TypeError: If dataframe is not a pandas DataFrame or file_path is not a string.
    """
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("dataframe must be a pandas DataFrame")
    if not isinstance(file_path, str):
        raise TypeError("file_path must be a string")
    if file_path.endswith('.csv'):
        dataframe.to_csv(file_path, index=False)
    elif file_path.endswith('.parquet'):
        dataframe.to_parquet(file_path, index=False)
    else:
        raise Exception(f"Warning: {file_path} is not a valid file type. Please try again!")
    

# Test scripts of the ETL pipeline functions
raw_electricity_capability_df = extract_json_data("electricity_capability_nested.json")
raw_electricity_sales_df = extract_tabular_data("electricity_sales.csv")
cleaned_electricity_sales_df = transform_electricity_sales_data(raw_electricity_sales_df)
load(raw_electricity_capability_df, "loaded__electricity_capability.parquet")
load(cleaned_electricity_sales_df, "loaded__electricity_sales.csv")

import pandas as pd
import json
from pandas import json_normalize
import os

def extract_tabular_data(file_path: str) -> pd.DataFrame:
    """
    Extract data from a tabular file format, with pandas.
    
    Parameters:
    file_path (str): Path to the input file (e.g., 'electricity_sales.csv').
    
    Returns:
    pandas.DataFrame: Data extracted from the file.
    
    Raises:
    Exception: If the file extension is not .csv or .parquet.
    TypeError: If file_path is not a string.
    FileNotFoundError: If the file is not found in the working directory.
    """
    if not isinstance(file_path, str):
        raise TypeError("file_path must be a string")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}. Ensure it is in the working directory (e.g., 'C:\\Users\\ayomi\\Documents\\Powering Data for the Department of Energy - Building an ETL Pipeline\\').")
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith('.parquet'):
        return pd.read_parquet(file_path)
    else:
        raise Exception("Warning: Invalid file extension. Please try with .csv or .parquet!")

def extract_json_data(file_path: str) -> pd.DataFrame:
    """
    Extract and flatten data from a JSON file.
    
    Parameters:
    file_path (str): Path to the JSON file (e.g., 'electricity_capability_nested.json').
    
    Returns:
    pandas.DataFrame: Flattened DataFrame from JSON data.
    
    Raises:
    Exception: If the file cannot be read or is not valid JSON.
    TypeError: If file_path is not a string.
    FileNotFoundError: If the file is not found in the working directory.
    """
    if not isinstance(file_path, str):
        raise TypeError("file_path must be a string")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}. Ensure it is in the working directory (e.g., 'C:\\Users\\ayomi\\Documents\\Powering Data for the Department of Energy - Building an ETL Pipeline\\').")
    try:
        with open(file_path, 'r') as file:
            data = pd.read_json(file)
        return json_normalize(data.to_dict(orient='records'))
    except Exception as e:
        raise Exception(f"Error reading JSON file: {str(e)}")

def transform_electricity_sales_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    """
    Transform electricity sales to find the total amount of electricity sold
    in the residential and transportation sectors.
    
    Requirements:
    - Drop any records with NA values in the `price` column. Do this inplace.
    - Only keep records with a `sectorName` of "residential" or "transportation".
    - Create a `month` column using the first 4 characters of the values in `period`.
    - Create a `year` column using the last 2 characters of the values in `period`.
    - Return the transformed DataFrame, keeping only the columns `year`, `month`, `stateid`, `price`, and `price-units`.
    
    Parameters:
    raw_data (pandas.DataFrame): Input DataFrame with electricity sales data.
    
    Returns:
    pandas.DataFrame: Transformed DataFrame with specified columns.
    
    Raises:
    KeyError: If required columns are missing.
    TypeError: If input is not a pandas DataFrame.
    """
    if not isinstance(raw_data, pd.DataFrame):
        raise TypeError("raw_data must be a pandas DataFrame")
    
    # Define expected columns and possible variations
    required_columns = ['period', 'stateid', 'sectorName', 'price', 'price-units']
    column_mapping = {
        'Period': 'period',
        'Date': 'period',  # Possible variation from incorrect CSV
        'StateID': 'stateid',
        'StateId': 'stateid',
        'SectorName': 'sectorName',
        'sectorname': 'sectorName',
        'Price': 'price',
        'UnitPrice': 'price',  # Possible variation
        'Price-Units': 'price-units',
        'price_units': 'price-units',
        'unit_price': 'price-units'  # Possible variation
    }
    
    # Rename columns to match expected names
    transformed_data = raw_data.copy()
    for actual_col, expected_col in column_mapping.items():
        if actual_col in transformed_data.columns:
            transformed_data.rename(columns={actual_col: expected_col}, inplace=True)
    
    # Early validation: check if any required columns are present
    if not any(col in transformed_data.columns for col in required_columns):
        raise KeyError(f"No required columns found. Expected: {', '.join(required_columns)}. Actual: {', '.join(raw_data.columns)}. Ensure 'electricity_sales.csv' contains electricity sales data.")
    
    # Check for missing required columns
    if not all(col in transformed_data.columns for col in required_columns):
        missing_cols = [col for col in required_columns if col not in transformed_data.columns]
        raise KeyError(f"Missing required columns: {', '.join(missing_cols)}. Actual columns in CSV: {', '.join(raw_data.columns)}. Ensure 'electricity_sales.csv' matches the expected data dictionary.")
    
    # Drop rows with NA values in the 'price' column inplace
    transformed_data.dropna(subset=['price'], inplace=True)
    
    # Filter for 'residential' or 'transportation' in sectorName
    transformed_data = transformed_data[transformed_data['sectorName'].isin(['residential', 'transportation'])]
    
    # Create 'month' column (first 4 characters of 'period')
    transformed_data['month'] = transformed_data['period'].str[:4]
    
    # Create 'year' column (last 2 characters of 'period')
    transformed_data['year'] = transformed_data['period'].str[-2:]
    
    # Select only the required columns
    transformed_data = transformed_data[['year', 'month', 'stateid', 'price', 'price-units']]
    
    return transformed_data

def load(dataframe: pd.DataFrame, file_path: str) -> None:
    """
    Load a DataFrame to a file in either CSV or Parquet format.
    
    Parameters:
    dataframe (pandas.DataFrame): DataFrame to be saved.
    file_path (str): Path to the output file.
    
    Raises:
    Exception: If the file extension is not .csv or .parquet.
    TypeError: If dataframe is not a pandas DataFrame or file_path is not a string.
    """
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("dataframe must be a pandas DataFrame")
    if not isinstance(file_path, str):
        raise TypeError("file_path must be a string")
    if file_path.endswith('.csv'):
        dataframe.to_csv(file_path, index=False)
    elif file_path.endswith('.parquet'):
        dataframe.to_parquet(file_path, index=False)
    else:
        raise Exception(f"Warning: {file_path} is not a valid file type. Please try again!")

# Test script for the ETL pipeline
if __name__ == "__main__":
    try:
        # Extract
        raw_electricity_capability_df = extract_json_data("electricity_capability_nested.json")
        raw_electricity_sales_df = extract_tabular_data("electricity_sales.csv")

        # Transform
        cleaned_electricity_sales_df = transform_electricity_sales_data(raw_electricity_sales_df)

        # Load
        load(raw_electricity_capability_df, "loaded__electricity_capability.parquet")
        load(cleaned_electricity_sales_df, "loaded__electricity_sales.csv")
        print("ETL pipeline completed successfully!")
    except FileNotFoundError as e:
        print(f"Pipeline failed: {e}")
    except KeyError as e:
        print(f"Pipeline failed due to column mismatch: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")