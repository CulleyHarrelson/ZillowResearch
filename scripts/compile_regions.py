import pandas as pd
import numpy as np
import glob
import os
import re
import pyarrow as pa
import pyarrow.parquet as pq


def normalize_and_uniquify_column_names(df):
    """
    Convert all column names to lowercase and make them unique.
    """
    # Convert to lowercase
    df.columns = df.columns.str.lower()

    # Make column names unique
    seen = {}
    new_columns = []
    for item in df.columns:
        if item not in seen:
            seen[item] = 1
            new_columns.append(item)
        else:
            seen[item] += 1
            new_columns.append(f"{item}_{seen[item]}")

    df.columns = new_columns
    return df


def safe_convert_to_string(value):
    """
    Safely convert a value to string, handling potential NaN values and removing unnecessary decimal places.
    """
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        # Convert to integer if it's a whole number
        if value.is_integer():
            return str(int(value))
        else:
            # Remove trailing zeros after decimal point
            return f"{value:g}"
    return str(value)


def convert_all_columns_to_string(df):
    """
    Convert all columns to string type, handling mixed types.
    """
    for col in df.columns:
        df[col] = df[col].apply(safe_convert_to_string).astype("string")
    return df


def process_home_values(df):
    """
    Process home values dataset to set correct data types, handling NaN and inf values.
    """
    # Handle 'regionid' column
    df["regionid"] = df["regionid"].fillna("0")  # Replace NaN with '0'
    df["regionid"] = df["regionid"].replace(
        [np.inf, -np.inf], "0"
    )  # Replace inf with '0'
    df["regionid"] = (
        df["regionid"].astype(float).round().astype(pd.Int64Dtype()).astype(str)
    )
    df["regionid"] = df["regionid"].replace("0", pd.NA)  # Replace '0' with pandas NA

    # Convert metric_date to datetime, then to date
    df["metric_date"] = pd.to_datetime(df["metric_date"]).dt.date

    # Ensure metric_value is float
    df["metric_value"] = df["metric_value"].astype(float)

    # Explicitly set the dtypes
    df = df.astype(
        {
            "regionid": "string",
            "metric_date": "object",  # date is stored as object
            "metric_value": "float64",
        }
    )

    return df


def extract_regions(df):
    """
    Extract region columns from the dataframe.
    """
    region_columns = [
        "regionid",
        "sizerank",
        "regionname",
        "regiontype",
        "statename",
        "state",
        "metro",
        "countyname",
        "city",
        "statecodefips",
        "municipalcodefips",
    ]
    existing_columns = [col for col in region_columns if col in df.columns]

    if len(existing_columns) != len(set(existing_columns)):
        raise ValueError("Duplicate region columns found")

    return df[existing_columns].drop_duplicates()


def extract_home_values(df):
    """
    Extract and unpivot home value columns, keeping only 'regionid', 'metric_date', and 'metric_value'.
    """
    date_columns = [col for col in df.columns if re.match(r"\d{4}-\d{2}-\d{2}", col)]

    if not date_columns:
        return None

    # Ensure 'regionid' is in lowercase
    if "RegionID" in df.columns:
        df["regionid"] = df["RegionID"]
    elif "regionid" not in df.columns:
        return None  # If neither 'RegionID' nor 'regionid' exists, return None

    melted_df = pd.melt(
        df,
        id_vars=["regionid"],
        value_vars=date_columns,
        var_name="metric_date",
        value_name="metric_value",
    )

    return melted_df[["regionid", "metric_date", "metric_value"]]


# Read all CSV files in a directory
all_files = glob.glob("../data/*.csv")
output_dir = "../data/"

# Initialize empty lists for regions and home values dataframes
regions_list = []
home_values_list = []

for file in all_files:
    try:
        df = pd.read_csv(file, low_memory=False)
        df = normalize_and_uniquify_column_names(df)  # Apply normalization here

        # Extract regions
        regions_df = extract_regions(df)
        regions_list.append(regions_df)

        # Extract and unpivot home values
        home_values_df = extract_home_values(df)
        if home_values_df is not None:
            home_values_list.append(home_values_df)

    except ValueError as ve:
        print(f"Error processing file {file}: {str(ve)}")
    except Exception as e:
        print(f"Unexpected error processing file {file}: {str(e)}")

# Combine all regions dataframes
regions_df = pd.concat(regions_list, axis=0).drop_duplicates()

# Combine all home values dataframes
home_values_df = pd.concat(home_values_list, axis=0)

# Convert all columns to string type for regions dataset
regions_df = convert_all_columns_to_string(regions_df)

# Process home values dataset
home_values_df = process_home_values(home_values_df)

# Print column names for debugging
print("Regions DataFrame columns:")
print(regions_df.columns)
print("\nHome Values DataFrame columns:")
print(home_values_df.columns)

# ... [rest of the script remains unchanged] ...

# Define schemas with lowercase column names
regions_schema = pa.schema(
    [
        ("regionid", pa.string()),
        ("sizerank", pa.string()),
        ("regionname", pa.string()),
        ("regiontype", pa.string()),
        ("statename", pa.string()),
        ("state", pa.string()),
        ("metro", pa.string()),
        ("countyname", pa.string()),
        ("city", pa.string()),
        ("statecodefips", pa.string()),
        ("municipalcodefips", pa.string()),
    ]
)

home_values_schema = pa.schema(
    [
        ("regionid", pa.string()),
        ("metric_date", pa.date32()),
        ("metric_value", pa.float64()),
    ]
)


def convert_to_parquet_table(df, schema):
    # Replace NaN in 'regionid' with a placeholder before conversion
    df["regionid"] = df["regionid"].fillna("NULL")

    # Create a PyArrow Table with the specified schema
    table = pa.Table.from_pandas(df, schema=schema)

    # Convert the regionid column to string type in the PyArrow Table
    regionid_array = table["regionid"].cast(pa.string())
    table = table.set_column(0, "regionid", regionid_array)

    return table


# Convert pandas DataFrames to PyArrow Tables with defined schemas
regions_table = pa.Table.from_pandas(regions_df, schema=regions_schema)
home_values_table = convert_to_parquet_table(home_values_df, home_values_schema)


# Write Parquet files with schemas
pq.write_table(regions_table, os.path.join(output_dir, "regions.parquet"))
pq.write_table(
    home_values_table, os.path.join(output_dir, "regions_home_values.parquet")
)

print(
    f"Regions Parquet file created successfully at {os.path.join(output_dir, 'regions.parquet')}"
)
print(
    f"Home Values Parquet file created successfully at {os.path.join(output_dir, 'regions_home_values.parquet')}"
)

print(f"\nRegions dataset:")
print(f"Total number of columns: {len(regions_df.columns)}")
print(f"Total number of rows: {len(regions_df)}")

print(f"\nHome Values dataset:")
print(f"Total number of columns: {len(home_values_df.columns)}")
print(f"Total number of rows: {len(home_values_df)}")

# Print data types of all columns for both datasets
for dataset_name, df in [("Regions", regions_df), ("Home Values", home_values_df)]:
    print(f"\n{dataset_name} column data types:")
    for col, dtype in df.dtypes.items():
        print(f"{col}: {dtype}")
