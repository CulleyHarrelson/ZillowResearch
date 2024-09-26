#!/bin/bash

# Array of input files
input_files=(
    "int_pivot_region_home_value_forecasts_raw.sql"
    "int_pivot_region_home_value_forecasts_smooth.sql"
    "int_pivot_region_home_values_property_types.sql"
    "int_pivot_region_home_values_smooth.sql"
    "int_pivot_region_home_values_tiers.sql"
    "int_pivot_region_rentals.sql"
    "int_pivot_region_renter_demand.sql"
)

# Function to generate SQL content
generate_sql_content() {
    local table_name=$1
    echo "{{ config(materialized='table') }}"
    echo
    echo
    echo "{{ unpivot_values(table_name='$table_name') }}"
}

# Loop through input files and generate corresponding SQL files
for input_file in "${input_files[@]}"; do
    # Extract the table name from the input file name
    table_name=$(echo "$input_file" | sed 's/int_pivot_/int_/' | sed 's/\.sql$//')

    # Generate the output file name
    output_file="$input_file"

    # Generate SQL content and write to output file
    generate_sql_content "$table_name" >"$output_file"

    echo "Generated $output_file"
done

echo "SQL file generation complete."
