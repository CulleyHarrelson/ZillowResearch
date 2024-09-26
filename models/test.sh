#!/bin/bash

# List of files to create pivot versions for
files=(
    "int_region_home_value_forecasts_raw.sql"
    "int_region_home_value_forecasts_smooth.sql"
    "int_region_home_values_property_types.sql"
    "int_region_home_values_raw.sql"
    "int_region_home_values_smooth.sql"
    "int_region_home_values_tiers.sql"
    "int_region_rentals.sql"
    "int_region_renter_demand.sql"
)

# Loop through each file
for file in "${files[@]}"; do
    # Create the new filename with 'pivot' inserted after 'int_'
    new_file="int_pivot_${file#int_}"

    # Create the new file and add a basic SQL structure
    cat <<EOF >"$new_file"
-- Pivot file for $file
WITH source_data AS (
    SELECT * FROM {{ ref('${file%.sql}') }}
),

pivoted_data AS (
    SELECT
        -- Add your pivot logic here
        -- Example:
        -- region_id,
        -- date,
        -- MAX(CASE WHEN metric_type = 'type1' THEN value END) AS type1_value,
        -- MAX(CASE WHEN metric_type = 'type2' THEN value END) AS type2_value
    FROM source_data
    -- Add GROUP BY clause if needed
)

SELECT * FROM pivoted_data
EOF

    echo "Created $new_file"
done

echo "All pivot files have been created."
