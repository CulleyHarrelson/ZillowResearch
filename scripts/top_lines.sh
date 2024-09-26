#!/bin/bash

# Set the data directory
data_dir="../data"

# Output file
output_file="zillow_research_smooth_raw_top10.txt"

# Remove the output file if it already exists
rm -f "$output_file"

# List of files
files=(
    "Metro_zhvf_growth_uc_sfrcondo_tier_0.33_0.67_month.csv"
    "Metro_zhvf_growth_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
    "Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_month.csv"
    "Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
    "Metro_zordi_uc_sfrcondomfr_month.csv"
    "Metro_zori_uc_sfrcondomfr_sm_month.csv"
)

# Loop through each file
for file in "${files[@]}"; do
    full_path="$data_dir/$file"
    # Check if the file exists
    if [ -f "$full_path" ]; then
        # Add file name to the output
        echo "File: $file" >>"$output_file"
        echo "----------------------------------------" >>"$output_file"

        # Extract top 10 lines and append to the output file
        head -n 10 "$full_path" >>"$output_file"

        # Add a separator
        echo -e "\n\n" >>"$output_file"
    else
        echo "Warning: File $full_path not found" >&2
    fi
done

echo "Top lines from all files have been combined in $output_file"
