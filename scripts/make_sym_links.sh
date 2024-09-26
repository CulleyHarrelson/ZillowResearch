#!/bin/bash

# Directory containing the data files
DATA_DIR="../data"

# Directory where symlinks will be created
SEEDS_DIR="."

# Create symlinks
ln -sf "$DATA_DIR/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_month.csv" "$SEEDS_DIR/region_home_values_raw.csv"
ln -sf "$DATA_DIR/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv" "$SEEDS_DIR/region_home_values_smooth.csv"
ln -sf "$DATA_DIR/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_month.csv" "$SEEDS_DIR/region_home_values_tiers.csv"
ln -sf "$DATA_DIR/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_month.csv" "$SEEDS_DIR/region_home_values_property_types.csv"
ln -sf "$DATA_DIR/Metro_zhvf_growth_uc_sfrcondo_tier_0.33_0.67_month.csv" "$SEEDS_DIR/region_home_value_forecasts_raw.csv"
ln -sf "$DATA_DIR/Metro_zhvf_growth_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv" "$SEEDS_DIR/region_home_value_forecasts_smooth.csv"
ln -sf "$DATA_DIR/Metro_zori_uc_sfrcondomfr_sm_month.csv" "$SEEDS_DIR/region_rentals.csv"
ln -sf "$DATA_DIR/Metro_zordi_uc_sfrcondomfr_month.csv" "$SEEDS_DIR/region_renter_demand.csv"

echo "Symlinks created successfully in $SEEDS_DIR"
