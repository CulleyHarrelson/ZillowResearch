#!/bin/bash

# Change to the data directory
cd ../data/symlinks || exit 1

# Create symlinks
ln -s ../Metro_zhvf_growth_uc_sfrcondo_tier_0.33_0.67_month.csv region_home_value_forecasts_raw.csv
ln -s ../Metro_zhvf_growth_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv region_home_value_forecasts_smooth.csv
ln -s ../Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_month.csv region_home_values_raw.csv
ln -s ../Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv region_home_values_smooth.csv
ln -s ../Metro_zordi_uc_sfrcondomfr_month.csv region_renter_demand.csv
ln -s ../Metro_zori_uc_sfrcondomfr_sm_month.csv region_rentals.csv

echo "Symlink creation complete."
