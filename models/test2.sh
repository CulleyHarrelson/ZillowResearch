cp int_pivot_region_home_values_raw.sql int_pivot_region_home_value_forecasts_raw.sql
sed -i '' 's/int_region_home_values_raw/int_region_home_value_forecasts_raw/g' int_pivot_region_home_value_forecasts_raw.sql

cp int_pivot_region_home_values_raw.sql int_pivot_region_home_value_forecasts_smooth.sql
sed -i '' 's/int_region_home_values_raw/int_region_home_value_forecasts_smooth/g' int_pivot_region_home_value_forecasts_smooth.sql

cp int_pivot_region_home_values_raw.sql int_pivot_region_home_values_property_types.sql
sed -i '' 's/int_region_home_values_raw/int_region_home_values_property_types/g' int_pivot_region_home_values_property_types.sql

cp int_pivot_region_home_values_raw.sql int_pivot_region_home_values_smooth.sql
sed -i '' 's/int_region_home_values_raw/int_region_home_values_smooth/g' int_pivot_region_home_values_smooth.sql

cp int_pivot_region_home_values_raw.sql int_pivot_region_home_values_tiers.sql
sed -i '' 's/int_region_home_values_raw/int_region_home_values_tiers/g' int_pivot_region_home_values_tiers.sql

cp int_pivot_region_home_values_raw.sql int_pivot_region_rentals.sql
sed -i '' 's/int_region_home_values_raw/int_region_rentals/g' int_pivot_region_rentals.sql

cp int_pivot_region_home_values_raw.sql int_pivot_region_renter_demand.sql
sed -i '' 's/int_region_home_values_raw/int_region_renter_demand/g' int_pivot_region_renter_demand.sql
