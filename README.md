# Zillow Research dbt Project

This project uses dbt (data build tool) to transform and analyze [Zillow Research](https://www.zillow.com/research/data/)
data, including home values and rental prices across various cities in the United States.

> Zillow Home Value Index (ZHVI): A measure of the typical home value and market changes across a given region and housing type. It reflects the typical value for homes in the 35th to 65th percentile range. Available as a smoothed, seasonally adjusted measure and as a raw measure.

> Zillow Observed Rent Index (ZORI): A smoothed measure of the typical observed market rate rent across a given region. ZORI is a repeat-rent index that is weighted to the rental housing stock to ensure representativeness across the entire market, not just those homes currently listed for-rent. The index is dollar-denominated by computing the mean of listed rents that fall into the 35th to 65th percentile range for all homes and apartments in a given region, which is weighted to reflect the rental housing stock.

[ZHVI User Guide](https://www.zillow.com/research/zhvi-user-guide/)

## Setup Instructions

1. **Prerequisites**:
   - Install [dbt](https://docs.getdbt.com/docs/installation)
   - Install PostgreSQL (or devise alternative)

[How to Use the Postgres Docker Official Image](https://www.docker.com/blog/how-to-use-the-postgres-docker-official-image/)

2. **Database Configuration**:
   - Create a .env file containing:
     ```
     export DBT_USER=your_database_user
     export DBT_PASS=your_database_password
     ```

3. **dbt Profile Setup**:
   - Ensure your `profiles.yml` is configured correctly with your database credentials

4. **Install dbt dependencies**:
   ```
   dbt deps
   ```

5. **Load Source Data**:
   - Download csv files from [Zillow Research](https://www.zillow.com/research/data/) and place in the data/ directory
   - execute scripts/create_sym_links.sh or equivalent to normalize raw table names - Postgresql does not like to have . in table names.
   - execute scripts/build_raw_db.py to (re)build the zillow_research PostgreSQL database and load the raw csv files

6. **Run dbt**:
   ```
   dbt run
   ```

## models

- models beginning with stg_region are views scrubbing the column names for Postgres
- models beginning with int_region are tables unpivoting the Zillow source csv data
- region.sql creates a distinct list of regions
- More soon!


