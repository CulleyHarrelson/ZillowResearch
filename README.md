# Zillow Research dbt Project

This project uses dbt (data build tool) to transform and analyze [Zillow Research](https://www.zillow.com/research/data/)
data, including home values and rental prices across various cities in the United States.

> Zillow Home Value Index (ZHVI): A measure of the typical home value and market changes across a given region and housing type. It reflects the typical value for homes in the 35th to 65th percentile range. Available as a smoothed, seasonally adjusted measure and as a raw measure.

> Zillow Observed Rent Index (ZORI): A smoothed measure of the typical observed market rate rent across a given region. ZORI is a repeat-rent index that is weighted to the rental housing stock to ensure representativeness across the entire market, not just those homes currently listed for-rent. The index is dollar-denominated by computing the mean of listed rents that fall into the 35th to 65th percentile range for all homes and apartments in a given region, which is weighted to reflect the rental housing stock.

[ZHVI User Guide](https://www.zillow.com/research/zhvi-user-guide/)

## Project Structure

- `dbt_project.yml`: Main configuration file for the dbt project
- `profiles.yml`: Contains database connection information
- `models/`: Directory containing SQL models
- `sources.yml`: Defines the source data tables
- `macros/`: Directory for custom SQL macros (if any)
- `seeds/`: Place Zillow Research CSV files here
- `tests/`: Directory for custom data tests
- `analyses/`: Directory for one-off analytical queries - have fun!

## Setup Instructions

1. **Prerequisites**:
   - Install [dbt](https://docs.getdbt.com/docs/installation)
   - Set up or connect to a PostgreSQL database named `zillow_research` (modify profiles.yml to change the default database name)

[How to Use the Postgres Docker Official Image](https://www.docker.com/blog/how-to-use-the-postgres-docker-official-image/)

2. **Database Configuration**:
   - Set the following environment variables:
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
   - Download csv files from [Zillow Research](https://www.zillow.com/research/data/)
   - Load the Zillow Home Value Index (ZHVI) and Zillow Observed Rent Index (ZORI) data into your PostgreSQL database in the `public` schema with

   ```
   dbt seed
   ```

6. **Run dbt**:
   ```
   dbt run
   ```

## models

- models beginning with int_region are views scrubbing the column names for Postgres
- models beginning with int_pivot_region are tables unpivoting the Zillow source csv data
- region.sql creates a distinct list of regions


