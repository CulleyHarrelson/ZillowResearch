# Zillow Research dbt Project

This project uses dbt (data build tool) to transform and analyze Zillow Research data, including home values and rental prices across various cities in the United States.

## Project Structure

- `dbt_project.yml`: Main configuration file for the dbt project
- `profiles.yml`: Contains database connection information
- `models/`: Directory containing SQL models
- `sources.yml`: Defines the source data tables
- `macros/`: Directory for custom SQL macros (if any)
- `seeds/`: Directory for static CSV files (if any)
- `tests/`: Directory for custom data tests
- `analyses/`: Directory for one-off analytical queries

## Setup Instructions

1. **Prerequisites**:
   - Install [dbt](https://docs.getdbt.com/docs/installation)
   - Set up a PostgreSQL database named `zillow_research`

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

## Metrics and Models

The `city_metrics` model is the core of this project, calculating various real estate metrics at different geographic levels: city, county, metro, and state. Here's a comprehensive list of all metrics and dimensions included in the model:

### Dimensions:

1. **Level**: Indicates the geographic level of aggregation (city, county, metro, or state)
2. **Group ID**: A unique identifier for each geographic entity (city_id for cities, name for other levels)
3. **Group Name**: The name of the geographic entity (city name, county name, metro name, or state name)
4. **State**: The state of the city (only for city level)
5. **Metro**: The metropolitan area of the city (for city level, or when level is metro)
6. **County Name**: The county of the city (for city level, or when level is county)
7. **Report Year**: The year for which the metrics are calculated

### Metrics:

1. **Average Home Value**: The average Zillow Home Value Index (ZHVI) for each geographic level and year.

2. **Average Rental Value**: The average Zillow Observed Rent Index (ZORI) for each geographic level and year.

3. **Home Value Year-over-Year Change Percent**: The percentage change in home values compared to the previous year. Calculated as:
   ```
   (Current Year Home Value - Previous Year Home Value) / Previous Year Home Value * 100
   ```

4. **Rental Value Year-over-Year Change Percent**: The percentage change in rental values compared to the previous year. Calculated as:
   ```
   (Current Year Rental Value - Previous Year Rental Value) / Previous Year Rental Value * 100
   ```

5. **Price-to-Annual-Rent Ratio**: The ratio of the average home value to the annual rental value. Calculated as:
   ```
   Average Home Value / (Average Rental Value * 12)
   ```
   This metric can indicate whether it's financially better to buy or rent in a particular area. A lower ratio suggests it might be more favorable to buy, while a higher ratio might favor renting.

### Data Sources and Transformations:

The `city_metrics` model performs the following transformations on the source data:

1. Aggregates home values and rental values to yearly averages from the source tables:
   - `home_values_by_city`: Contains the Zillow Home Value Index (ZHVI) data
   - `rentals_by_city`: Contains the Zillow Observed Rent Index (ZORI) data

2. Joins the aggregated data with the `cities` table to include geographic information (state, metro, county)

3. Calculates year-over-year changes for both home values and rental values

4. Computes the price-to-annual-rent ratio

5. Aggregates all metrics at the city, county, metro, and state levels

6. Combines all geographic levels into a single output table, with the 'level' column indicating the level of aggregation

This comprehensive model allows for analysis and comparison of real estate trends across different geographic levels and time periods, providing valuable insights into housing markets across the United States.

## Running Tests

To run tests on your models:

```
dbt test
```

## Generating Documentation

To generate and view documentation for your dbt project:

```
dbt docs generate
dbt docs serve
```

This will start a local server and open the documentation in your default web browser.

## Contributing

If you'd like to contribute to this project, please follow these steps:

1. Fork the repository
2. Create a new branch for your feature
3. Make your changes and commit them
4. Push to your fork and submit a pull request

## License

This project is licensed under the MIT License.
