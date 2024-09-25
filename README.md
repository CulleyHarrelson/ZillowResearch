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
## Metrics and Models

This project includes two main analytical models: `city_metrics` and `correlation_analysis`.

### City Metrics Model

The `city_metrics` model is the core of this project, calculating various real estate metrics at different geographic levels: city, county, metro, and state. Here's a comprehensive list of all metrics and dimensions included in the model:

[... keep the existing content for city_metrics ...]

### Correlation Analysis Model

The `correlation_analysis` model provides insights into the relationship between home values and rental prices across different geographic levels and time periods.

#### Dimensions:

1. **Level**: Indicates the geographic level of aggregation (city, county, metro, or state)
2. **Group ID**: A unique identifier for each geographic entity
3. **Group Name**: The name of the geographic entity
4. **State**: The state of the entity (where applicable)
5. **Metro**: The metropolitan area of the entity (where applicable)
6. **County Name**: The county of the entity (where applicable)
7. **Report Year**: The year for which the correlations are calculated

#### Metrics:

1. **Group Home-Rental Correlation**: The correlation coefficient between home values and rental prices for each geographic entity over all available years. This metric indicates the strength and direction of the relationship between home values and rental prices within each geographic area.

2. **Yearly Home-Rental Correlation**: The correlation coefficient between home values and rental prices across all entities within a geographic level for each year. This metric shows how the relationship between home values and rental prices changes over time at each geographic level.

### Data Sources and Transformations:

The `correlation_analysis` model performs the following transformations:

1. Uses the `city_metrics` model as its data source, ensuring consistent data across models.
2. Calculates correlations between home values and rental prices for each geographic entity (city, county, metro, state) over all available years.
3. Computes yearly correlations between home values and rental prices for each geographic level.
4. Combines entity-level and yearly correlations into a single output table for easy analysis.

This model allows for in-depth analysis of how home values and rental prices relate to each other across different geographic scales and over time. It can provide valuable insights into housing market dynamics, such as:

- Identifying areas where home values and rental prices move in tandem or diverge
- Analyzing how the relationship between home values and rents changes over time in different geographic areas
- Comparing the strength of the home value-rent relationship across different cities, counties, metros, or states

By combining insights from both the `city_metrics` and `correlation_analysis` models, users can gain a comprehensive understanding of real estate trends and relationships across the United States.

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
