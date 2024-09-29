WITH home_values AS (
    SELECT *
    FROM {{ ref('home_values') }}
),

rentals AS (
    SELECT *
    FROM {{ ref('rental_prices') }}
),

combined_data AS (
    SELECT
        hv.region_id,
        hv.region_name,
        hv.region_type,
        hv.state_name,
        hv.size_rank,
        hv.metric_year,
        r.metric_month,
        hv.avg_home_value,
        r.avg_rental_price * 12 AS annual_rental_price,
        hv.yoy_growth_rate AS home_value_yoy_growth,
        r.mom_growth_rate AS rental_price_mom_growth,
        hv.city_size_category
    FROM home_values hv
    INNER JOIN rentals r 
        ON hv.region_id = r.region_id 
        AND DATE_TRUNC('year', r.metric_month) = DATE_TRUNC('year', CAST(hv.metric_year AS DATE))
),

yearly_metrics AS (
    SELECT
        region_id,
        DATE_TRUNC('year', CAST(metric_year AS DATE)) AS year,
        AVG(avg_home_value) AS yearly_avg_home_value,
        AVG(annual_rental_price) AS yearly_avg_rental_price,
        (MAX(avg_home_value) - MIN(avg_home_value)) / NULLIF(MIN(avg_home_value), 0) AS home_value_yearly_volatility,
        (MAX(annual_rental_price) - MIN(annual_rental_price)) / NULLIF(MIN(annual_rental_price), 0) AS rental_price_yearly_volatility
    FROM combined_data
    GROUP BY 1, 2
),

long_term_trends AS (
    SELECT
        region_id,
        (MAX(yearly_avg_home_value) - MIN(yearly_avg_home_value)) / NULLIF(MIN(yearly_avg_home_value), 0) AS home_value_total_appreciation,
        (MAX(yearly_avg_rental_price) - MIN(yearly_avg_rental_price)) / NULLIF(MIN(yearly_avg_rental_price), 0) AS rental_price_total_appreciation
    FROM yearly_metrics
    GROUP BY 1
)

SELECT
    cd.*,
    cd.avg_home_value / NULLIF(cd.annual_rental_price, 0) AS price_to_rent_ratio,
    AVG(cd.avg_home_value / NULLIF(cd.annual_rental_price, 0)) OVER (PARTITION BY cd.state_name, DATE_TRUNC('year', CAST(cd.metric_year AS DATE))) AS state_avg_price_to_rent_ratio,
    AVG(cd.avg_home_value / NULLIF(cd.annual_rental_price, 0)) OVER (PARTITION BY DATE_TRUNC('year', CAST(cd.metric_year AS DATE))) AS national_avg_price_to_rent_ratio,
    (cd.home_value_yoy_growth - cd.rental_price_mom_growth) AS growth_difference,
    ym.home_value_yearly_volatility,
    ym.rental_price_yearly_volatility,
    lt.home_value_total_appreciation,
    lt.rental_price_total_appreciation,
    (lt.home_value_total_appreciation - lt.rental_price_total_appreciation) AS long_term_appreciation_difference
FROM combined_data cd
LEFT JOIN yearly_metrics ym ON cd.region_id = ym.region_id AND DATE_TRUNC('year', CAST(cd.metric_year AS DATE)) = ym.year
LEFT JOIN long_term_trends lt ON cd.region_id = lt.region_id
