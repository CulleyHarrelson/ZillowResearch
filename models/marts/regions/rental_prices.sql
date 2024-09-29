WITH rentals AS (
    SELECT *
    FROM {{ ref('int_region_rentals_pivoted') }}
),

size_rank_stats AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY CAST(size_rank AS INTEGER)) AS p25_rank,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST(size_rank AS INTEGER)) AS p50_rank,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CAST(size_rank AS INTEGER)) AS p75_rank
    FROM rentals
),

metrics AS (
    SELECT
        region_id,
        size_rank,
        region_name,
        region_type,
        state_name,
        DATE_TRUNC('month', CAST(metric_date AS DATE)) AS metric_month,
        AVG(CAST(metric_value AS FLOAT)) AS avg_rental_price,
        LAG(AVG(CAST(metric_value AS FLOAT))) OVER (PARTITION BY region_id ORDER BY DATE_TRUNC('month', CAST(metric_date AS DATE))) AS prev_month_avg_rental_price,
        MIN(CAST(metric_value AS FLOAT)) AS min_rental_price,
        MAX(CAST(metric_value AS FLOAT)) AS max_rental_price
    FROM rentals
    GROUP BY 1, 2, 3, 4, 5, 6
),

monthly_growth AS (
    SELECT
        *,
        (avg_rental_price - prev_month_avg_rental_price) / NULLIF(prev_month_avg_rental_price, 0) AS mom_growth_rate
    FROM metrics
),

yearly_metrics AS (
    SELECT
        region_id,
        DATE_TRUNC('year', metric_month) AS year,
        AVG(avg_rental_price) AS yearly_avg_rental_price,
        MIN(avg_rental_price) AS yearly_min_rental_price,
        MAX(avg_rental_price) AS yearly_max_rental_price,
        (MAX(avg_rental_price) - MIN(avg_rental_price)) / NULLIF(MIN(avg_rental_price), 0) AS yearly_price_volatility
    FROM metrics
    GROUP BY 1, 2
),

long_term_trends AS (
    SELECT
        region_id,
        (MAX(yearly_avg_rental_price) - MIN(yearly_avg_rental_price)) / NULLIF(MIN(yearly_avg_rental_price), 0) AS total_appreciation,
        AVG(yearly_price_volatility) AS avg_yearly_volatility
    FROM yearly_metrics
    GROUP BY 1
),

seasonal_analysis AS (
    SELECT
        region_id,
        EXTRACT(MONTH FROM metric_month) AS month_number,
        AVG(avg_rental_price) AS avg_monthly_rental_price
    FROM metrics
    GROUP BY 1, 2
)

SELECT
    m.*,
    mg.mom_growth_rate,
    ym.yearly_avg_rental_price,
    ym.yearly_min_rental_price,
    ym.yearly_max_rental_price,
    ym.yearly_price_volatility,
    lt.total_appreciation,
    lt.avg_yearly_volatility,
    sa.avg_monthly_rental_price AS seasonal_avg_rental_price,
    CASE 
        WHEN CAST(m.size_rank AS INTEGER) <= s.p25_rank THEN 'Very Large'
        WHEN CAST(m.size_rank AS INTEGER) <= s.p50_rank THEN 'Large'
        WHEN CAST(m.size_rank AS INTEGER) <= s.p75_rank THEN 'Medium'
        ELSE 'Small'
    END AS city_size_category
FROM metrics m
LEFT JOIN monthly_growth mg ON m.region_id = mg.region_id AND m.metric_month = mg.metric_month
LEFT JOIN yearly_metrics ym ON m.region_id = ym.region_id AND DATE_TRUNC('year', m.metric_month) = ym.year
LEFT JOIN long_term_trends lt ON m.region_id = lt.region_id
LEFT JOIN seasonal_analysis sa ON m.region_id = sa.region_id AND EXTRACT(MONTH FROM m.metric_month) = sa.month_number
CROSS JOIN size_rank_stats s
