WITH home_values AS (
    SELECT *
    FROM {{ ref('int_region_home_values_raw') }}
),

size_rank_stats AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY CAST(size_rank AS INTEGER)) AS p25_rank,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST(size_rank AS INTEGER)) AS p50_rank,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CAST(size_rank AS INTEGER)) AS p75_rank
    FROM home_values
),

metrics AS (
    SELECT
        region_id,
        size_rank,
        region_name,
        region_type,
        state_name,
        DATE_TRUNC('month', CAST(metric_date AS DATE)) AS month,
        AVG(CAST(metric_value AS FLOAT)) AS avg_home_value,
        LAG(AVG(CAST(metric_value AS FLOAT))) OVER (PARTITION BY region_id ORDER BY DATE_TRUNC('month', CAST(metric_date AS DATE))) AS prev_month_avg_home_value,
        MIN(CAST(metric_value AS FLOAT)) AS min_home_value,
        MAX(CAST(metric_value AS FLOAT)) AS max_home_value
    FROM home_values
    GROUP BY 1, 2, 3, 4, 5, 6
),

monthly_growth AS (
    SELECT
        *,
        (avg_home_value - prev_month_avg_home_value) / NULLIF(prev_month_avg_home_value, 0) AS mom_growth_rate
    FROM metrics
),

seasonal_analysis AS (
    SELECT
        region_id,
        EXTRACT(MONTH FROM CAST(metric_date AS DATE)) AS month_number,
        AVG(CAST(metric_value AS FLOAT)) AS avg_monthly_value
    FROM home_values
    GROUP BY 1, 2
)

SELECT
    m.*,
    mg.mom_growth_rate,
    sa.avg_monthly_value AS seasonal_avg_value,
    CASE 
        WHEN CAST(m.size_rank AS INTEGER) <= s.p25_rank THEN 'Very Large'
        WHEN CAST(m.size_rank AS INTEGER) <= s.p50_rank THEN 'Large'
        WHEN CAST(m.size_rank AS INTEGER) <= s.p75_rank THEN 'Medium'
        ELSE 'Small'
    END AS city_size_category
FROM metrics m
LEFT JOIN monthly_growth mg ON m.region_id = mg.region_id AND m.month = mg.month
LEFT JOIN seasonal_analysis sa ON m.region_id = sa.region_id AND EXTRACT(MONTH FROM m.month) = sa.month_number
CROSS JOIN size_rank_stats s
