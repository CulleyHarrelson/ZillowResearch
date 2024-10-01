WITH home_values AS (
    SELECT *
    FROM {{ ref('int_region_home_values_pivoted') }}
),

size_rank_stats AS (
    SELECT
        APPROX_PERCENTILE(CAST(size_rank AS BIGINT), 0.25) AS p25_rank,
        APPROX_PERCENTILE(CAST(size_rank AS BIGINT), 0.5) AS p50_rank,
        APPROX_PERCENTILE(CAST(size_rank AS BIGINT), 0.75) AS p75_rank
    FROM home_values
),

metrics AS (
    SELECT
        region_id,
        size_rank,
        region_name,
        region_type,
        state_name,
        DATE_TRUNC('year', CAST(metric_date AS DATE)) AS metric_year,
        AVG(CAST(metric_value AS DOUBLE)) AS avg_home_value,
        LAG(AVG(CAST(metric_value AS DOUBLE))) OVER (PARTITION BY region_id ORDER BY DATE_TRUNC('year', CAST(metric_date AS DATE))) AS prev_year_avg_home_value,
        MIN(CAST(metric_value AS DOUBLE)) AS min_home_value,
        MAX(CAST(metric_value AS DOUBLE)) AS max_home_value
    FROM home_values
    GROUP BY 1, 2, 3, 4, 5, 6
),

yearly_growth AS (
    SELECT
        *,
        (avg_home_value - prev_year_avg_home_value) / NULLIF(prev_year_avg_home_value, 0) AS yoy_growth_rate
    FROM metrics
),

long_term_appreciation AS (
    SELECT
        region_id,
        size_rank,
        region_name,
        region_type,
        state_name,
        (MAX(avg_home_value) - MIN(avg_home_value)) / NULLIF(MIN(avg_home_value), 0) AS total_appreciation
    FROM metrics
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    m.*,
    yg.yoy_growth_rate,
    la.total_appreciation,
    CASE 
        WHEN CAST(m.size_rank AS BIGINT) <= s.p25_rank THEN 'Very Large'
        WHEN CAST(m.size_rank AS BIGINT) <= s.p50_rank THEN 'Large'
        WHEN CAST(m.size_rank AS BIGINT) <= s.p75_rank THEN 'Medium'
        ELSE 'Small'
    END AS city_size_category
FROM metrics m
LEFT JOIN yearly_growth yg ON m.region_id = yg.region_id AND m.metric_year = yg.metric_year
LEFT JOIN long_term_appreciation la ON m.region_id = la.region_id
CROSS JOIN size_rank_stats s
