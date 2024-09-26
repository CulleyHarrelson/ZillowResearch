
SELECT distinct column_name
FROM information_schema.columns
WHERE table_name = 'region_home_values_raw' AND column_name !~ '^\d{4}-\d{2}-\d{2}$'
UNION ALL
SELECT distinct column_name
FROM information_schema.columns
WHERE table_name = 'region_home_values_raw' AND column_name ~ '^\d{4}-\d{2}-\d{2}$'
ORDER BY column_name desc
