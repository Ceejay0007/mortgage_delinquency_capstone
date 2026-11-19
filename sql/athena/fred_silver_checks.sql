-- FRED Silver validation checks (Athena, database(mortgage_silver)).

-- 1) Row count and missing values: Here we expect 24676 rows, 53 missing
SELECT COUNT(*) AS total_rows,
       SUM(CASE WHEN is_missing THEN 1 ELSE 0 END) AS missing_values
FROM fred_observations;

-- 2) Labels. expect 7 national series (1 each) and 51 per state measure
SELECT geo_level, measure, frequency, COUNT(DISTINCT series_id) AS series
FROM fred_observations
GROUP BY geo_level, measure, frequency
ORDER BY geo_level, measure;

-- 3) Duplicates check. expect ZERO rows
SELECT series_id, observation_date, COUNT(*) AS n
FROM fred_observations
GROUP BY series_id, observation_date
HAVING COUNT(*) > 1;

-- 4) Missing values check. expect one row, 2025-10-01, with 53 series
SELECT observation_date, COUNT(*) AS series_missing
FROM fred_observations
WHERE is_missing
GROUP BY observation_date;

-- 5) Sanity check against known history. national unemployment
--    should be near 10% in Oct 2009 and near 15% in Apr 2020
SELECT observation_date, value AS unemployment_rate
FROM fred_observations
WHERE series_id = 'UNRATE'
  AND observation_date IN (DATE '2009-10-01', DATE '2020-04-01');