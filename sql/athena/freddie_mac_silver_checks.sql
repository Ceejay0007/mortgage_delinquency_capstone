-- Checks for the Freddie Mac Silver tables.
-- Run after each Glue job. Expected values come from notebook 02.


-- 1) Loans per vintage. Expect 18 rows with 50,000 each.
SELECT vintage, COUNT(*) AS loans
FROM mortgage_silver.freddie_origination
GROUP BY vintage
ORDER BY vintage;


-- 2) Performance rows per vintage. Expect the same counts as the all years scan
--    (2005: 3,880,689 ... 2022: 2,151,863).
SELECT vintage, COUNT(*) AS rows
FROM mortgage_silver.freddie_performance
GROUP BY vintage
ORDER BY vintage;


-- 3) Missing codes became nulls in 2008. Expect 35 and 1026.
SELECT
  COUNT(*) - COUNT(credit_score) AS credit_score_nulls,
  COUNT(*) - COUNT(orig_dti)     AS dti_nulls
FROM mortgage_silver.freddie_origination
WHERE vintage = 2008;


-- 4) Foreclosure rows kept in 2008. Expect 10091 RA rows and a maximum of 99 months late.
SELECT
  COUNT_IF(is_reo)        AS reo_rows,
  MAX(delinquency_months) AS max_months_late
FROM mortgage_silver.freddie_performance
WHERE vintage = 2008;