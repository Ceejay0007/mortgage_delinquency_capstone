-- Freddie Mac Silver tables in Athena.
-- The data is the Parquet written by the freddie_mac_silver Glue job.
-- Run each statement one at a time in the Athena query editor.

-- 1) Origination table: one row per loan
CREATE EXTERNAL TABLE IF NOT EXISTS mortgage_silver.freddie_origination (
  loan_id STRING,
  credit_score INT,
  first_payment_date DATE,
  maturity_date DATE,
  first_time_homebuyer_flag STRING,
  msa STRING,
  mi_pct INT,
  num_units INT,
  occupancy_status STRING,
  orig_cltv INT,
  orig_dti INT,
  orig_upb DOUBLE,
  orig_ltv INT,
  orig_interest_rate DOUBLE,
  channel STRING,
  prepayment_penalty_flag STRING,
  amortization_type STRING,
  property_state STRING,
  property_type STRING,
  postal_code STRING,
  loan_purpose STRING,
  orig_loan_term INT,
  num_borrowers INT,
  seller_name STRING,
  super_conforming_flag STRING,
  pre_harp_loan_id STRING,
  special_eligibility_program STRING,
  harp_indicator STRING,
  property_valuation_method STRING,
  interest_only_flag STRING,
  vantage_score INT,
  processed_at TIMESTAMP
)
PARTITIONED BY (vintage INT)
STORED AS PARQUET
LOCATION 's3://conrad-mortgage-capstone-2026/silver/freddie_mac/origination/';


-- 2) Performance table: one row per loan per month
CREATE EXTERNAL TABLE IF NOT EXISTS mortgage_silver.freddie_performance (
  loan_id STRING,
  period DATE,
  delinquency_status STRING,
  delinquency_months INT,
  is_reo BOOLEAN,
  current_upb DOUBLE,
  current_interest_rate DOUBLE,
  loan_age INT,
  remaining_months INT,
  estimated_ltv INT,
  modification_flag STRING,
  is_modified BOOLEAN,
  step_rate_flag STRING,
  payment_deferral_flag STRING,
  has_payment_deferral BOOLEAN,
  zero_balance_code STRING,
  zero_balance_date DATE,
  zero_balance_removal_upb DOUBLE,
  defect_settlement_date DATE,
  last_paid_installment_date DATE,
  non_interest_bearing_upb DOUBLE,
  interest_bearing_upb DOUBLE,
  mi_recoveries DOUBLE,
  net_sales_proceeds DOUBLE,
  non_mi_recoveries DOUBLE,
  total_expenses DOUBLE,
  legal_costs DOUBLE,
  maintenance_costs DOUBLE,
  taxes_insurance DOUBLE,
  misc_expenses DOUBLE,
  actual_loss DOUBLE,
  cumulative_mod_costs DOUBLE,
  current_period_mod_costs DOUBLE,
  delinquent_accrued_interest DOUBLE,
  bankruptcy_cramdown_costs DOUBLE,
  disaster_delinquency_flag STRING,
  borrower_assistance_plan STRING,
  mi_cancellation_flag STRING,
  servicer_name STRING,
  processed_at TIMESTAMP
)
PARTITIONED BY (vintage INT)
STORED AS PARQUET
LOCATION 's3://conrad-mortgage-capstone-2026/silver/freddie_mac/performance/';


-- 3. Load the vintage=YYYY folders as partitions (rerun if a new vintage is added)
MSCK REPAIR TABLE mortgage_silver.freddie_origination;

MSCK REPAIR TABLE mortgage_silver.freddie_performance;