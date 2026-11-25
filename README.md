# Mortgage Delinquency Prediction Under Economic Stress

M.S. Data Science Capstone, Houston Christian University (DSCI 5390)
Author: Conrad Toke

An end to end data engineering and machine learning project that predicts mortgage delinquency from Freddie Mac loan level data and measures how model performance changes when economic conditions shift (the 2008 financial crisis and COVID-19).

## Research question

How well does a delinquency model trained in one economic period perform in another, and does adding macroeconomic features make it more robust?

Target: a loan is seriously delinquent if, within 24 months of its first payment date, it becomes 90 or more days delinquent, is acquired through foreclosure (REO), or ends in a foreclosure sale, short sale, or charge off. A fixed 24 month window gives every loan the same chance to become delinquent, so loans from different years can be compared fairly.

Evaluation design: models are trained on earlier periods and tested on later ones (for example, trained on 2005–2006 loans and tested on 2007–2009 loans, and trained on 2013–2018 loans and tested on 2019–2021 loans). Each model is compared with and without economic indicators measured at origination. This tests real historical economic shifts rather than simulated scenarios.

## Architecture

![Pipeline architecture](docs/diagrams/pipeline_architecture.png)

Why this split? The heavy data work runs in AWS. About 56 million monthly loan records are cleaned with Spark in Glue and joined with dbt in Athena, where storage and compute scale with the data. Only the finished modeling table (about 900,000 loans) and the model results are loaded into PostgreSQL, where modeling and the dashboard run locally. This keeps processing fast and cheap while mirroring how lenders pair cloud processing with a curated warehouse.

## Repository layout

```
config/          pipeline settings and the Freddie Mac column layout (non secret)
ingestion/       Bronze: Freddie Mac and FRED ingestion scripts
glue/            Silver: PySpark jobs (run locally or in AWS Glue)
infra/aws/       IAM roles and Glue jobs as code (run in AWS CloudShell)
sql/athena/      Athena table definitions (DDL) and validation queries
dbt/             Gold models and data tests (dbt on Athena)
ml/              training, evaluation by time window, drift analysis
airflow/         Airflow DAG
notebooks/       data profiling and EDA
dashboard/       Tableau screenshots only (no data)
docs/            project log, diagrams, and write ups
tests/           unit tests
```

## Data and licensing

Loan data comes from the Freddie Mac Single Family Loan Level Dataset (sample files, release 2026Q2), used under Freddie Mac's terms of use. No Freddie Mac data is stored in this repository, and notebook outputs that show individual loans are cleared before committing. To reproduce, register with Freddie Mac and download the sample files yourself. Macroeconomic data comes from FRED, Federal Reserve Bank of St. Louis.

## AWS environment

All AWS resources run in one AWS project account in us-east-2 (Ohio).

- Least privilege: the laptop pipeline user can read and write the project bucket only. It cannot create roles or change permissions. Admin tasks run in AWS CloudShell.
- Infrastructure as code: IAM roles and Glue jobs are created from scripts in `infra/aws/`, so the setup is documented, reviewable, and repeatable.
- Cost controls: a project spending cap, a per query Athena scan limit, and Glue job timeouts (10 minutes for FRED, 45 minutes for Freddie Mac).
- Security: public access is blocked on the bucket, objects are encrypted at rest, bucket versioning is on, and Athena query results are written to the project bucket with the expected owner verified.

## Setup

```bash
conda create -n capstone python=3.11 -y
conda activate capstone
pip install -r requirements.txt
conda install -c conda-forge openjdk=17 -y   # Java, required to run Spark locally
cp .env.example .env                          # then fill in your values
pytest -q                                     # confirm everything imports and passes
```

## Step 1) Bronze ingestion

Bronze stores raw data exactly as received and is never modified.

FRED (API, fully automated). Pulls 109 series: 7 national (unemployment, CPI, federal funds rate, 30 year mortgage rate, real GDP, house price index, and the NBER recession indicator) plus unemployment and house prices for all 50 states and DC.

```bash
python -m ingestion.fred_ingestion --local --national-only   # quick test, no AWS needed
python -m ingestion.fred_ingestion                            # full run to S3
```

Freddie Mac (licensed download, then automated upload). The dataset requires a login, so the 18 sample zip files (2005–2022) are downloaded once by hand into `data/bronze/freddie_mac/`, a local landing folder. The script then validates each file, records a checksum, and uploads it to S3.

```bash
python -m ingestion.freddie_mac_ingestion --release 2026Q2 --dry-run   # validate first
python -m ingestion.freddie_mac_ingestion --release 2026Q2             # upload to S3
```

Bronze layout in S3:

```
bronze/fred/series_id=<ID>/ingest_date=<YYYY-MM-DD>/observations.json
bronze/freddie_mac/release=<YYYYQn>/origination/vintage=<YYYY>/sample_orig_<YYYY>.txt
bronze/freddie_mac/release=<YYYYQn>/performance/vintage=<YYYY>/sample_perf_<YYYY>.txt
```

Result: 145 objects (109 FRED series and 36 Freddie Mac files), about 6.5 GB, including roughly 56 million monthly loan performance records.

## Data profiling

Before writing any cleaning code, the raw data was profiled.

FRED (`notebooks/01_fred_profiling.ipynb`):

- All 109 series share the same structure, so one script can clean all of them.
- Values and dates are stored as text, and the series ID appears only in the folder path.
- 53 series are missing one value, all in October 2025, when the Bureau of Labor Statistics canceled its employment and CPI reports during the federal government shutdown. These stay missing in Silver and are flagged, not filled.

Freddie Mac (`notebooks/02_freddie_mac_profiling.ipynb`):

- All 18 vintages (2005–2022) have 50,000 loans, 31 origination columns, and 35 performance columns, so one script can clean every year.
- Missing values use placeholder codes (9999, 999, 99, 9, 7) defined in the Freddie Mac General User Guide. These become nulls in Silver.
- RA (REO Acquisition, meaning the lender took the property through foreclosure) is the only non numeric delinquency status, and the status is capped at 99.
- Loan age restarts when a loan is modified, so the 24 month target window is measured from first_payment_date. This corrected 251 wrong labels in the 2008 vintage.
- The number of borrowers changed coding in 2018Q2, so all years are capped at 2 to keep them comparable.
- 1,199 loans in Puerto Rico, Guam, and the US Virgin Islands have no FRED state data and will be excluded in Gold.
- The 2008 vintage has a target rate of about 3.98%, so models are evaluated with imbalance aware metrics (PR-AUC and recall) rather than accuracy.

## Step 2) Silver, FRED

The FRED Silver job (`glue/fred_silver.py`) turns raw FRED JSON into one clean table. It unpacks observations, converts types, flags missing values, labels each series, keeps the latest download, and runs quality checks before writing Parquet. The job stops without writing if any check fails.

Run locally (reads `data/bronze/`, writes `data/silver/`):

```bash
python glue/fred_silver.py
```

Run in AWS Glue (one time setup, then run; from AWS CloudShell with the scripts from `infra/aws/`):

```bash
bash create_glue_role.sh            # IAM role for Glue (least privilege), shared by all Glue jobs
bash create_fred_silver_job.sh      # Glue 5.0 job pointing at scripts/glue/fred_silver.py
bash run_glue_job.sh fred_silver    # start the job and wait for the result
```

Deploy script changes with:

```bash
aws s3 cp glue/fred_silver.py s3://<bucket>/scripts/glue/fred_silver.py
```

Query in Athena: create the table with `sql/athena/fred_silver_ddl.sql`, then validate with `sql/athena/fred_silver_checks.sql`.

Result: 24,676 rows across 109 series, 53 missing values (all October 2025), no duplicates. Row counts match between the local run and AWS.

## Step 3) Silver, Freddie Mac

The Freddie Mac Silver job (`glue/freddie_mac_silver.py`) cleans all 18 vintages and writes two Parquet tables partitioned by vintage:

```
silver/freddie_mac/origination/vintage=<YYYY>/    one row per loan
silver/freddie_mac/performance/vintage=<YYYY>/    one row per loan per month
```

The job runs in four steps:

1. Read: name the columns from `config/freddie_mac_columns.yaml` and check the column count.
2. Clean: replace not available codes with nulls, convert types, and add is_reo, is_modified, and has_payment_deferral. Freddie Mac's original codes are kept.
3. Check: stop the job if any value cannot be converted, if a loan or loan month is duplicated, if a delinquency status is unknown, or if a performance loan has no origination record.
4. Write: Parquet with dynamic partition overwrite, then read back and confirm the row counts per vintage.

Test locally on two vintages. Spark cannot read inside zip files, so 2008 and 2019 are first unzipped into the same folder layout as S3:

```bash
RELEASE=2026Q2
for YEAR in 2008 2019; do
  for KIND in origination performance; do
    mkdir -p data/bronze/freddie_mac/release=$RELEASE/$KIND/vintage=$YEAR
  done
  unzip -o data/bronze/freddie_mac/sample_$YEAR.zip sample_orig_$YEAR.txt \
    -d data/bronze/freddie_mac/release=$RELEASE/origination/vintage=$YEAR/
  unzip -o data/bronze/freddie_mac/sample_$YEAR.zip sample_perf_$YEAR.txt \
    -d data/bronze/freddie_mac/release=$RELEASE/performance/vintage=$YEAR/
done

python glue/freddie_mac_silver.py
```

Deploy the script and column layout (from the laptop):

```bash
aws s3 cp glue/freddie_mac_silver.py s3://<bucket>/scripts/glue/freddie_mac_silver.py
aws s3 cp config/freddie_mac_columns.yaml s3://<bucket>/scripts/config/freddie_mac_columns.yaml
```

Create and run the Glue job (from AWS CloudShell):

```bash
bash create_freddie_mac_silver_job.sh     # 5 workers, 45 minute timeout, installs PyYAML
bash run_glue_job.sh freddie_mac_silver
```

Query in Athena: create the tables with `sql/athena/freddie_mac_silver_ddl.sql`, then validate with `sql/athena/freddie_mac_silver_checks.sql`.

Result: 36 Parquet files (18 vintages, 2 tables), about 749 MB. Loan counts, performance row counts, null counts, and REO counts in Athena all match the profiling notebook.

## Design decisions

| Decision | Reason |
|---|---|
| Medallion layers (Bronze, Silver, Gold) | Raw data is kept untouched, so any cleaning step can be rerun from the original |
| Bronze is never overwritten | Uploads are checksummed. A changed file is refused rather than silently replaced |
| Freddie Mac partitioned by release | The dataset is updated quarterly; each release is kept side by side for a full audit trail |
| Manual Freddie Mac download | The dataset requires a login, and scripting around it would be fragile and could breach the terms of use |
| name=value folder structure | Spark reads folder names as columns, so the series ID, dates, and vintage come for free |
| Missing values stay null in Silver | Silver records what the source published; any filling is a documented Gold decision |
| Quality checks stop the job | Bad data never reaches downstream layers, and failures are visible to orchestration |
| One script for laptop and Glue | The --BUCKET parameter switches between local folders and S3, so code is tested locally before it runs in AWS |
| Mirror the S3 layout locally for testing | Local tests read exactly the same folder structure the Glue job reads |
| Column names in a config file | The notebook and the Glue job read the same layout file, so names are defined once |
| Keep Freddie Mac flag codes and add true/false columns | Filling blanks with N would invent codes Freddie Mac does not use |
| Target window measured from first_payment_date | Loan age restarts after a modification, which mislabeled 251 loans in 2008 |
| Silver partitioned by vintage | Queries for a training window only read the years they need, which lowers Athena cost |
| Dynamic partition overwrite | Rerunning one vintage replaces only that vintage, so reruns are safe |
| Athena tables defined in versioned DDL, not a crawler | Column types are chosen on purpose, reviewed in Git, and reproducible |
| One run script for all Glue jobs | `run_glue_job.sh` takes the job name, so there is less duplication and Airflow can call one script |
| Least privilege access | The Glue role can read Bronze but not change it; the laptop user cannot change permissions |
| Infrastructure as code | Roles and jobs are created from versioned scripts instead of console clicks |
| Gold built in Athena, not PostgreSQL | Joining 56 million rows is faster and cheaper in Athena. PostgreSQL receives only the finished modeling table |
| Feature selection on the training window only | Looking at test period data when choosing features would leak information |
| Tableau instead of Power BI | Power BI Desktop does not run on macOS |
| Airflow added after the manual pipeline works | Each stage runs with one command, can be rerun safely, keeps settings outside code, and fails loudly, so wiring it into Airflow later needs no code changes |

## Project status

| Stage | Status |
|---|---|
| Bronze ingestion (FRED, Freddie Mac) | Done |
| Profiling (FRED, Freddie Mac) | Done |
| Silver: FRED (Glue job and Athena validation) | Done |
| Silver: Freddie Mac (Glue job and Athena validation) | Done |
| Gold layer (dbt on Athena) | Next |
| Modeling, MLflow, and evaluation by time window | Planned |
| Airflow orchestration | Planned |
| Tableau dashboard | Planned |

## Use of AI tools

AI coding assistants were used to help scaffold code and documentation. All analysis, interpretation, and written academic work are my own.
