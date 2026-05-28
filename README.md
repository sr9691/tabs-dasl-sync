# DASL → BigQuery Sync

Google Cloud Function (Python) that extracts school data from the NAIS DASL
RESTful API and loads it into BigQuery as a star schema, then rebuilds one
wide view per top-level variable category for downstream BI tools.

## What it writes

Dataset: `dasl-493214.dasl`

| Object | Type | Contents |
| --- | --- | --- |
| `dim_school` | table | One row per association school. |
| `dim_variable` | table | One row per DASL variable, with auto-generated snake_case column names. |
| `dim_lookup` | table | One row per `(lookup_group_id, key)` choice value. |
| `fact_school_data` | table | One row per `(school_id, year, var_id)` data point. Range-partitioned by `year`, clustered by `school_id, var_id`. |
| `vw_<category>` | view | One per top-level category — wide format, one row per `(school_id, year)`, with column descriptions auto-generated from each variable's label. |

## Behavior

- **Each run** authenticates against DASL, pulls the latest metadata
  (variables, lookup groups, association schools), then for every
  `(year, subcategory)` in the configured backfill window makes one bulk call
  to `/assocSchoolData?subCategoryId=X&year=Y` — the response contains all
  schools' data points for that slice. Existing `fact_school_data` rows for
  every `(school_id, year)` are deleted before the new rows are appended, so
  the function is idempotent on `(school_id, year)`.
- The bulk endpoint is used instead of the per-school
  `/assocSchoolData/{schoolId}` endpoint because the latter returns 404 for
  ~30-40% of schools that nevertheless have valid data available via the bulk
  call. See the comment in `main.py` near `get_assoc_school_data`.
- **Backfill window** is controlled by `BACKFILL_YEARS` (default **1** =
  current year only). Each year costs ~25 API calls (one per subcategory),
  so a 3-year backfill runs in well under a minute.
- **Cleaning**: numeric fields are stripped of `$`/`,`/spaces; explicit `NA`
  values become NULL; `choiceSingle` / `choiceMulti` values are resolved
  against the lookup groups; `last_updated` is normalized to UTC ISO 8601.
  Full rules live at the top of `main.py`.
- **Authentication**: the DASL client secret is fetched at runtime from
  Google Secret Manager (`DASL_CLIENT_SECRET_NAME`). Credentials are never
  logged.

## Files

| Path | Purpose |
| --- | --- |
| `main.py` | Cloud Function entry point (`run_pipeline`) and pipeline logic. |
| `requirements.txt` | Python dependencies. |
| `run_local.py` | Local driver — calls `main.run()` directly using ADC / a local SA key. |
| `.gcloudignore` | Excludes the SA key, dev scripts, and CSV/JSON artifacts from the deploy bundle. |
| `export_metadata.py` | One-shot helper that dumped `variables.csv`, `lookup_values.csv`, etc. for inspection. |
| `create_tuition_tall_view.sql` | Optional unpivoted view of the tuition columns (run manually if useful). |
| `*.csv`, `varid_subcat_map.json`, `swagger.tmp.json` | Reference / inspection artifacts; not used at runtime. |

## Environment variables

All have defaults baked into `main.py`; override only if you need to.

| Name | Default | Purpose |
| --- | --- | --- |
| `GCP_PROJECT_ID` | `dasl-493214` | Google Cloud project ID. |
| `BQ_DATASET` | `dasl` | BigQuery dataset (created if missing). |
| `DASL_BASE_URL` | `https://daslwebapi.azurewebsites.net` | DASL API root. Use `https://stagedaslwebapi.azurewebsites.net` for staging. |
| `DASL_CLIENT_ID` | `tabs` | DASL OAuth client ID (literal value, not a secret). |
| `DASL_CLIENT_SECRET_NAME` | `dasl-tabs-api-key` | Secret Manager secret holding the DASL client secret. |
| `BACKFILL_YEARS` | `1` | Number of trailing reporting years to load each run. |
| `GOOGLE_APPLICATION_CREDENTIALS` | _(local only)_ | Path to a SA key file. Cloud Functions use the runtime SA via ADC. |

## Production deployment

Already live in `dasl-493214`:

- **Function**: `dasl-sync` (Gen2, us-central1) — `https://us-central1-dasl-493214.cloudfunctions.net/dasl-sync`
- **Runtime SA**: `ansa-dasl-pipeline@dasl-493214.iam.gserviceaccount.com` (has `roles/bigquery.dataEditor`, `roles/bigquery.jobUser`, `roles/secretmanager.secretAccessor`, `roles/run.invoker`).
- **Schedule**: Cloud Scheduler `dasl-sync-weekly` — `0 6 * * 1` America/New_York (Mondays 6 AM ET), OIDC-authenticated with the same SA.

### Re-deploy after a code change

```bash
gcloud functions deploy dasl-sync \
  --gen2 \
  --project=dasl-493214 \
  --runtime=python311 \
  --region=us-central1 \
  --source=. \
  --entry-point=run_pipeline \
  --trigger-http \
  --no-allow-unauthenticated \
  --timeout=3600 \
  --memory=1Gi \
  --service-account=ansa-dasl-pipeline@dasl-493214.iam.gserviceaccount.com \
  --set-env-vars="GCP_PROJECT_ID=dasl-493214,BQ_DATASET=dasl,DASL_BASE_URL=https://daslwebapi.azurewebsites.net,DASL_CLIENT_ID=tabs,DASL_CLIENT_SECRET_NAME=dasl-tabs-api-key,BACKFILL_YEARS=1"
```

To update only an env var without rebuilding:

```bash
gcloud functions deploy dasl-sync --gen2 --project=dasl-493214 \
  --region=us-central1 --update-env-vars="BACKFILL_YEARS=2"
```

### Trigger manually

```bash
# CLI (uses your gcloud identity for OIDC)
gcloud functions call dasl-sync --region=us-central1 --project=dasl-493214

# Or hit the Cloud Run URL directly
curl -X POST -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  https://dasl-sync-wkkg2kg7uq-uc.a.run.app
```

### Logs

```bash
gcloud functions logs read dasl-sync --region=us-central1 --project=dasl-493214 --limit=200
```

Look for the `Pipeline summary:` line — it has years loaded, school/variable
counts, fact-row count, and API-call/error counts.

## Local development

```bash
pip install -r requirements.txt
# Place the SA key at the path GOOGLE_APPLICATION_CREDENTIALS expects, then:
python run_local.py
```

The local driver invokes `main.run()` directly (no Functions Framework). A
full prod backfill takes ~3.5 hours over a residential connection; in Cloud
Functions it's faster, but the 60-minute timeout is the reason
`BACKFILL_YEARS` defaults to 1.

## Useful BigQuery query

`dasl_coverage_by_year_category` — one row per (year, top-level category)
with school count, variables filled, and populated %:

```sql
WITH per_slice AS (
  SELECT
    f.year,
    v.category,
    f.school_id,
    f.var_id,
    f.is_na,
    CASE
      WHEN f.value_numeric IS NOT NULL OR f.value_string IS NOT NULL OR f.value_choice IS NOT NULL
      THEN 1 ELSE 0
    END AS has_value
  FROM `dasl-493214.dasl.fact_school_data` f
  JOIN `dasl-493214.dasl.dim_variable`     v USING (var_id)
)
SELECT
  year,
  category,
  COUNT(DISTINCT school_id)                         AS schools_reporting,
  COUNT(DISTINCT var_id)                            AS variables_with_data,
  COUNT(*)                                          AS data_points,
  SUM(has_value)                                    AS non_null_values,
  SUM(CASE WHEN is_na THEN 1 ELSE 0 END)            AS na_marked,
  ROUND(100 * SUM(has_value) / COUNT(*), 1)         AS pct_populated
FROM per_slice
GROUP BY year, category
ORDER BY year DESC, data_points DESC;
```

## One-time setup (reference, already done)

### 1. Dataset

```bash
bq --location=US mk --dataset "dasl-493214:dasl"
```

### 2. Secret Manager

```bash
gcloud secrets create dasl-tabs-api-key --replication-policy="automatic"
printf '%s' 'YOUR_DASL_CLIENT_SECRET' | \
  gcloud secrets versions add dasl-tabs-api-key --data-file=-
```

### 3. Runtime SA grants

```bash
SA="ansa-dasl-pipeline@dasl-493214.iam.gserviceaccount.com"

gcloud secrets add-iam-policy-binding dasl-tabs-api-key \
  --member="serviceAccount:$SA" --role="roles/secretmanager.secretAccessor"

gcloud projects add-iam-policy-binding dasl-493214 \
  --member="serviceAccount:$SA" --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding dasl-493214 \
  --member="serviceAccount:$SA" --role="roles/bigquery.jobUser"
```

### 4. Scheduler

```bash
gcloud scheduler jobs create http dasl-sync-weekly \
  --project=dasl-493214 \
  --location=us-central1 \
  --schedule="0 6 * * 1" \
  --time-zone="America/New_York" \
  --http-method=POST \
  --uri="https://dasl-sync-wkkg2kg7uq-uc.a.run.app" \
  --oidc-service-account-email="ansa-dasl-pipeline@dasl-493214.iam.gserviceaccount.com" \
  --oidc-token-audience="https://dasl-sync-wkkg2kg7uq-uc.a.run.app" \
  --attempt-deadline=1800s
```
