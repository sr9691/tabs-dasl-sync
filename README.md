# DASL → BigQuery Sync

Google Cloud Function (Python) that extracts school data from the NAIS DASL
RESTful API and loads it into BigQuery in a wide/columnar format (one row per
school per year).

## Behavior

- **First run** (target table does not exist): creates the BigQuery table
  with one column per DASL variable (column names derived from the metadata
  endpoint labels) and performs a full historical backfill.
- **Subsequent runs**: pulls the current year only and appends any rows whose
  `(school_id, year)` pair is not already present. The function is idempotent.
- **Cleaning**: numeric fields are stripped of `$`/`,`/spaces; `"NA"` values
  become NULL; `choiceSingle`/`choiceMulti` values are validated against the
  lookup groups; `last_updated` is parsed as ISO 8601. Full rules live at the
  top of `main.py`.
- **Authentication**: DASL `client_id` / `client_secret` are fetched at
  runtime from Google Secret Manager. They are never written to logs.

## Files

- `main.py` — Cloud Function entry point (`run_pipeline`) and pipeline logic
- `requirements.txt` — Python dependencies
- `README.md` — this file

## Required environment variables

| Name                       | Description                                           |
| -------------------------- | ----------------------------------------------------- |
| `GCP_PROJECT_ID`           | Google Cloud project ID                               |
| `BQ_DATASET`               | BigQuery dataset name (must already exist)            |
| `BQ_TABLE`                 | BigQuery table name (created on first run)            |
| `DASL_CLIENT_ID_SECRET`    | Secret Manager secret name holding the DASL client ID |
| `DASL_CLIENT_SECRET_SECRET`| Secret Manager secret name holding the client secret  |

## One-time setup

### 1. Create the BigQuery dataset

```bash
bq --location=US mk --dataset "$GCP_PROJECT_ID:dasl"
```

### 2. Store DASL credentials in Secret Manager

```bash
gcloud secrets create dasl-client-id --replication-policy="automatic"
printf '%s' 'YOUR_CLIENT_ID' | gcloud secrets versions add dasl-client-id --data-file=-

gcloud secrets create dasl-client-secret --replication-policy="automatic"
printf '%s' 'YOUR_CLIENT_SECRET' | gcloud secrets versions add dasl-client-secret --data-file=-
```

### 3. Grant the Cloud Function's runtime service account access

Replace `SA_EMAIL` with the service account the function runs as (default:
`PROJECT_NUMBER-compute@developer.gserviceaccount.com`).

```bash
# Secret Manager read
gcloud secrets add-iam-policy-binding dasl-client-id \
  --member="serviceAccount:SA_EMAIL" --role="roles/secretmanager.secretAccessor"
gcloud secrets add-iam-policy-binding dasl-client-secret \
  --member="serviceAccount:SA_EMAIL" --role="roles/secretmanager.secretAccessor"

# BigQuery write
gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member="serviceAccount:SA_EMAIL" --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member="serviceAccount:SA_EMAIL" --role="roles/bigquery.jobUser"
```

## Deploy

From this directory:

```bash
gcloud functions deploy dasl-sync \
  --gen2 \
  --runtime=python311 \
  --region=us-central1 \
  --source=. \
  --entry-point=run_pipeline \
  --trigger-http \
  --no-allow-unauthenticated \
  --timeout=3600 \
  --memory=1Gi \
  --set-env-vars="GCP_PROJECT_ID=your-project,BQ_DATASET=dasl,BQ_TABLE=school_data,DASL_CLIENT_ID_SECRET=dasl-client-id,DASL_CLIENT_SECRET_SECRET=dasl-client-secret"
```

The first deploy runs the full backfill and can take a while (~200 schools ×
~20 years × 1 request each). A 1-hour timeout is recommended.

## Triggering manually

### From Cloud Console

1. Open **Cloud Functions** → **dasl-sync** → **Testing** tab.
2. Click **Test the function** with an empty body `{}`.
3. The response body contains the summary message; full logs appear in the
   **Logs** tab.

### From the command line

```bash
gcloud functions call dasl-sync --region=us-central1

# Or hit the HTTPS endpoint directly with an auth token:
curl -X POST \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  "$(gcloud functions describe dasl-sync --region=us-central1 --format='value(serviceConfig.uri)')"
```

## Scheduling annual runs with Cloud Scheduler

DASL publishes new data roughly once a year. Schedule the function to run
every week — no-op after the first successful run because duplicate
`(school_id, year)` rows are skipped.

```bash
FUNCTION_URL="$(gcloud functions describe dasl-sync --region=us-central1 --format='value(serviceConfig.uri)')"
SA_EMAIL="PROJECT_NUMBER-compute@developer.gserviceaccount.com"

gcloud scheduler jobs create http dasl-sync-weekly \
  --location=us-central1 \
  --schedule="0 6 * * 1" \
  --time-zone="America/New_York" \
  --http-method=POST \
  --uri="$FUNCTION_URL" \
  --oidc-service-account-email="$SA_EMAIL" \
  --oidc-token-audience="$FUNCTION_URL" \
  --attempt-deadline=3600s
```

The scheduler service account needs `roles/cloudfunctions.invoker` (gen2:
`roles/run.invoker` on the underlying Cloud Run service).

## Monitoring

Logs are written via Python `logging` and are visible in Cloud Logging under
the `dasl-sync` function. Look for the `Pipeline summary:` line for a
machine-readable summary of each run, including cleaning counts (nullified
values, skipped rows, validation warnings).
