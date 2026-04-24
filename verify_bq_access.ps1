# verify_bq_access.ps1
# Place in the same folder as dasl-493214-f2b53b1573dc.json, then run:
#   .\verify_bq_access.ps1

$KeyFile   = Join-Path $PSScriptRoot "dasl-493214-f2b53b1573dc.json"
$ProjectId = "dasl-493214"

Write-Host "`nChecking Python..." -ForegroundColor Cyan
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Host "  ERROR: python not found. Install from https://python.org" -ForegroundColor Red
    exit 1
}

Write-Host "Installing dependencies..." -ForegroundColor Cyan
python -m pip install --quiet google-cloud-bigquery 2>&1 | Out-Null

Write-Host "Verifying credentials and BigQuery access...`n" -ForegroundColor Cyan

$PyScript = @"
from google.oauth2 import service_account
from google.cloud import bigquery
import sys

KEY_FILE = r"$KeyFile"
PROJECT  = "$ProjectId"

try:
    creds = service_account.Credentials.from_service_account_file(
        KEY_FILE, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    print(f"  [OK] Credentials loaded")
    print(f"       Service account : {creds.service_account_email}")
    print(f"       Project         : {PROJECT}")
except Exception as e:
    print(f"  [FAIL] Could not load credentials: {e}")
    sys.exit(1)

try:
    client   = bigquery.Client(project=PROJECT, credentials=creds)
    datasets = [d.dataset_id for d in client.list_datasets()]
    if datasets:
        print(f"  [OK] BigQuery access confirmed")
        print(f"       Existing datasets: {datasets}")
    else:
        print(f"  [OK] BigQuery access confirmed (no datasets yet - run setup first)")
except Exception as e:
    print(f"  [FAIL] BigQuery error: {e}")
    sys.exit(1)
"@

$TmpFile = [System.IO.Path]::GetTempFileName() + ".py"
$PyScript | Set-Content -Path $TmpFile -Encoding UTF8
python $TmpFile
Remove-Item $TmpFile
