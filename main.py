"""Google Cloud Function: DASL -> BigQuery sync pipeline.

Extracts school data from the NAIS DASL RESTful API and loads it into
BigQuery in a wide/columnar format (one row per school per year).
"""

from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import functions_framework
import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, secretmanager
from google.oauth2 import service_account

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DASL_BASE_URL = os.environ.get(
    "DASL_BASE_URL", "https://stagedaslwebapi.azurewebsites.net"
)
DASL_AUTH_URL = f"{DASL_BASE_URL}/auth"
DASL_VARIABLE_METADATA_URL = f"{DASL_BASE_URL}/variableMetadata"
DASL_LOOKUP_GROUPS_URL = f"{DASL_BASE_URL}/lookupGroups"
DASL_ASSOC_SCHOOL_URL = f"{DASL_BASE_URL}/assocSchool"
DASL_ASSOC_SCHOOL_DATA_URL = f"{DASL_BASE_URL}/assocSchoolData"

DASL_ACCEPT_HEADER = "Application/vnd.nais.dasl.api+json; version=1"

# Historical backfill window. DASL datasets start in the early 2000s; this
# range is generous enough to capture anything available.
BACKFILL_START_YEAR = 2005
YEAR_MIN = 1990
YEAR_MAX = 2050

MAX_RETRIES = 5
BASE_BACKOFF_SECONDS = 2.0

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dasl_sync")
logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Credentials / config
# ---------------------------------------------------------------------------


GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "dasl-493214")
SERVICE_ACCOUNT_KEY_PATH = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.join(os.path.dirname(__file__), "dasl-493214-f2b53b1573dc.json"),
)

# DASL client_id for Tabs
DASL_CLIENT_ID = os.environ.get("DASL_CLIENT_ID", "tabs")

# Name of the Secret Manager secret holding the Tabs API key (client_secret)
DASL_CLIENT_SECRET_NAME = os.environ.get(
    "DASL_CLIENT_SECRET_NAME", "dasl-tabs-api-key"
)


def _get_credentials() -> service_account.Credentials:
    """Load service account credentials from the JSON key file."""
    return service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_KEY_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )


def _get_secret(project_id: str, secret_name: str) -> str:
    """Fetch the latest version of a Secret Manager secret."""
    credentials = _get_credentials()
    client = secretmanager.SecretManagerServiceClient(credentials=credentials)
    resource = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": resource})
    return response.payload.data.decode("UTF-8").strip()


# ---------------------------------------------------------------------------
# DASL API client
# ---------------------------------------------------------------------------


class DaslClient:
    def __init__(self, client_id: str, client_secret: str):
        self._client_id = client_id
        self._client_secret = client_secret
        self._token: Optional[str] = None
        self._session = requests.Session()

    def authenticate(self) -> None:
        logger.info("Authenticating with DASL API")
        body = {
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "grant_type": "client_credentials",
            "scope": "public",
        }
        resp = self._session.post(DASL_AUTH_URL, data=body, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        token = payload.get("access_token") or payload.get("accessToken")
        if not token:
            raise RuntimeError(f"DASL auth response missing access_token: {payload}")
        self._token = token
        logger.info("DASL authentication successful")

    def _headers(self) -> Dict[str, str]:
        if not self._token:
            self.authenticate()
        return {
            "Authorization": f"Bearer {self._token}",
            "Accept": DASL_ACCEPT_HEADER,
        }

    def get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """GET with exponential backoff, token refresh on 401, and rate-limit handling."""
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = self._session.get(
                    url, headers=self._headers(), params=params, timeout=60
                )
            except requests.RequestException as exc:
                if attempt >= MAX_RETRIES:
                    raise
                wait = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                logger.warning(
                    "DASL GET %s network error (%s), retry %d/%d in %.1fs",
                    url, exc, attempt, MAX_RETRIES, wait,
                )
                time.sleep(wait)
                continue

            if resp.status_code == 401:
                logger.warning("DASL returned 401, re-authenticating")
                self._token = None
                self.authenticate()
                if attempt >= MAX_RETRIES:
                    resp.raise_for_status()
                continue

            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                if attempt >= MAX_RETRIES:
                    resp.raise_for_status()
                retry_after = resp.headers.get("Retry-After")
                try:
                    wait = float(retry_after) if retry_after else BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                except ValueError:
                    wait = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                logger.warning(
                    "DASL GET %s -> %d, retry %d/%d in %.1fs",
                    url, resp.status_code, attempt, MAX_RETRIES, wait,
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()
            if not resp.content:
                return None
            return resp.json()

    def get_variable_metadata(self) -> List[Dict[str, Any]]:
        return self.get(DASL_VARIABLE_METADATA_URL) or []

    def get_lookup_groups(self) -> List[Dict[str, Any]]:
        return self.get(DASL_LOOKUP_GROUPS_URL) or []

    def get_assoc_schools(self) -> List[Dict[str, Any]]:
        return self.get(DASL_ASSOC_SCHOOL_URL) or []

    def get_school_data(self, school_id: str, year: Optional[int] = None) -> Optional[Dict[str, Any]]:
        url = f"{DASL_ASSOC_SCHOOL_DATA_URL}/{school_id}"
        params = {"year": year} if year is not None else None
        try:
            return self.get(url, params=params)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status in (404, 204):
                return None
            raise


# ---------------------------------------------------------------------------
# Helpers: column name sanitization, value cleaning
# ---------------------------------------------------------------------------


_COLUMN_STRIP_RE = re.compile(r"[^a-zA-Z0-9_]")


def sanitize_column_name(label: str) -> str:
    if label is None:
        return ""
    name = label.strip().lower()
    name = name.replace(" ", "_").replace("-", "_")
    name = _COLUMN_STRIP_RE.sub("", name)
    name = name[:300]
    if name and name[0].isdigit():
        name = f"v_{name}"
    return name


def build_column_map(
    variable_metadata: List[Dict[str, Any]],
) -> Tuple[Dict[str, str], Dict[str, Dict[str, Any]]]:
    """Return (var_id -> column_name, var_id -> metadata dict).

    Collisions on sanitized column names are disambiguated with a numeric suffix.
    """
    var_to_col: Dict[str, str] = {}
    var_to_meta: Dict[str, Dict[str, Any]] = {}
    used: Dict[str, int] = {}
    for meta in variable_metadata:
        var_id = str(meta.get("varId"))
        label = meta.get("label") or var_id
        col = sanitize_column_name(label) or f"v_{var_id}"
        if col in used:
            used[col] += 1
            col = f"{col}_{used[col]}"
        else:
            used[col] = 0
        var_to_col[var_id] = col
        var_to_meta[var_id] = meta
    return var_to_col, var_to_meta


def build_lookup_index(
    lookup_groups: List[Dict[str, Any]],
) -> Dict[str, set]:
    """Index: lookupGroupId -> set of valid keys (as strings)."""
    index: Dict[str, set] = {}
    for group in lookup_groups:
        gid = str(group.get("lookupGroupId") or group.get("id") or "")
        if not gid:
            continue
        items = group.get("items") or group.get("values") or group.get("lookups") or []
        keys: set = set()
        for item in items:
            if isinstance(item, dict):
                key = item.get("key") or item.get("value") or item.get("id")
            else:
                key = item
            if key is not None:
                keys.add(str(key).strip())
        index[gid] = keys
    return index


class CleaningStats:
    def __init__(self) -> None:
        self.nullified = 0
        self.skipped_rows = 0
        self.warnings = 0

    def as_dict(self) -> Dict[str, int]:
        return {
            "nullified": self.nullified,
            "skipped_rows": self.skipped_rows,
            "warnings": self.warnings,
        }


def _is_na(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip().upper() == "NA":
        return True
    return False


def _clean_numeric(value: Any) -> Optional[float]:
    if _is_na(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = re.sub(r"[^0-9.\-eE]", "", str(value))
    if not s or s in ("-", ".", "-.", ".-"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _clean_string(value: Any) -> Optional[str]:
    if _is_na(value):
        return None
    s = str(value).strip()
    return s if s else None


def _clean_timestamp(value: Any) -> Optional[str]:
    if _is_na(value):
        return None
    s = str(value).strip()
    if not s:
        return None
    # Normalize trailing Z to +00:00 so fromisoformat accepts it.
    candidate = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(candidate)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except ValueError:
        return None


def clean_data_point(
    var_id: str,
    raw_value: Any,
    var_meta: Dict[str, Any],
    lookup_index: Dict[str, set],
    stats: CleaningStats,
) -> Any:
    data_type = (var_meta.get("dataType") or "").strip()
    lookup_group_id = str(var_meta.get("lookupGroupId") or "")

    if _is_na(raw_value):
        return None

    dt_lower = data_type.lower()

    if dt_lower in ("integer", "int"):
        n = _clean_numeric(raw_value)
        if n is None:
            stats.nullified += 1
            return None
        return int(n)

    if dt_lower in ("float", "double", "decimal", "currency", "number"):
        n = _clean_numeric(raw_value)
        if n is None:
            stats.nullified += 1
            return None
        return n

    if dt_lower == "choicesingle":
        s = _clean_string(raw_value)
        if s is None:
            return None
        valid = lookup_index.get(lookup_group_id)
        if valid is not None and s not in valid:
            logger.warning(
                "choiceSingle value '%s' not in lookup group %s for varId %s",
                s, lookup_group_id, var_id,
            )
            stats.warnings += 1
            stats.nullified += 1
            return None
        return s

    if dt_lower == "choicemulti":
        s = _clean_string(raw_value)
        if s is None:
            return None
        valid = lookup_index.get(lookup_group_id)
        parts = [p.strip() for p in s.split(",") if p.strip()]
        cleaned_parts: List[str] = []
        for p in parts:
            if valid is not None and p not in valid:
                logger.warning(
                    "choiceMulti value '%s' not in lookup group %s for varId %s",
                    p, lookup_group_id, var_id,
                )
                stats.warnings += 1
                continue
            cleaned_parts.append(p)
        if not cleaned_parts:
            return None
        return ",".join(cleaned_parts)

    # Default: treat as string
    return _clean_string(raw_value)


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------

BASE_COLUMNS: List[Tuple[str, str]] = [
    ("school_id", "STRING"),
    ("school_name", "STRING"),
    ("year", "INTEGER"),
    ("last_updated", "TIMESTAMP"),
]


def _bq_type_for_data_type(data_type: str) -> str:
    dt = (data_type or "").lower()
    if dt in ("integer", "int"):
        return "INTEGER"
    if dt in ("float", "double", "decimal", "currency", "number"):
        return "FLOAT"
    return "STRING"


def build_table_schema(
    var_to_col: Dict[str, str],
    var_to_meta: Dict[str, Dict[str, Any]],
) -> List[bigquery.SchemaField]:
    schema = [bigquery.SchemaField(name, type_) for name, type_ in BASE_COLUMNS]
    seen = {name for name, _ in BASE_COLUMNS}
    for var_id, col in var_to_col.items():
        if col in seen:
            continue
        seen.add(col)
        bq_type = _bq_type_for_data_type(var_to_meta[var_id].get("dataType", ""))
        schema.append(bigquery.SchemaField(col, bq_type))
    return schema


def ensure_table(
    bq: bigquery.Client,
    table_ref: str,
    schema: List[bigquery.SchemaField],
) -> Tuple[bigquery.Table, bool]:
    """Return (table, created_now). Adds any new columns to an existing table."""
    try:
        table = bq.get_table(table_ref)
        existing = {f.name for f in table.schema}
        new_fields = [f for f in schema if f.name not in existing]
        if new_fields:
            logger.info("Adding %d new columns to %s", len(new_fields), table_ref)
            table.schema = list(table.schema) + new_fields
            table = bq.update_table(table, ["schema"])
        return table, False
    except NotFound:
        logger.info("Creating BigQuery table %s", table_ref)
        table = bigquery.Table(table_ref, schema=schema)
        table = bq.create_table(table)
        return table, True


def fetch_existing_keys(bq: bigquery.Client, table_ref: str) -> set:
    query = f"SELECT school_id, year FROM `{table_ref}`"
    keys: set = set()
    for row in bq.query(query).result():
        keys.add((row["school_id"], row["year"]))
    return keys


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def _discover_years(
    client: DaslClient, schools: List[Dict[str, Any]]
) -> List[int]:
    """Discover the set of years with data by probing a sample school across the backfill window."""
    current_year = datetime.now(timezone.utc).year
    years_available: set = set()
    sample = schools[: min(5, len(schools))]
    for school in sample:
        sid = str(school.get("schoolId"))
        for y in range(BACKFILL_START_YEAR, current_year + 1):
            try:
                payload = client.get_school_data(sid, year=y)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Probe failed for school %s year %s: %s", sid, y, exc)
                continue
            if payload and payload.get("dataPoints"):
                years_available.add(y)
    if not years_available:
        # Fall back to the full window
        return list(range(BACKFILL_START_YEAR, current_year + 1))
    return sorted(years_available)


def build_row(
    payload: Dict[str, Any],
    var_to_col: Dict[str, str],
    var_to_meta: Dict[str, Dict[str, Any]],
    lookup_index: Dict[str, set],
    stats: CleaningStats,
) -> Optional[Dict[str, Any]]:
    school_id = _clean_string(payload.get("schoolId"))
    school_name = _clean_string(payload.get("schoolName"))
    if not school_id:
        logger.warning("Skipping row with empty school_id: %s", payload)
        stats.skipped_rows += 1
        stats.warnings += 1
        return None

    year_raw = payload.get("year")
    try:
        year = int(year_raw)
    except (TypeError, ValueError):
        logger.warning("Skipping row with unparseable year %r for school %s", year_raw, school_id)
        stats.skipped_rows += 1
        stats.warnings += 1
        return None
    if year < YEAR_MIN or year > YEAR_MAX:
        logger.warning("Skipping row with out-of-range year %d for school %s", year, school_id)
        stats.skipped_rows += 1
        stats.warnings += 1
        return None

    last_updated = _clean_timestamp(payload.get("lastUpdated"))
    if payload.get("lastUpdated") and last_updated is None:
        logger.warning("Unparseable lastUpdated %r for school %s/%d", payload.get("lastUpdated"), school_id, year)
        stats.warnings += 1

    row: Dict[str, Any] = {
        "school_id": school_id,
        "school_name": school_name,
        "year": year,
        "last_updated": last_updated,
    }

    for dp in payload.get("dataPoints") or []:
        var_id = str(dp.get("varId"))
        col = var_to_col.get(var_id)
        if not col:
            continue  # unknown variable (not in metadata)
        meta = var_to_meta.get(var_id, {})
        row[col] = clean_data_point(var_id, dp.get("value"), meta, lookup_index, stats)

    return row


def deduplicate_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep the row with the most recent last_updated per (school_id, year)."""
    best: Dict[Tuple[str, int], Dict[str, Any]] = {}
    for row in rows:
        key = (row["school_id"], row["year"])
        existing = best.get(key)
        if existing is None:
            best[key] = row
            continue
        a = row.get("last_updated") or ""
        b = existing.get("last_updated") or ""
        if a > b:
            best[key] = row
    return list(best.values())


def run(
    project_id: str,
    dataset: str,
    table: str,
) -> Dict[str, Any]:
    logger.info("Starting DASL -> BigQuery pipeline (base URL: %s)", DASL_BASE_URL)

    client_secret = _get_secret(project_id, DASL_CLIENT_SECRET_NAME)

    dasl = DaslClient(DASL_CLIENT_ID, client_secret)
    dasl.authenticate()

    logger.info("Fetching variable metadata")
    variable_metadata = dasl.get_variable_metadata()
    logger.info("Fetched %d variables", len(variable_metadata))
    var_to_col, var_to_meta = build_column_map(variable_metadata)

    logger.info("Fetching lookup groups")
    lookup_groups = dasl.get_lookup_groups()
    lookup_index = build_lookup_index(lookup_groups)
    logger.info("Indexed %d lookup groups", len(lookup_index))

    logger.info("Fetching association schools")
    schools = dasl.get_assoc_schools()
    logger.info("Fetched %d schools", len(schools))

    credentials = _get_credentials()
    bq = bigquery.Client(project=project_id, credentials=credentials)
    table_ref = f"{project_id}.{dataset}.{table}"
    schema = build_table_schema(var_to_col, var_to_meta)
    bq_table, created_now = ensure_table(bq, table_ref, schema)

    if created_now:
        logger.info("New table created — running full historical backfill")
        years = _discover_years(dasl, schools)
        existing_keys: set = set()
    else:
        logger.info("Existing table — running current-year append")
        years = [datetime.now(timezone.utc).year]
        existing_keys = fetch_existing_keys(bq, table_ref)

    logger.info("Years to load: %s", years)

    stats = CleaningStats()
    collected_rows: List[Dict[str, Any]] = []

    for idx, school in enumerate(schools, start=1):
        sid = str(school.get("schoolId"))
        if not sid:
            logger.warning("School entry missing schoolId, skipping: %s", school)
            stats.skipped_rows += 1
            continue
        for year in years:
            try:
                payload = dasl.get_school_data(sid, year=year)
            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to fetch school %s year %s: %s", sid, year, exc)
                stats.warnings += 1
                continue
            if not payload:
                continue
            # Ensure school metadata present in payload
            payload.setdefault("schoolId", sid)
            payload.setdefault("schoolName", school.get("schoolName"))
            payload.setdefault("year", year)
            row = build_row(payload, var_to_col, var_to_meta, lookup_index, stats)
            if row is not None:
                collected_rows.append(row)
        if idx % 25 == 0:
            logger.info("Processed %d/%d schools", idx, len(schools))

    logger.info("Collected %d raw rows", len(collected_rows))
    deduped = deduplicate_rows(collected_rows)
    logger.info("After in-batch dedup: %d rows", len(deduped))

    # Skip rows already present in the table
    to_load = [
        r for r in deduped if (r["school_id"], r["year"]) not in existing_keys
    ]
    skipped_existing = len(deduped) - len(to_load)
    if skipped_existing:
        logger.info("Skipped %d rows already present in BigQuery", skipped_existing)

    loaded = 0
    if to_load:
        # Ensure every row has all schema columns (fill missing with None)
        all_cols = [f.name for f in bq_table.schema]
        normalized = []
        for r in to_load:
            normalized.append({c: r.get(c) for c in all_cols})

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=list(bq_table.schema),
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        logger.info("Loading %d rows into %s", len(normalized), table_ref)
        load_job = bq.load_table_from_json(normalized, table_ref, job_config=job_config)
        load_job.result()
        loaded = len(normalized)

    summary = {
        "schools": len(schools),
        "years": len(years),
        "rows_loaded": loaded,
        "rows_skipped_existing": skipped_existing,
        "cleaning": stats.as_dict(),
        "created_table": created_now,
    }
    logger.info("Pipeline summary: %s", summary)
    return summary


# ---------------------------------------------------------------------------
# Cloud Function entry point
# ---------------------------------------------------------------------------


@functions_framework.http
def run_pipeline(request):  # noqa: ARG001  (request unused; triggered by HTTP)
    project_id = os.environ.get("GCP_PROJECT_ID", GCP_PROJECT_ID)
    dataset = os.environ.get("BQ_DATASET", "dasl")
    table = os.environ.get("BQ_TABLE", "school_data")

    try:
        summary = run(
            project_id=project_id,
            dataset=dataset,
            table=table,
        )
        message = (
            f"Pipeline complete: {summary['schools']} schools, "
            f"{summary['years']} years, {summary['rows_loaded']} rows loaded "
            f"({summary['rows_skipped_existing']} already present). "
            f"Cleaning: {summary['cleaning']['nullified']} nullified, "
            f"{summary['cleaning']['skipped_rows']} skipped rows, "
            f"{summary['cleaning']['warnings']} warnings."
        )
        return (message, 200)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Pipeline failed")
        return (f"Pipeline failed: {exc}", 500)
