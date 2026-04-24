"""Google Cloud Function: DASL -> BigQuery sync pipeline.

Extracts school data from the NAIS DASL RESTful API and loads it into a
star schema in BigQuery, then (re)builds one curated wide view per top-level
variable category for Looker.

Tables written:
  - dim_school         one row per association school
  - dim_variable       one row per DASL variable (includes sub_category_id
                       and an auto-generated snake_case column_name)
  - dim_lookup         one row per (lookup_group_id, key) choice value
  - fact_school_data   one row per (school_id, year, var_id) data point;
                       partitioned by year, clustered by school_id, var_id

Views written (regenerated each run from dim_variable):
  - vw_<category>      one per top-level category, ~100-600 columns,
                       one row per (school_id, year)
"""

from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

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

# Number of reporting years to load, ending with the current calendar year.
BACKFILL_YEARS = int(os.environ.get("BACKFILL_YEARS", "3"))

# Subcategory IDs that return data on the stage server. 19, 20, 21, 28 return
# 500 errors and are skipped. 13, 14, 22, 23, 24 have returned empty in probes
# but we still try them in case newer data lands there.
VALID_SUBCATEGORY_IDS = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
    13, 14, 15, 16, 17, 18,
    22, 23, 24, 25, 26, 27, 29,
]
# Subcategory IDs known to return 500 — skip without retries.
BROKEN_SUBCATEGORY_IDS = {19, 20, 21, 28}

MAX_RETRIES = 5
BASE_BACKOFF_SECONDS = 2.0

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger("dasl_sync")
logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Credentials / Secret Manager
# ---------------------------------------------------------------------------

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "dasl-493214")
SERVICE_ACCOUNT_KEY_PATH = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.join(os.path.dirname(__file__), "dasl-493214-f2b53b1573dc.json"),
)

DASL_CLIENT_ID = os.environ.get("DASL_CLIENT_ID", "tabs")
DASL_CLIENT_SECRET_NAME = os.environ.get(
    "DASL_CLIENT_SECRET_NAME", "dasl-tabs-api-key"
)


def _get_credentials() -> Optional[service_account.Credentials]:
    if SERVICE_ACCOUNT_KEY_PATH and os.path.exists(SERVICE_ACCOUNT_KEY_PATH):
        logger.info("Using service account key: %s", SERVICE_ACCOUNT_KEY_PATH)
        return service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_KEY_PATH,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
    logger.info("No service account key; using Application Default Credentials")
    return None


def _get_secret(project_id: str, secret_name: str) -> str:
    client = secretmanager.SecretManagerServiceClient(credentials=_get_credentials())
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
        """GET with exponential backoff, token refresh on 401, retry on 429/5xx."""
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
                logger.warning("DASL GET %s network error (%s), retry %d/%d in %.1fs",
                               url, exc, attempt, MAX_RETRIES, wait)
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
                logger.warning("DASL GET %s -> %d, retry %d/%d in %.1fs",
                               url, resp.status_code, attempt, MAX_RETRIES, wait)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            if not resp.content:
                return None
            return resp.json()

    @staticmethod
    def _unwrap(payload: Any, key: str) -> List[Dict[str, Any]]:
        if payload is None:
            return []
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            return payload.get(key) or []
        return []

    def get_variable_metadata(self) -> List[Dict[str, Any]]:
        return self._unwrap(self.get(DASL_VARIABLE_METADATA_URL), "variableMetadata")

    def get_lookup_groups(self) -> List[Dict[str, Any]]:
        return self._unwrap(self.get(DASL_LOOKUP_GROUPS_URL), "lookupGroups")

    def get_assoc_schools(self) -> List[Dict[str, Any]]:
        return self._unwrap(self.get(DASL_ASSOC_SCHOOL_URL), "schoolEntries")

    def get_school_subcategory_data(
        self, school_id: str, year: int, sub_category_id: int
    ) -> Optional[Dict[str, Any]]:
        """Return the `schoolEntry` dict for one (school, year, subcategory) slice."""
        url = f"{DASL_ASSOC_SCHOOL_DATA_URL}/{school_id}"
        params = {"year": year, "subCategoryId": sub_category_id}
        try:
            payload = self.get(url, params=params)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status in (404, 204):
                return None
            raise
        if not isinstance(payload, dict):
            return None
        return payload.get("schoolEntry")


# ---------------------------------------------------------------------------
# Helpers: column name generation, value coercion
# ---------------------------------------------------------------------------

_COLUMN_STRIP_RE = re.compile(r"[^a-zA-Z0-9_]")
_NUMERIC_DATA_TYPES = {"integer", "int", "float", "double", "decimal", "currency", "number"}
_CHOICE_DATA_TYPES = {"choicesingle", "choicemulti"}

# Text substitutions applied to question/section text BEFORE sanitization.
# Order matters — applied top to bottom on lowercased input. Anchored to whole
# words/phrases via the surrounding regex assertions to avoid mangling.
# The goal: produce concise, readable snake_case column names.
_LABEL_REPLACEMENTS: List[Tuple[str, str]] = [
    # Drop redundant prefix that just describes the variable group
    (r"\bgrade\s*1\s*to\s*grade\s*12\b[:\s]*", ""),
    # Section-level abbreviations (boarding types) — applied to section text
    (r"\b5[\s-]*day\s+boarding\b", "b5"),
    (r"\b7[\s-]*day\s+boarding\b", "b7"),
    (r"\b5[\s-]*day\s+domestic\s+boarders[\w\s]*", "b5_domestic"),
    (r"\b7[\s-]*day\s+domestic\s+boarders[\w\s]*", "b7_domestic"),
    (r"\bday\s+students\b", "day"),
    # Fee-type compaction
    (r"\btuition\s+and\s+fees\b", "tuition_fees"),
    (r"\btuition\s+only\b", "tuition"),
    (r"\bfees\s+only\b", "fees"),
    # Grade level compaction: "Grade 9" -> "g9", "PK-5" -> "pk5"
    (r"\bgrade\s+(\d+)\b", r"g\1"),
    (r"\bpre[\s-]*kindergarten\b", "pk"),
    (r"\bkindergarten\s+part[\s-]*time\b", "k_pt"),
    (r"\bkindergarten\s+full[\s-]*day\b", "k_full"),
    (r"\bkindergarten\b", "k"),
    (r"\bpre[\s-]*first(?:\s+grade)?\b", "pre_first"),
    (r"\bpreschool\b", "ps"),
    # Trailing parenthetical noise like "(TABS)" or "(through 2018-19)"
    (r"\s*\(tabs\)", ""),
    (r"\s*\(through\s+\d{4}-\d{2,4}\)", "_legacy"),
    # Common verbose phrases
    # Percentage range — run BEFORE "family paying" prefix so word boundaries
    # work correctly on " 60-99%" rather than after underscore prefixing.
    (r"(\d+)\s*[-\s]\s*(\d+)\s*%\s*of\s+tuition", r"\1_\2pct"),
    (r"\bfamily\s+paying\s+", "fam_pay_"),
    (r"\bfull\s+tuition\b", "full"),
    (r"\bno\s+tuition\b", "none"),
    (r"\btotal\s+count\s+of\s+", "count_"),
    # Strip filler
    (r"\b(of|the|for|on|in|at|to|a|an)\b", ""),
]
_LABEL_RES = [(re.compile(p, re.IGNORECASE), r) for p, r in _LABEL_REPLACEMENTS]


def _clean_label_text(text: str) -> str:
    """Apply readability-focused substitutions before snake-casing."""
    s = (text or "").strip().lower()
    for rx, repl in _LABEL_RES:
        s = rx.sub(repl, s)
    return s


def sanitize_column_name(label: str) -> str:
    if not label:
        return ""
    name = label.strip().lower().replace(" ", "_").replace("-", "_")
    name = _COLUMN_STRIP_RE.sub("", name)
    # Collapse runs of underscores from substitution
    name = re.sub(r"_+", "_", name).strip("_")
    name = name[:200]
    if name and name[0].isdigit():
        name = f"v_{name}"
    return name


def _candidate_column_name(label: str) -> str:
    """Derive a compact snake_case name from a 4-level arrow-delimited label.

    Strategy: combine the last meaningful path part (question) with the section
    when sections meaningfully disambiguate (boarding types, day students, etc).
    Apply _clean_label_text first to compress redundant phrasing.
    """
    parts = [p.strip() for p in (label or "").split(" -> ")]
    while len(parts) < 4:
        parts.append("")
    _, _, section, question = parts[:4]

    # Don't let section duplicate subcategory text — e.g. "Tuition and Fees" appears
    # at multiple levels; we only care about it as a real differentiator (boarding etc).
    cleaned_q = _clean_label_text(question)
    cleaned_s = _clean_label_text(section)

    # If section is generic ("Tuition and Fees" repeated, "School Characteristics" etc.)
    # don't prefix; otherwise include it as a disambiguator.
    SKIP_SECTIONS = {
        "tuition_fees", "tuition", "school_characteristics", "school",
        "demographic_variables", "students", "advancement", "financial_aid",
        "admission_attrition", "additional_questions", "",
    }

    base = cleaned_q
    if cleaned_s and cleaned_s not in SKIP_SECTIONS and cleaned_s not in cleaned_q:
        base = f"{cleaned_s}_{cleaned_q}"

    return sanitize_column_name(base)


def build_column_names(variables: List[Dict[str, Any]]) -> Dict[int, str]:
    """varId -> compact snake_case column_name. Collisions get numeric suffixes."""
    used: Dict[str, int] = {}
    result: Dict[int, str] = {}
    for v in variables:
        var_id = v.get("varId")
        if var_id is None:
            continue
        col = _candidate_column_name(v.get("label") or "") or f"var_{var_id}"
        if col in used:
            used[col] += 1
            col = f"{col}_{used[col]}"
        else:
            used[col] = 0
        result[var_id] = col
    return result


def _coerce_numeric(raw: Any) -> Optional[float]:
    if raw is None or raw == "":
        return None
    if isinstance(raw, bool):
        return float(raw)
    if isinstance(raw, (int, float)):
        return float(raw)
    s = re.sub(r"[^0-9.\-eE]", "", str(raw))
    if not s or s in ("-", ".", "-.", ".-"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _coerce_string(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    s = str(raw).strip()
    return s if s else None


def _normalize_timestamp(raw: Any) -> Optional[str]:
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# BigQuery schema
# ---------------------------------------------------------------------------

DIM_SCHOOL_SCHEMA = [
    bigquery.SchemaField("school_id", "STRING", "REQUIRED"),
    bigquery.SchemaField("dasl_school_id", "STRING"),
    bigquery.SchemaField("school_name", "STRING"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("state_code", "STRING"),
    bigquery.SchemaField("cola_index", "FLOAT"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP"),
]

DIM_VARIABLE_SCHEMA = [
    bigquery.SchemaField("var_id", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("subcategory", "STRING"),
    bigquery.SchemaField("section", "STRING"),
    bigquery.SchemaField("question", "STRING"),
    bigquery.SchemaField("label", "STRING"),
    bigquery.SchemaField("data_type", "STRING"),
    bigquery.SchemaField("sub_category_id", "INTEGER"),
    bigquery.SchemaField("lookup_group_id", "INTEGER"),
    bigquery.SchemaField("association_specific", "BOOLEAN"),
    bigquery.SchemaField("column_name", "STRING"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP"),
]

DIM_LOOKUP_SCHEMA = [
    bigquery.SchemaField("lookup_group_id", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("group_name", "STRING"),
    bigquery.SchemaField("lookup_key", "STRING", "REQUIRED"),
    bigquery.SchemaField("lookup_value", "STRING"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP"),
]

FACT_SCHOOL_DATA_SCHEMA = [
    bigquery.SchemaField("school_id", "STRING", "REQUIRED"),
    bigquery.SchemaField("year", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("var_id", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("value_numeric", "FLOAT"),
    bigquery.SchemaField("value_string", "STRING"),
    bigquery.SchemaField("value_choice", "STRING"),
    bigquery.SchemaField("value_choice_label", "STRING"),
    bigquery.SchemaField("is_na", "BOOLEAN"),
    bigquery.SchemaField("sub_category_id", "INTEGER"),
    bigquery.SchemaField("last_updated", "TIMESTAMP"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP"),
]


def _qualify(project: str, dataset: str, table: str) -> str:
    return f"{project}.{dataset}.{table}"


def ensure_dataset(bq: bigquery.Client, project: str, dataset: str, location: str = "US") -> None:
    ref = bigquery.DatasetReference(project, dataset)
    try:
        bq.get_dataset(ref)
    except NotFound:
        logger.info("Creating dataset %s.%s in %s", project, dataset, location)
        ds = bigquery.Dataset(ref)
        ds.location = location
        bq.create_dataset(ds)


def replace_table(
    bq: bigquery.Client,
    table_ref: str,
    rows: List[Dict[str, Any]],
    schema: List[bigquery.SchemaField],
) -> None:
    """Truncate and reload a table via a JSON load job."""
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    logger.info("Replacing %s with %d rows", table_ref, len(rows))
    job = bq.load_table_from_json(rows or [{}][:0], table_ref, job_config=job_config)
    job.result()


def ensure_fact_table(bq: bigquery.Client, table_ref: str) -> None:
    try:
        bq.get_table(table_ref)
        return
    except NotFound:
        pass
    logger.info("Creating fact table %s", table_ref)
    table = bigquery.Table(table_ref, schema=FACT_SCHOOL_DATA_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.YEAR,
        field=None,  # range partitioning set below
    )
    # Range partition on year (integer), clustered by school_id, var_id.
    table.time_partitioning = None
    table.range_partitioning = bigquery.RangePartitioning(
        field="year",
        range_=bigquery.PartitionRange(start=2000, end=2100, interval=1),
    )
    table.clustering_fields = ["school_id", "var_id"]
    bq.create_table(table)


def replace_fact_slices(
    bq: bigquery.Client,
    table_ref: str,
    rows: List[Dict[str, Any]],
    slices: List[Tuple[str, int]],
) -> None:
    """Delete then append rows for the specified (school_id, year) slices."""
    if not slices:
        return
    # DELETE existing rows for the slices we're about to load.
    slice_filter = " OR ".join(
        f"(school_id = @sid_{i} AND year = @yr_{i})" for i in range(len(slices))
    )
    query = f"DELETE FROM `{table_ref}` WHERE {slice_filter}"
    params: List[bigquery.ScalarQueryParameter] = []
    for i, (sid, yr) in enumerate(slices):
        params.append(bigquery.ScalarQueryParameter(f"sid_{i}", "STRING", sid))
        params.append(bigquery.ScalarQueryParameter(f"yr_{i}", "INT64", yr))
    logger.info("Deleting existing fact rows for %d slices", len(slices))
    bq.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()

    if rows:
        job_config = bigquery.LoadJobConfig(
            schema=FACT_SCHOOL_DATA_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        logger.info("Appending %d rows to %s", len(rows), table_ref)
        job = bq.load_table_from_json(rows, table_ref, job_config=job_config)
        job.result()


# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------


def build_dim_school_rows(schools: List[Dict[str, Any]], now_iso: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for s in schools:
        sid = _coerce_string(s.get("schoolId"))
        if not sid:
            continue  # schools without schoolId can't appear in fact_school_data anyway
        rows.append({
            "school_id": sid,
            "dasl_school_id": _coerce_string(s.get("daslSchoolId")),
            "school_name": _coerce_string(s.get("schoolName")),
            "city": _coerce_string(s.get("city")),
            "state_code": _coerce_string(s.get("stateCode")),
            "cola_index": s.get("colaIndex"),
            "loaded_at": now_iso,
        })
    return rows


def build_dim_variable_rows(
    variables: List[Dict[str, Any]],
    column_names: Dict[int, str],
    varid_to_subcat: Dict[int, int],
    now_iso: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for v in variables:
        vid = v.get("varId")
        if vid is None:
            continue
        label = v.get("label") or ""
        parts = [p.strip() for p in label.split(" -> ")]
        parts += [""] * (4 - len(parts))
        rows.append({
            "var_id": vid,
            "category": parts[0],
            "subcategory": parts[1],
            "section": parts[2],
            "question": parts[3],
            "label": label,
            "data_type": _coerce_string(v.get("dataType")),
            "sub_category_id": varid_to_subcat.get(vid),
            "lookup_group_id": v.get("lookupGroupId"),
            "association_specific": v.get("associationSpecific"),
            "column_name": column_names.get(vid),
            "loaded_at": now_iso,
        })
    return rows


def build_dim_lookup_rows(lookup_groups: List[Dict[str, Any]], now_iso: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for g in lookup_groups:
        gid = g.get("lookupGroupId")
        gname = _coerce_string(g.get("name"))
        items = g.get("items") or g.get("values") or g.get("lookups") or []
        for item in items:
            if isinstance(item, dict):
                rows.append({
                    "lookup_group_id": gid,
                    "group_name": gname,
                    "lookup_key": _coerce_string(item.get("key") or item.get("id")),
                    "lookup_value": _coerce_string(item.get("value") or item.get("label")),
                    "loaded_at": now_iso,
                })
    return rows


def build_fact_row(
    school_id: str,
    year: int,
    data_point: Dict[str, Any],
    sub_category_id: int,
    last_updated: Optional[str],
    data_type_by_varid: Dict[int, str],
    lookup_value_map: Dict[Tuple[int, str], str],
    lookup_group_by_varid: Dict[int, int],
    now_iso: str,
) -> Optional[Dict[str, Any]]:
    vid = data_point.get("varId")
    if vid is None:
        return None
    raw = data_point.get("value")
    is_na = bool(data_point.get("isNA"))
    dtype = (data_type_by_varid.get(vid) or "").lower()

    value_numeric: Optional[float] = None
    value_string: Optional[str] = None
    value_choice: Optional[str] = None
    value_choice_label: Optional[str] = None

    if not is_na and raw not in (None, ""):
        if dtype in _NUMERIC_DATA_TYPES:
            value_numeric = _coerce_numeric(raw)
        elif dtype in _CHOICE_DATA_TYPES:
            value_choice = _coerce_string(raw)
            if value_choice is not None:
                grp = lookup_group_by_varid.get(vid)
                if grp is not None:
                    # For choiceMulti, value may be comma-separated — resolve each.
                    parts = [p.strip() for p in value_choice.split(",") if p.strip()]
                    labels = [
                        lookup_value_map.get((grp, p), p) for p in parts
                    ]
                    value_choice_label = ", ".join(labels) if labels else None
        else:
            value_string = _coerce_string(raw)

    return {
        "school_id": school_id,
        "year": year,
        "var_id": vid,
        "value_numeric": value_numeric,
        "value_string": value_string,
        "value_choice": value_choice,
        "value_choice_label": value_choice_label,
        "is_na": is_na,
        "sub_category_id": sub_category_id,
        "last_updated": last_updated,
        "loaded_at": now_iso,
    }


# ---------------------------------------------------------------------------
# View generator
# ---------------------------------------------------------------------------


def _safe_view_suffix(category: str) -> str:
    suffix = sanitize_column_name(category) or "misc"
    return f"vw_{suffix}"


def rebuild_category_views(
    bq: bigquery.Client,
    project: str,
    dataset: str,
    variables: List[Dict[str, Any]],
    column_names: Dict[int, str],
    varid_to_subcat: Dict[int, int],
) -> List[str]:
    """One view per top-level category. Returns list of view IDs created."""
    fact = f"`{_qualify(project, dataset, 'fact_school_data')}`"
    school = f"`{_qualify(project, dataset, 'dim_school')}`"

    # Group varIds by top-level category; include only those that applied on probe
    by_category: Dict[str, List[Dict[str, Any]]] = {}
    for v in variables:
        vid = v.get("varId")
        if vid is None or vid not in varid_to_subcat:
            continue  # only expose variables we actually pull
        label = v.get("label") or ""
        cat = (label.split(" -> ")[0] if label else "").strip() or "Misc"
        by_category.setdefault(cat, []).append(v)

    created: List[str] = []
    for category, vars_in_cat in by_category.items():
        view_name = _safe_view_suffix(category)
        view_id = _qualify(project, dataset, view_name)
        select_exprs: List[str] = []
        used: Dict[str, int] = {}
        for v in vars_in_cat:
            vid = v.get("varId")
            col = column_names.get(vid)
            if not col:
                continue
            if col in used:
                used[col] += 1
                col = f"{col}_{used[col]}"
            else:
                used[col] = 0
            dtype = (v.get("dataType") or "").lower()
            if dtype in _NUMERIC_DATA_TYPES:
                expr = f"MAX(IF(f.var_id = {vid}, f.value_numeric, NULL)) AS {col}"
            elif dtype in _CHOICE_DATA_TYPES:
                expr = (
                    f"MAX(IF(f.var_id = {vid}, "
                    f"COALESCE(f.value_choice_label, f.value_choice), NULL)) AS {col}"
                )
            else:
                expr = f"MAX(IF(f.var_id = {vid}, f.value_string, NULL)) AS {col}"
            select_exprs.append("  " + expr)

        if not select_exprs:
            continue

        var_ids = ", ".join(str(v["varId"]) for v in vars_in_cat if v.get("varId") is not None)
        sql = (
            f"CREATE OR REPLACE VIEW `{view_id}` AS\n"
            f"SELECT\n"
            f"  f.school_id,\n"
            f"  s.school_name,\n"
            f"  s.state_code,\n"
            f"  f.year,\n"
            + ",\n".join(select_exprs) + "\n"
            f"FROM {fact} f\n"
            f"LEFT JOIN {school} s USING (school_id)\n"
            f"WHERE f.var_id IN ({var_ids})\n"
            f"GROUP BY f.school_id, s.school_name, s.state_code, f.year;\n"
        )
        logger.info("Creating view %s (%d columns)", view_id, len(select_exprs))
        bq.query(sql).result()
        created.append(view_id)
    return created


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------


def run(project_id: str, dataset: str) -> Dict[str, Any]:
    logger.info("Starting DASL -> BigQuery pipeline (base URL: %s)", DASL_BASE_URL)
    now_iso = datetime.now(timezone.utc).isoformat()
    current_year = datetime.now(timezone.utc).year
    years = list(range(current_year - BACKFILL_YEARS + 1, current_year + 1))
    logger.info("Loading years: %s", years)

    credentials = _get_credentials()
    bq = bigquery.Client(project=project_id, credentials=credentials)
    ensure_dataset(bq, project_id, dataset)

    # --- Auth + metadata --------------------------------------------------
    client_secret = _get_secret(project_id, DASL_CLIENT_SECRET_NAME)
    dasl = DaslClient(DASL_CLIENT_ID, client_secret)
    dasl.authenticate()

    variables = dasl.get_variable_metadata()
    lookup_groups = dasl.get_lookup_groups()
    schools = dasl.get_assoc_schools()
    mapped_schools = [s for s in schools if _coerce_string(s.get("schoolId"))]
    logger.info(
        "Metadata: %d variables, %d lookup groups, %d schools (%d mapped)",
        len(variables), len(lookup_groups), len(schools), len(mapped_schools),
    )

    # Convenience indexes
    column_names = build_column_names(variables)
    data_type_by_varid = {v["varId"]: (v.get("dataType") or "") for v in variables if v.get("varId") is not None}
    lookup_group_by_varid = {
        v["varId"]: v["lookupGroupId"]
        for v in variables
        if v.get("varId") is not None and v.get("lookupGroupId") is not None
    }
    lookup_value_map: Dict[Tuple[int, str], str] = {}
    for g in lookup_groups:
        gid = g.get("lookupGroupId")
        if gid is None:
            continue
        for item in g.get("items") or []:
            if isinstance(item, dict):
                key = item.get("key") or item.get("id")
                val = item.get("value") or item.get("label") or key
                if key is not None:
                    lookup_value_map[(gid, str(key))] = str(val)

    # --- Fetch data points -----------------------------------------------
    varid_to_subcat: Dict[int, int] = {}
    fact_rows: List[Dict[str, Any]] = []
    slices: List[Tuple[str, int]] = []
    api_calls = 0
    api_errors = 0

    for school in mapped_schools:
        sid = school["schoolId"]
        for year in years:
            slices.append((sid, year))
            last_updated_for_slice: Optional[str] = None
            for scid in VALID_SUBCATEGORY_IDS:
                if scid in BROKEN_SUBCATEGORY_IDS:
                    continue
                api_calls += 1
                try:
                    entry = dasl.get_school_subcategory_data(sid, year, scid)
                except Exception as exc:  # noqa: BLE001
                    api_errors += 1
                    logger.warning("school=%s year=%d subcat=%d failed: %s", sid, year, scid, exc)
                    continue
                if not entry:
                    continue
                last_updated = _normalize_timestamp(entry.get("lastUpdated"))
                if last_updated and (not last_updated_for_slice or last_updated > last_updated_for_slice):
                    last_updated_for_slice = last_updated
                for dp in entry.get("dataPoints") or []:
                    vid = dp.get("varId")
                    if vid is not None:
                        varid_to_subcat.setdefault(vid, scid)
                    row = build_fact_row(
                        school_id=sid,
                        year=year,
                        data_point=dp,
                        sub_category_id=scid,
                        last_updated=last_updated,
                        data_type_by_varid=data_type_by_varid,
                        lookup_value_map=lookup_value_map,
                        lookup_group_by_varid=lookup_group_by_varid,
                        now_iso=now_iso,
                    )
                    if row is not None:
                        fact_rows.append(row)
        logger.info("Finished school %s: running fact_rows=%d", sid, len(fact_rows))

    logger.info("API calls: %d (errors: %d). Fact rows: %d",
                api_calls, api_errors, len(fact_rows))

    # --- Write dim tables ------------------------------------------------
    replace_table(
        bq, _qualify(project_id, dataset, "dim_school"),
        build_dim_school_rows(schools, now_iso), DIM_SCHOOL_SCHEMA,
    )
    replace_table(
        bq, _qualify(project_id, dataset, "dim_variable"),
        build_dim_variable_rows(variables, column_names, varid_to_subcat, now_iso),
        DIM_VARIABLE_SCHEMA,
    )
    replace_table(
        bq, _qualify(project_id, dataset, "dim_lookup"),
        build_dim_lookup_rows(lookup_groups, now_iso), DIM_LOOKUP_SCHEMA,
    )

    # --- Write fact table ------------------------------------------------
    fact_ref = _qualify(project_id, dataset, "fact_school_data")
    ensure_fact_table(bq, fact_ref)
    replace_fact_slices(bq, fact_ref, fact_rows, slices)

    # --- Rebuild category views ------------------------------------------
    views = rebuild_category_views(
        bq, project_id, dataset, variables, column_names, varid_to_subcat,
    )

    summary = {
        "years": years,
        "schools_total": len(schools),
        "schools_mapped": len(mapped_schools),
        "variables": len(variables),
        "variables_with_data": len(varid_to_subcat),
        "lookup_groups": len(lookup_groups),
        "fact_rows": len(fact_rows),
        "api_calls": api_calls,
        "api_errors": api_errors,
        "views_created": views,
    }
    logger.info("Pipeline summary: %s", summary)
    return summary


# ---------------------------------------------------------------------------
# Cloud Function entry point
# ---------------------------------------------------------------------------


@functions_framework.http
def run_pipeline(request):  # noqa: ARG001
    project_id = os.environ.get("GCP_PROJECT_ID", GCP_PROJECT_ID)
    dataset = os.environ.get("BQ_DATASET", "dasl")
    try:
        summary = run(project_id=project_id, dataset=dataset)
        msg = (
            f"Pipeline complete. Years: {summary['years']}, "
            f"schools: {summary['schools_mapped']}/{summary['schools_total']}, "
            f"variables with data: {summary['variables_with_data']}/{summary['variables']}, "
            f"fact rows: {summary['fact_rows']}, "
            f"API calls: {summary['api_calls']} (errors: {summary['api_errors']}), "
            f"views: {len(summary['views_created'])}."
        )
        return (msg, 200)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Pipeline failed")
        return (f"Pipeline failed: {exc}", 500)
