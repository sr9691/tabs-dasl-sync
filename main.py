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
    "DASL_BASE_URL", "https://daslwebapi.azurewebsites.net"
)
DASL_AUTH_URL = f"{DASL_BASE_URL}/auth"
DASL_VARIABLE_METADATA_URL = f"{DASL_BASE_URL}/variableMetadata"
DASL_LOOKUP_GROUPS_URL = f"{DASL_BASE_URL}/lookupGroups"
DASL_ASSOC_SCHOOL_URL = f"{DASL_BASE_URL}/assocSchool"
DASL_ASSOC_SCHOOL_DATA_URL = f"{DASL_BASE_URL}/assocSchoolData"

DASL_ACCEPT_HEADER = "Application/vnd.nais.dasl.api+json; version=1"

# Number of reporting years to load, ending with the current calendar year.
BACKFILL_YEARS = int(os.environ.get("BACKFILL_YEARS", "1"))

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
    # --- Parenthetical noise (run first so later patterns aren't confused) ---
    # Association acronym lists like "(ISACS,EMA,VAIS,NJAIS,NYSAIS,TABS)"
    (r"\s*\(\s*[A-Z]{2,8}(?:\s*[,/]\s*[A-Z]{2,8})+\s*\)", ""),
    # Solo association tags
    (r"\s*\((?:tabs|isacs|ema|vais|njais|nysais|nais)\)", ""),
    # Year-range qualifiers
    (r"\s*\(through\s+\d{4}-\d{2,4}\)", ""),
    (r"\s*\(\s*\d{4}-\d{2,4}\s+forward\s*\)", "_recent"),
    (r"\s*\(\s*previous\s+years?\s*\)", ""),
    (r"\s*\(\s*previous\s+year\s*\)", ""),

    # --- Domain-specific phrasing ---
    # Drop redundant prefix that just describes the variable group
    (r"\bgrade\s*1\s*to\s*grade\s*12\b[:\s]*", ""),
    # Boarding sections
    (r"\b5[\s-]*day\s+boarding\b", "b5"),
    (r"\b7[\s-]*day\s+boarding\b", "b7"),
    (r"\b5[\s-]*day\s+domestic\s+boarders[\w\s]*", "b5_domestic"),
    (r"\b7[\s-]*day\s+domestic\s+boarders[\w\s]*", "b7_domestic"),
    (r"\bday\s+students\b", "day"),
    # Fee-type compaction
    (r"\btuition\s+and\s+fees\b", "tuition_fees"),
    (r"\btuition\s+only\b", "tuition"),
    (r"\bfees\s+only\b", "fees"),
    # Survey question phrasing — drop conversational stems
    (r"\bdoes\s+your\s+school\b\s*", ""),
    (r"\bdo\s+you\s+have\s+(?:a\s+)?", "has_"),
    (r"\bhow\s+many\s+", ""),
    (r"\bwhat\s+is\s+(?:the\s+)?", ""),
    (r"\bwhat\s+(?:percentage|percent)\s+of\s+", "pct_"),
    (r"\bplease\s+", ""),
    (r"\?", ""),
    (r"\band\s*/\s*or\b", "or"),
    # Roles / titles
    (r"\bchief\s+financial\s+officer\b", "cfo"),
    (r"\bchief\s+executive\s+officer\b", "ceo"),
    (r"\bchief\s+operating\s+officer\b", "coo"),
    (r"\bhead\s+of\s+school\b", "head"),
    (r"\bhead\s+school\b", "head"),
    (r"\bpresident\b", "pres"),
    (r"\bdirector\s+of\s+(\w+)\b", r"dir_\1"),
    # Compensation / salary
    (r"\bsalary\s*&\s*deferred\s+compensation\b", "def_comp"),
    (r"\bsalary\s+and\s+deferred\s+compensation\b", "def_comp"),
    (r"\bdeferred\s+compensation\b", "def_comp"),
    (r"\bother\s+compensation\b", "other_comp"),
    (r"\bstandard\s+benefits\b", "benefits"),
    (r"\bemployee\s+benefits\b", "benefits"),
    (r"\binstructional\s+support\b", "instr_support"),
    # Enrollment terms
    (r"\btotal\s+school\s+enrollment\b", "enroll"),
    (r"\benrollment\s+by\s+grade\b", "enroll_grade"),
    (r"\btotal\s+full[\s-]*time\b", "ft"),
    (r"\bfull[\s-]*time\b", "ft"),
    (r"\bpart[\s-]*time\b", "pt"),
    (r"\btotal\s+enrollment\b", "enroll"),
    (r"\bboarding\s+enrollment\b", "boarding"),
    (r"\btotal\s+boarding\b", "boarding"),
    (r"\benrollment\b", "enroll"),
    # Race / ethnicity
    (r"\bamerican\s+indian\s+(?:and\s+|/\s*)?alaska\s+native\b", "aian"),
    (r"\bnative\s+hawaiian\s*(?:or\s+|/\s*)?other\s+pacific\s+islander\b", "nhopi"),
    (r"\bnative\s+hawaiian(?:other)?\s+pacific\s+islander\b", "nhopi"),
    (r"\bus\s+citizens?\s+or\s+permanent\s+us\s+resident\b", "us"),
    (r"\busa?\s+citizens?\s+or\s+permanent\s+u\.?s\.?\s+residents?\b", "us"),
    (r"\bafrican\s+american\b", "afam"),
    (r"\blatino\s*/\s*hispanic\s+american\b", "latino"),
    (r"\bunsure\s*/\s*not\s+reported\b", "unsure"),
    (r"\bnot\s+reported\b", "nr"),
    (r"\bnon[\s-]*binary\b", "nb"),
    (r"\bnon[\s-]*hispanic\b", "nonhisp"),
    (r"\bethnicity\b", "eth"),
    (r"\bschool\s+age\s+population\b", "sap"),
    # International / domestic
    (r"\binternational\s+students?\b", "intl"),
    (r"\binternational\b", "intl"),
    (r"\bdomestic\s+students?\b", "domestic"),
    # Time periods
    (r"\bcurrent\s+school\s+year\b", "csy"),
    (r"\bprevious\s+school\s+year\b", "psy"),
    (r"\bprior\s+school\s+year\b", "psy"),
    (r"\bschool\s+year\b", ""),
    (r"\bfiscal\s+year\b", "fy"),
    (r"\bopening\s+day\b", "opening"),
    # Quantifiers
    (r"\btotal\s+number\s+of\b", "count"),
    (r"\bnumber\s+of\b", "n"),
    (r"\bpercentage\s+of\b", "pct"),
    (r"\bpercent\s+of\b", "pct"),
    (r"\baverage\b", "avg"),
    (r"\bamount\b", "amt"),
    (r"\bamounts\b", "amts"),
    (r"\bmaximum\b", "max"),
    (r"\bminimum\b", "min"),
    (r"\bhighest\b", "max"),
    (r"\blowest\b", "min"),
    # Admission / applications
    (r"\bapplications\s+rate\b", "app_rate"),
    (r"\bacceptances?\s+rate\b", "accept_rate"),
    (r"\bacceptance\s+rate\b", "accept_rate"),
    (r"\bapplications\b", "apps"),
    (r"\bapplication\b", "app"),
    (r"\bacceptances\b", "accepts"),
    (r"\bacceptance\b", "accept"),
    (r"\binquiries\b", "inq"),
    (r"\badmissions\b", "adm"),
    (r"\badmission\b", "adm"),
    (r"\battrition\b", "attr"),
    # Financial / accounting
    (r"\btemporarily\s+restricted\b", "temp_restr"),
    (r"\bpermanently\s+restricted\b", "perm_restr"),
    (r"\brestricted\b", "restr"),
    (r"\bunrestricted\b", "unrestr"),
    (r"\bexpenditure\b", "expend"),
    (r"\bexpenses\b", "exp"),
    (r"\bexpense\b", "exp"),
    (r"\brevenue\b", "rev"),
    (r"\bincome\b", "inc"),
    (r"\bendowment\b", "endow"),
    (r"\bcompensation\b", "comp"),
    (r"\bcommitment\b", "commit"),
    (r"\bpayments?\b", "pmts"),
    (r"\bdonor\s+restrictions\b", "donor_restr"),
    (r"\bfinancial\s+aid\b", "fa"),
    (r"\btuition\s+discounting\b", "tuition_disc"),
    (r"\bfunds\s+received\b", "funds_recd"),
    # Programs / teaching
    (r"\bprograms\b", "progs"),
    (r"\bprogram\b", "prog"),
    (r"\bteaching\s+staff\b", "teach_staff"),
    (r"\bteachers\b", "tchrs"),
    (r"\bteacher\b", "tchr"),
    (r"\binstructional\b", "instr"),
    (r"\bmedia\s*/?\s*library\b", "media_lib"),
    (r"\bmedialibrary\b", "media_lib"),
    # Education / families
    (r"\beducational\s+attainment\b", "educ_attain"),
    (r"\baggregate\b", "agg"),
    (r"\bfamilies\b", "fams"),
    (r"\bone\s+or\s+more\s+children\s+aged\b", "kids_aged"),
    (r"\bone\s+or\s+more\s+children\b", "kids"),
    (r"\bpopulation\b", "pop"),
    # Time / period
    (r"\bprevious\b", "prev"),
    (r"\bcurrent\b", "curr"),
    (r"\bpast\b", "past"),
    (r"\bbeginning\b", "begin"),
    # Misc compaction
    (r"\bmarketing\s+communications?\b", "mktg_comm"),
    (r"\blearning\s+differences?\b", "ld"),
    (r"\bsabbatical\s+opportunities\b", "sabbatical"),
    (r"\baid\s*/?\s*scholarships?\b", "aid_schol"),
    (r"\breplacement\s+and?\s*renewal\b", "repl_renewal"),
    (r"\bspecial\s+maintenance\b", "spec_maint"),
    (r"\bcombined\s+award\b", "comb_award"),
    (r"\btuition\s+remission\b", "tuition_rem"),
    (r"\byears?\s+of\s+service\b", "yos"),
    (r"\btotal\s+experience\b", "total_yrs_exp"),
    (r"\bexperience\s+at\s+the\s+school\b", "school_exp"),
    (r"\bexperience\b", "yrs_exp"),
    (r"\b1\s*:\s*1\b", "1to1"),
    # Grade level compaction
    (r"\bgrade\s+(\d+)\b", r"g\1"),
    (r"\bpre[\s-]*kindergarten\b", "pk"),
    (r"\bkindergarten\s+part[\s-]*time\b", "k_pt"),
    (r"\bkindergarten\s+full[\s-]*day\b", "k_full"),
    (r"\bkindergarten\b", "k"),
    (r"\bpre[\s-]*first(?:\s+grade)?\b", "pre_first"),
    (r"\bpreschool\b", "ps"),
    (r"\bpost[\s-]*graduate\b", "pg"),
    # Family-paying tuition tier — percentage range BEFORE the prefix substitution
    (r"(\d+)\s*[-\s]\s*(\d+)\s*%\s*of\s+tuition", r"\1_\2pct"),
    (r"\bfamily\s+paying\s+", "fam_pay_"),
    (r"\bfull\s+tuition\b", "full"),
    (r"\bno\s+tuition\b", "none"),
    (r"\btotal\s+count\s+of\s+", "count_"),
    # Strip generic filler & connector words
    (r"\bof\s+the\b", ""),
    (r"\b(of|the|for|on|in|at|to|a|an|that|which|who|whom|with|by|from)\b", ""),
]
_LABEL_RES = [(re.compile(p, re.IGNORECASE), r) for p, r in _LABEL_REPLACEMENTS]


def _dedupe_tokens(name: str) -> str:
    """Collapse repeated tokens after sanitization. Keeps first occurrence
    of any 4+ char token; drops repeats. "school_enroll_school_enroll_boys"
    -> "school_enroll_boys".
    """
    if "_" not in name:
        return name
    tokens = [t for t in name.split("_") if t]
    seen: Dict[str, bool] = {}
    out: List[str] = []
    for t in tokens:
        if len(t) >= 4 and t in seen:
            continue
        if out and out[-1] == t:
            continue
        seen[t] = True
        out.append(t)
    return "_".join(out)


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
    # Drop repeated tokens (e.g. "enroll_enroll" -> "enroll")
    name = _dedupe_tokens(name)
    # Cap at 64 chars (BQ limit is 300; we want readable + the full label is
    # always preserved in the column DESCRIPTION metadata).
    if len(name) > 64:
        name = name[:64].rstrip("_")
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

def _f(name: str, type_: str, mode: str = "NULLABLE", description: str = "") -> bigquery.SchemaField:
    return bigquery.SchemaField(name, type_, mode, description=description)


DIM_SCHOOL_SCHEMA = [
    _f("school_id", "STRING", "REQUIRED",
       "Salesforce-style school identifier used by /assocSchoolData endpoint."),
    _f("dasl_school_id", "STRING", description="DASL legacy numeric school ID."),
    _f("school_name", "STRING", description="School name as reported by NAIS."),
    _f("city", "STRING", description="City."),
    _f("state_code", "STRING", description="US state or Canadian province code."),
    _f("cola_index", "FLOAT", description="Cost-of-living adjustment index for the school's location."),
    _f("loaded_at", "TIMESTAMP", description="UTC timestamp this row was last loaded."),
]

DIM_VARIABLE_SCHEMA = [
    _f("var_id", "INTEGER", "REQUIRED",
       "DASL variable identifier. Stable across years."),
    _f("category", "STRING", description="Top-level category (1st level of the variable's label)."),
    _f("subcategory", "STRING", description="Second level of the variable's label."),
    _f("section", "STRING", description="Third level — usually the survey section."),
    _f("question", "STRING", description="The actual question text (leaf level)."),
    _f("label", "STRING", description="Full ' -> ' delimited label as DASL provides it."),
    _f("data_type", "STRING", description="DASL datatype: integer, float, choiceSingle, choiceMulti, string, etc."),
    _f("sub_category_id", "INTEGER", description="DASL API subCategoryId (used to fetch the variable's data)."),
    _f("lookup_group_id", "INTEGER", description="ID of the lookup group for choice variables (FK to dim_lookup)."),
    _f("association_specific", "BOOLEAN", description="True if this variable applies only to specific associations."),
    _f("column_name", "STRING",
       description="Auto-generated snake_case column name used in the wide vw_* views."),
    _f("loaded_at", "TIMESTAMP", description="UTC timestamp this row was last loaded."),
]

DIM_LOOKUP_SCHEMA = [
    _f("lookup_group_id", "INTEGER", "REQUIRED",
       "Lookup group identifier (referenced by dim_variable.lookup_group_id)."),
    _f("group_name", "STRING", description="Human-readable name of the lookup group, e.g. 'Yes/No'."),
    _f("lookup_key", "STRING", "REQUIRED",
       "The raw key as stored in fact_school_data.value_choice."),
    _f("lookup_value", "STRING", description="Human-readable label for the key."),
    _f("loaded_at", "TIMESTAMP", description="UTC timestamp this row was last loaded."),
]

FACT_SCHOOL_DATA_SCHEMA = [
    _f("school_id", "STRING", "REQUIRED", "FK to dim_school.school_id."),
    _f("year", "INTEGER", "REQUIRED", "Reporting year (ending year of academic calendar)."),
    _f("var_id", "INTEGER", "REQUIRED", "FK to dim_variable.var_id."),
    _f("value_numeric", "FLOAT", description="Numeric value (populated for integer/float/currency types)."),
    _f("value_string", "STRING", description="String value (populated for plain string types)."),
    _f("value_choice", "STRING", description="Lookup key for choice variables."),
    _f("value_choice_label", "STRING", description="Resolved human-readable label for choice variables."),
    _f("is_na", "BOOLEAN", description="True if the school explicitly marked this variable as Not Applicable."),
    _f("sub_category_id", "INTEGER", description="DASL subCategoryId this value was fetched under."),
    _f("last_updated", "TIMESTAMP", description="DASL-reported last-update timestamp for the school's row."),
    _f("loaded_at", "TIMESTAMP", description="UTC timestamp this row was last loaded."),
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
        existing = bq.get_table(table_ref)
        # Refresh column descriptions on every run in case the schema changed.
        # We can only relax modes / update descriptions in place — the column
        # set itself must match for this to be a no-op load.
        existing.schema = FACT_SCHOOL_DATA_SCHEMA
        bq.update_table(existing, ["schema"])
        return
    except NotFound:
        pass
    logger.info("Creating fact table %s", table_ref)
    table = bigquery.Table(table_ref, schema=FACT_SCHOOL_DATA_SCHEMA)
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


def _friendly_description(label: str) -> str:
    """Build a human-readable description from a 4-level arrow-delimited label.

    "Tuition and Fees -> Tuition and Fees -> 5 Day Boarding -> Grade 1 to Grade 12: Tuition Only: Grade 9"
      -> "5 Day Boarding — Grade 1 to Grade 12: Tuition Only: Grade 9"
    The first two levels are usually the category twice; we drop them.
    """
    parts = [p.strip() for p in (label or "").split(" -> ") if p.strip()]
    if not parts:
        return ""
    if len(parts) >= 4:
        section, question = parts[2], parts[3]
        # If section is a generic repeat of the category, drop it
        if section.lower() in (parts[0].lower(), parts[1].lower()):
            return question
        return f"{section} — {question}"
    return parts[-1]


def _sql_escape(text: str) -> str:
    """Escape a string for inclusion inside a double-quoted SQL string literal."""
    return (text or "").replace("\\", "\\\\").replace('"', '\\"')


def rebuild_category_views(
    bq: bigquery.Client,
    project: str,
    dataset: str,
    variables: List[Dict[str, Any]],
    column_names: Dict[int, str],
    varid_to_subcat: Dict[int, int],
) -> List[str]:
    """One view per top-level category, with column descriptions auto-generated
    from each variable's label. Returns list of view IDs created.
    """
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

    # Common columns at the start of every view
    common_cols = [
        ("school_id",   "School ID (Salesforce-style)."),
        ("school_name", "School name."),
        ("state_code",  "US state or province code."),
        ("year",        "Reporting year (ending year of academic calendar)."),
    ]

    created: List[str] = []
    for category, vars_in_cat in by_category.items():
        view_name = _safe_view_suffix(category)
        view_id = _qualify(project, dataset, view_name)

        # Collect (col_name, var_id, description, sql_expr) for each variable column
        cols: List[Tuple[str, int, str, str]] = []
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
                expr = f"MAX(IF(f.var_id = {vid}, f.value_numeric, NULL))"
            elif dtype in _CHOICE_DATA_TYPES:
                expr = (
                    f"MAX(IF(f.var_id = {vid}, "
                    f"COALESCE(f.value_choice_label, f.value_choice), NULL))"
                )
            else:
                expr = f"MAX(IF(f.var_id = {vid}, f.value_string, NULL))"
            desc = _friendly_description(v.get("label") or "")
            cols.append((col, vid, desc, expr))

        if not cols:
            continue

        # Build the column list + descriptions for the CREATE VIEW header
        header_cols = []
        for col_name, desc in common_cols:
            header_cols.append(f'  {col_name} OPTIONS(description="{_sql_escape(desc)}")')
        for col_name, _vid, desc, _expr in cols:
            header_cols.append(f'  {col_name} OPTIONS(description="{_sql_escape(desc)}")')

        # Build the SELECT list
        select_lines = [
            "  f.school_id",
            "  s.school_name",
            "  s.state_code",
            "  f.year",
        ]
        for col_name, _vid, _desc, expr in cols:
            select_lines.append(f"  {expr} AS {col_name}")

        var_ids = ", ".join(str(v["varId"]) for v in vars_in_cat if v.get("varId") is not None)
        sql = (
            f"CREATE OR REPLACE VIEW `{view_id}` (\n"
            + ",\n".join(header_cols) + "\n"
            f")\n"
            f"AS\n"
            f"SELECT\n"
            + ",\n".join(select_lines) + "\n"
            f"FROM {fact} f\n"
            f"LEFT JOIN {school} s USING (school_id)\n"
            f"WHERE f.var_id IN ({var_ids})\n"
            f"GROUP BY f.school_id, s.school_name, s.state_code, f.year;\n"
        )
        logger.info("Creating view %s (%d columns)", view_id, len(cols))
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
