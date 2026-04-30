"""One-off helper: dump DASL metadata to CSVs for human mapping work.

Produces these files in the current directory:
  - variables.csv           (one row per DASL variable + subCategoryId if known)
  - lookup_values.csv       (one row per lookup value, flattened across groups)
  - schools.csv             (every school the association reports on)
  - schools_mapped.csv      (only schools with a non-empty schoolId — the ones
                             we can actually query /assocSchoolData for)

Variables gain a `subCategoryId` column (populated by probing each valid
subcategory for the first mapped school in the current year). Variables that
don't appear in any probed subcategory get subCategoryId blank and
applies_to_tabs=False.
"""

from __future__ import annotations

import csv
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from main import (
    DASL_ASSOC_SCHOOL_DATA_URL,
    DASL_CLIENT_ID,
    DASL_CLIENT_SECRET_NAME,
    GCP_PROJECT_ID,
    DaslClient,
    _get_secret,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("export_metadata")

# Subcategory IDs that return data on stage for Tabs in 2024.
# 13, 14, 22, 23, 24 return 0 data points (may be empty).
# 19, 20, 21, 28 return 500 (server-side issue — skip).
VALID_SUBCATEGORY_IDS = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
    15, 16, 17, 18,
    25, 26, 27, 29,
]


def _write_csv(path: str, rows: List[Dict[str, Any]], columns: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    log.info("Wrote %d rows -> %s", len(rows), path)


def _build_varid_to_subcat(
    dasl: DaslClient, school_id: str, year: int
) -> Dict[int, int]:
    mapping: Dict[int, int] = {}
    for scid in VALID_SUBCATEGORY_IDS:
        try:
            payload = dasl.get(
                f"{DASL_ASSOC_SCHOOL_DATA_URL}/{school_id}",
                params={"year": year, "subCategoryId": scid},
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("subCategoryId=%d failed: %s", scid, exc)
            continue
        entry = (payload or {}).get("schoolEntry") or {}
        for dp in entry.get("dataPoints") or []:
            var_id = dp.get("varId")
            if var_id is not None and var_id not in mapping:
                mapping[var_id] = scid
    log.info(
        "Mapped %d varIds across %d subcategories (school=%s year=%d)",
        len(mapping), len(VALID_SUBCATEGORY_IDS), school_id, year,
    )
    return mapping


def main() -> None:
    secret = _get_secret(GCP_PROJECT_ID, DASL_CLIENT_SECRET_NAME)
    dasl = DaslClient(DASL_CLIENT_ID, secret)
    dasl.authenticate()

    # --- schools ---------------------------------------------------------
    schools = dasl.get_assoc_schools()
    schools_mapped = [s for s in schools if s.get("schoolId")]

    _write_csv(
        "schools.csv",
        schools,
        columns=["schoolId", "daslSchoolId", "schoolName", "city", "stateCode", "colaIndex"],
    )
    _write_csv(
        "schools_mapped.csv",
        schools_mapped,
        columns=["schoolId", "daslSchoolId", "schoolName", "city", "stateCode", "colaIndex"],
    )

    # --- varId -> subCategoryId (probe first mapped school, current year) -
    if schools_mapped:
        probe_school = schools_mapped[0]["schoolId"]
        probe_year = datetime.now(timezone.utc).year
        # Previous academic year is usually the most complete
        varid_to_subcat = _build_varid_to_subcat(dasl, probe_school, probe_year)
        if not varid_to_subcat:
            # Fall back to prior year
            log.info("Current year empty, retrying with %d", probe_year - 1)
            varid_to_subcat = _build_varid_to_subcat(dasl, probe_school, probe_year - 1)
    else:
        log.warning("No schools with mapped schoolId; cannot probe subcategories")
        varid_to_subcat = {}

    # --- variables.csv ---------------------------------------------------
    variables = dasl.get_variable_metadata()
    variable_rows: List[Dict[str, Any]] = []
    for v in variables:
        label = v.get("label") or ""
        parts = [p.strip() for p in label.split(" -> ")]
        parts += [""] * (4 - len(parts))
        var_id = v.get("varId")
        subcat = varid_to_subcat.get(var_id)
        variable_rows.append(
            {
                "varId": var_id,
                "subCategoryId": subcat if subcat is not None else "",
                "applies_to_tabs": bool(subcat is not None),
                "category": parts[0],
                "subcategory": parts[1],
                "section": parts[2],
                "question": parts[3],
                "label": label,
                "dataType": v.get("dataType"),
                "lookupGroupId": v.get("lookupGroupId"),
                "associationSpecific": v.get("associationSpecific"),
            }
        )

    _write_csv(
        "variables.csv",
        variable_rows,
        columns=[
            "varId",
            "subCategoryId",
            "applies_to_tabs",
            "category",
            "subcategory",
            "section",
            "question",
            "label",
            "dataType",
            "lookupGroupId",
            "associationSpecific",
        ],
    )

    # --- lookup_values.csv (flattened) -----------------------------------
    lookup_groups = dasl.get_lookup_groups()
    lookup_rows: List[Dict[str, Any]] = []
    for group in lookup_groups:
        gid = group.get("lookupGroupId")
        gname = group.get("name")
        items = group.get("items") or group.get("values") or group.get("lookups") or []
        for item in items:
            if isinstance(item, dict):
                lookup_rows.append(
                    {
                        "lookupGroupId": gid,
                        "groupName": gname,
                        "key": item.get("key") or item.get("id"),
                        "value": item.get("value") or item.get("label"),
                    }
                )
            else:
                lookup_rows.append(
                    {"lookupGroupId": gid, "groupName": gname, "key": item, "value": item}
                )
    _write_csv(
        "lookup_values.csv",
        lookup_rows,
        columns=["lookupGroupId", "groupName", "key", "value"],
    )

    # --- Summary ---------------------------------------------------------
    applies = sum(1 for r in variable_rows if r["applies_to_tabs"])
    log.info("Variables applicable to Tabs (found in probe): %d / %d",
             applies, len(variable_rows))


if __name__ == "__main__":
    main()
