"""
CMIE CapEx sync logic: normalize records from the CMIE API and upsert
them into the cmie_projects table. Also logs each sync attempt into
cmie_sync_runs.

This module is intentionally standalone (does not import from main.py)
to avoid circular imports, since main.py will import from this module.
It opens its own short-lived DB connections using DATABASE_URL.
"""

import os
import logging
from datetime import datetime

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json

from cmie_client import call_cmie_api, CmieApiError

logger = logging.getLogger(__name__)


def get_db_connection():
    """Create and return a database connection using DATABASE_URL."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise Exception("DATABASE_URL environment variable not set")
    return psycopg.connect(database_url, row_factory=dict_row)


def _first_present(record, keys):
    """Return the first non-None/non-empty value found in record for any of keys."""
    for key in keys:
        if key in record:
            value = record[key]
            if value is not None and value != "":
                return value
    return None


def normalize_cmie_record(record):
    """
    Map a raw CMIE API record (dict, with keys of unknown/variable casing)
    into our normalized cmie_projects column shape.

    Returns a dict with normalized keys, plus 'raw_payload' holding the
    original record untouched. Returns None if the record isn't a dict.
    """
    if not isinstance(record, dict):
        return None

    cmie_project_id = _first_present(
        record, ["PROJECTID", "projectid", "Project ID", "ProjectID"]
    )
    project_name = _first_present(
        record, ["PROJECT_NAME", "project_name", "Project Name", "name"]
    )
    promoter_name = _first_present(
        record,
        ["COMPANY_NAME", "company_name", "Promoter", "promoter_name", "Company Name"],
    )
    project_cost = _first_present(
        record, ["COST", "cost", "Project Cost", "project_cost"]
    )
    project_status = _first_present(
        record, ["STATUS", "status", "Project Status", "project_status"]
    )
    industry = _first_present(record, ["INDUSTRY", "industry"])
    sector = _first_present(record, ["SECTOR", "sector"])
    ownership = _first_present(record, ["OWNERSHIP", "ownership"])
    state = _first_present(record, ["STATE", "state"])
    district = _first_present(record, ["DISTRICT", "district"])
    location = _first_present(record, ["LOCATION", "location"])
    expected_completion = _first_present(
        record, ["COMPLETION_DATE", "completion_date", "Expected Completion"]
    )
    latest_event_date = _first_present(
        record, ["EVENT_DATE", "event_date", "Latest Event Date"]
    )
    latest_event = _first_present(record, ["EVENT", "event", "Latest Event"])

    return {
        "cmie_project_id": str(cmie_project_id) if cmie_project_id is not None else None,
        "project_name": str(project_name) if project_name is not None else None,
        "promoter_name": str(promoter_name) if promoter_name is not None else None,
        "project_cost": project_cost,
        "project_status": str(project_status) if project_status is not None else None,
        "industry": str(industry) if industry is not None else None,
        "sector": str(sector) if sector is not None else None,
        "ownership": str(ownership) if ownership is not None else None,
        "state": str(state) if state is not None else None,
        "district": str(district) if district is not None else None,
        "location": str(location) if location is not None else None,
        "expected_completion": str(expected_completion) if expected_completion is not None else None,
        "latest_event_date": str(latest_event_date) if latest_event_date is not None else None,
        "latest_event": str(latest_event) if latest_event is not None else None,
        "raw_payload": record,
    }


def _to_numeric(value):
    """Best-effort conversion of a project cost value to float; None if not possible."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    try:
        cleaned = str(value).replace(",", "").strip()
        if cleaned == "":
            return None
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def upsert_cmie_projects(records):
    """
    Upsert a list of normalized records into cmie_projects, keyed on
    cmie_project_id. Returns the number of rows upserted.
    """
    if not records:
        return 0

    conn = None
    upserted = 0
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        for rec in records:
            cur.execute(
                """
                INSERT INTO cmie_projects (
                    cmie_project_id, project_name, promoter_name, project_cost,
                    project_status, industry, sector, ownership, state, district,
                    location, expected_completion, latest_event_date, latest_event,
                    raw_payload, last_synced_at, created_at, updated_at
                ) VALUES (
                    %(cmie_project_id)s, %(project_name)s, %(promoter_name)s, %(project_cost)s,
                    %(project_status)s, %(industry)s, %(sector)s, %(ownership)s, %(state)s, %(district)s,
                    %(location)s, %(expected_completion)s, %(latest_event_date)s, %(latest_event)s,
                    %(raw_payload)s, NOW(), NOW(), NOW()
                )
                ON CONFLICT (cmie_project_id) DO UPDATE SET
                    project_name = EXCLUDED.project_name,
                    promoter_name = EXCLUDED.promoter_name,
                    project_cost = EXCLUDED.project_cost,
                    project_status = EXCLUDED.project_status,
                    industry = EXCLUDED.industry,
                    sector = EXCLUDED.sector,
                    ownership = EXCLUDED.ownership,
                    state = EXCLUDED.state,
                    district = EXCLUDED.district,
                    location = EXCLUDED.location,
                    expected_completion = EXCLUDED.expected_completion,
                    latest_event_date = EXCLUDED.latest_event_date,
                    latest_event = EXCLUDED.latest_event,
                    raw_payload = EXCLUDED.raw_payload,
                    last_synced_at = NOW(),
                    updated_at = NOW()
                """,
                {
                    "cmie_project_id": rec["cmie_project_id"],
                    "project_name": rec["project_name"],
                    "promoter_name": rec["promoter_name"],
                    "project_cost": _to_numeric(rec["project_cost"]),
                    "project_status": rec["project_status"],
                    "industry": rec["industry"],
                    "sector": rec["sector"],
                    "ownership": rec["ownership"],
                    "state": rec["state"],
                    "district": rec["district"],
                    "location": rec["location"],
                    "expected_completion": rec["expected_completion"],
                    "latest_event_date": rec["latest_event_date"],
                    "latest_event": rec["latest_event"],
                    "raw_payload": Json(rec["raw_payload"]),
                },
            )
            upserted += 1

        conn.commit()
        cur.close()
        return upserted
    except Exception:
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if conn is not None:
            conn.close()


def _extract_record_list(data):
    """
    Normalize the various shapes the CMIE API might return into a plain list
    of record dicts: a bare list, or a dict containing 'data'/'rows'/'projects'.
    """
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for key in ("data", "rows", "projects"):
            value = data.get(key)
            if isinstance(value, list):
                return value
        # Some single-project-profile responses may come back as one dict
        # representing a single record rather than a list.
        if any(k in data for k in ("PROJECTID", "projectid", "Project ID", "ProjectID")):
            return [data]

    return []


def _log_sync_start(cur, sync_type, setid, batchid, reporttype):
    cur.execute(
        """
        INSERT INTO cmie_sync_runs (sync_type, setid, batchid, reporttype, status, started_at)
        VALUES (%s, %s, %s, %s, 'running', NOW())
        RETURNING id
        """,
        (sync_type, setid, batchid, reporttype),
    )
    return cur.fetchone()["id"]


def _log_sync_end(cur, run_id, status, records_in, records_upserted, error_message=None):
    cur.execute(
        """
        UPDATE cmie_sync_runs
        SET status = %s,
            records_in = %s,
            records_upserted = %s,
            error_message = %s,
            ended_at = NOW()
        WHERE id = %s
        """,
        (status, records_in, records_upserted, error_message, run_id),
    )


def sync_cmie_projects(setid=None, batchid=None, reporttype="details"):
    """
    Run a full CMIE sync: call the API for the given setid/batchid + reporttype,
    normalize the records, and upsert valid ones into cmie_projects.

    Logs the attempt in cmie_sync_runs (started, then success/failed).

    Returns a dict: {status, records_in, records_upserted, error_message}
    """
    sync_type = "batch" if batchid else ("set" if setid else "unknown")

    conn = None
    run_id = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        run_id = _log_sync_start(cur, sync_type, setid, batchid, reporttype)
        conn.commit()
        cur.close()
    except Exception as exc:
        logger.exception("Failed to log CMIE sync run start")
        if conn is not None:
            conn.close()
        return {
            "status": "failed",
            "records_in": 0,
            "records_upserted": 0,
            "error_message": f"Could not create sync run log entry: {exc}",
        }
    finally:
        if conn is not None:
            conn.close()

    records_in = 0
    records_upserted = 0
    error_message = None
    status = "failed"

    try:
        raw_data = call_cmie_api(setid=setid, batchid=batchid, reporttype=reporttype)
        raw_records = _extract_record_list(raw_data)
        records_in = len(raw_records)

        valid_records = []
        for raw_record in raw_records:
            normalized = normalize_cmie_record(raw_record)
            if not normalized:
                continue
            if not normalized["cmie_project_id"] or not normalized["project_name"]:
                continue
            valid_records.append(normalized)

        records_upserted = upsert_cmie_projects(valid_records)
        status = "success"
    except CmieApiError as exc:
        error_message = str(exc)
        logger.error("CMIE sync failed (API error): %s", error_message)
    except Exception as exc:
        error_message = str(exc)
        logger.exception("CMIE sync failed (unexpected error)")

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        _log_sync_end(cur, run_id, status, records_in, records_upserted, error_message)
        conn.commit()
        cur.close()
    except Exception:
        logger.exception("Failed to log CMIE sync run end")
    finally:
        if conn is not None:
            conn.close()

    return {
        "status": status,
        "records_in": records_in,
        "records_upserted": records_upserted,
        "error_message": error_message,
    }
