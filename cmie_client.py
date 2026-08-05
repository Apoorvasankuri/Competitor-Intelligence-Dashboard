"""
CMIE CapEx API client.

Thin, isolated wrapper around the CMIE CapEx REST API
(https://capex.cmie.com/api/). This module has no dependency on
main.py, scraper_production.py, or llm_processor_production.py.

Usage:
    from cmie_client import call_cmie_api

    data = call_cmie_api(setid="12345", reporttype="details")
    data = call_cmie_api(projectid="98765")
    data = call_cmie_api(batchid="55555", reporttype="events")
"""

import os
import requests

CMIE_API_URL = "https://capex.cmie.com/api/"
CMIE_REQUEST_TIMEOUT_SECONDS = 60


class CmieApiError(Exception):
    """Raised whenever the CMIE API cannot be called or returns an error payload."""
    pass


def _get_api_key() -> str:
    api_key = os.environ.get("CMIE_API_KEY")
    if not api_key:
        raise CmieApiError("CMIE_API_KEY environment variable is not set")
    return api_key


def call_cmie_api(projectid=None, setid=None, batchid=None, reporttype="details"):
    """
    Call the CMIE CapEx API and return the parsed JSON response.

    Exactly one of projectid, setid, or batchid should normally be supplied.
    reporttype is only sent when setid or batchid is used (project profile
    calls using projectid do not need a reporttype).

    Raises:
        CmieApiError: if CMIE_API_KEY is missing, the HTTP call fails,
                      or the API responds with an errno/errmsg payload.
    """
    api_key = _get_api_key()

    form_data = {
        "apikey": api_key,
    }

    if projectid is not None:
        form_data["projectid"] = str(projectid)

    if setid is not None:
        form_data["setid"] = str(setid)
        if reporttype:
            form_data["reporttype"] = reporttype

    if batchid is not None:
        form_data["batchid"] = str(batchid)
        if reporttype:
            form_data["reporttype"] = reporttype

    try:
        response = requests.post(
            CMIE_API_URL,
            files={k: (None, v) for k, v in form_data.items()},
            timeout=CMIE_REQUEST_TIMEOUT_SECONDS,
        )
    except requests.exceptions.RequestException as exc:
        raise CmieApiError(f"CMIE API request failed: {exc}") from exc

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        raise CmieApiError(
            f"CMIE API returned HTTP {response.status_code}: {response.text[:500]}"
        ) from exc

    try:
        data = response.json()
    except ValueError as exc:
        raise CmieApiError(
            f"CMIE API returned a non-JSON response: {response.text[:500]}"
        ) from exc

    # Errors can come back as a dict with errno/errmsg, or as a list containing
    # such a dict as its first element. Check both shapes defensively.
    meta = data.get("meta") if isinstance(data, dict) else None
    if isinstance(meta, dict):
        errno = meta.get("errno")
        errmsg = meta.get("errmsg")
        if errno not in (0, "0", None):
            raise CmieApiError(f"CMIE API error (errno={errno}): {errmsg}")

    # Defensive fallback for any response shape that doesn't use "meta"
    # (kept in case a future/other reporttype responds differently).
    elif isinstance(data, dict) and ("errno" in data or "errmsg" in data):
        errno = data.get("errno")
        if errno not in (0, "0", None):
            raise CmieApiError(f"CMIE API error (errno={errno}): {data.get('errmsg')}")

    return data