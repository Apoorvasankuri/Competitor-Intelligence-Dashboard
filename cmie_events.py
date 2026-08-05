"""
CMIE CapEx per-project profile fetcher (events + last-updated).

Standalone module (mirrors the isolation pattern of cmie_client.py /
cmie_sync.py). Fetches the full Project Profile from CMIE for a single
project -- this is the ONLY CMIE API call that returns "last_updated_on",
and it also includes the full project_events list in the same response,
so one call gets both pieces of data needed for the Civil Highlights
feature.

Response shape (per CMIE's own API doc, "Project Details API Response"):
    {
      "meta": {"errno": 0, "errmsg": "Success", "projectid": "...", ...},
      "data": {
        "company_name": "...",
        "project_name": "...",
        "basic_information": {
          "cost": "43,210 (Rs. million)",
          "status": "Completed",
          "last_updated_on": "09 Jun 2014",
          ...
        },
        "project_events": [
          {"date": "16 Oct 2012", "amount": "", "description": "Land acquisition problem resolved", ...},
          ...
        ],
        ...
      }
    }

Usage:
    from cmie_events import get_project_profile_highlights

    result = get_project_profile_highlights("373697:1", months_back=3)
    # -> {
    #      "last_updated_on": "09 Jun 2014",
    #      "last_updated_parsed": date(2014, 6, 9)  # or None if unparseable,
    #      "recent_events": [{"date": "...", "description": "...", "amount": "..."}, ...]
    #    }
"""

import logging
from datetime import datetime, timedelta

from cmie_client import call_cmie_api, CmieApiError

logger = logging.getLogger(__name__)

# CMIE's project profile dates are consistently "DD Mon YYYY" per the doc
# examples (e.g. "09 Jun 2014"), but kept as a list in case other fields
# vary -- extend if you see a ValueError in practice.
_DATE_FORMATS = ["%d %b %Y", "%d-%b-%Y", "%d/%m/%Y", "%Y-%m-%d"]


def _parse_cmie_date(value):
    """Best-effort parse of a CMIE date string. Returns a date or None."""
    if not value:
        return None
    text = str(value).strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def get_project_profile_highlights(project_id, months_back=3):
    """
    Fetch a single project's profile and extract:
      - last_updated_on (raw string) + last_updated_parsed (date or None)
      - recent_events: project_events entries within the last `months_back`
        months (events with an unparseable/missing date are excluded from
        this list, not from an error -- a project can validly have zero
        recent events).

    Raises:
        CmieApiError: if the API call fails, or CMIE returns a non-zero
                      errno (missing project, bad projectid, etc).
    """
    raw_data = call_cmie_api(projectid=project_id)

    if not isinstance(raw_data, dict) or "data" not in raw_data:
        raise CmieApiError(
            f"Unexpected project profile response shape for projectid={project_id}: "
            f"{str(raw_data)[:300]}"
        )

    profile = raw_data["data"]
    basic_info = profile.get("basic_information") or {}
    last_updated_on = basic_info.get("last_updated_on")
    last_updated_parsed = _parse_cmie_date(last_updated_on)

    cutoff = datetime.now().date() - timedelta(days=30 * int(months_back))

    recent_events = []
    for event in (profile.get("project_events") or []):
        if not isinstance(event, dict):
            continue
        event_date = _parse_cmie_date(event.get("date"))
        if event_date is None or event_date < cutoff:
            continue
        recent_events.append({
            "date": event.get("date"),
            "description": event.get("description"),
            "amount": event.get("amount"),
        })

    recent_events.sort(key=lambda e: e["date"] or "", reverse=True)

    return {
        "last_updated_on": last_updated_on,
        "last_updated_parsed": last_updated_parsed,
        "recent_events": recent_events,
    }
