# ─── CMIE CapEx: Civil Highlights (high-value, recently-updated projects) ─────
# Appended block. Does not modify any existing endpoint, table, or import above.
# Requires: cmie_events.py present alongside cmie_client.py / cmie_sync.py.
# Does NOT require the last_updated_date DB column or any cmie_sync.py edits --
# "last updated" only exists via the per-project Profile API, so it's fetched
# live for candidates, not stored during the bulk batch/set sync.
#
# Criteria (defaults, all overridable via query params):
#   - project_cost > 250 crore (CMIE stores cost in Rs. million -> > 2500)
#     -- filtered from already-synced cmie_projects data (fast, no live call)
#   - last updated within the last N months (default 3)
#     -- fetched live per cost-qualifying project via the Project Profile API,
#        since this field isn't present in batch/set sync data at all
#
# For each project whose live last_updated_on falls in the window, returns
# its recent (last N months) events, already extracted from the same call.

import logging as _civil_hl_logging
from cmie_events import get_project_profile_highlights

_civil_hl_logger = _civil_hl_logging.getLogger("cmie.civil_highlights")

CIVIL_HIGHLIGHTS_MAX_CANDIDATES = 50  # cap on live Project Profile API calls per request


@app.get("/api/cmie/civil-highlights")
def cmie_civil_highlights(
    token: str,
    min_cost_crore: float = 250,
    months_back: int = 3,
    limit: int = CIVIL_HIGHLIGHTS_MAX_CANDIDATES,
):
    """
    Civil/Admin only. High-value projects (by already-synced cost data),
    filtered live to those updated in the last N months, with their recent
    event history -- for the Civil CapEx tab's highlights panel.
    """
    require_cmie_access(token)

    conn = None
    try:
        min_cost_million = float(min_cost_crore) * 10  # crore -> Rs. million
        limit = max(1, min(int(limit or CIVIL_HIGHLIGHTS_MAX_CANDIDATES), 200))

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT cmie_project_id, project_name, promoter_name, project_cost,
                   project_status, industry, sector, ownership, state, district,
                   location, expected_completion
            FROM cmie_projects
            WHERE project_cost > %s
            ORDER BY project_cost DESC
            LIMIT %s
            """,
            (min_cost_million, limit),
        )
        candidates = cur.fetchall()
        cur.close()

        results = []
        for r in candidates:
            project_id = r.get("cmie_project_id")
            if not project_id:
                continue

            try:
                highlight = get_project_profile_highlights(project_id, months_back=months_back)
            except CmieApiError as exc:
                # Skip projects the live profile call fails for (e.g. bad ID),
                # rather than failing the whole request.
                _civil_hl_logger.warning("Civil highlights: profile fetch failed for %s: %s", project_id, exc)
                continue

            if highlight["last_updated_parsed"] is None:
                continue  # can't confirm recency -> exclude rather than guess
            cutoff = datetime.now().date() - timedelta(days=30 * int(months_back))
            if highlight["last_updated_parsed"] < cutoff:
                continue

            results.append({
                "cmie_project_id": project_id,
                "project_name": r.get("project_name"),
                "promoter_name": r.get("promoter_name"),
                "project_cost": safe_float(r.get("project_cost")),
                "project_status": r.get("project_status"),
                "industry": r.get("industry"),
                "sector": r.get("sector"),
                "state": r.get("state"),
                "location": r.get("location"),
                "expected_completion": r.get("expected_completion"),
                "last_updated": highlight["last_updated_on"],
                "recent_events": highlight["recent_events"],
            })

        return {
            "status": "success",
            "criteria": {
                "min_cost_crore": min_cost_crore,
                "months_back": months_back,
                "candidates_checked": len(candidates),
            },
            "count": len(results),
            "projects": results,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
