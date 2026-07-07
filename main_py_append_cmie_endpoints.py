# ─── CMIE CapEx integration (Civil SBU) ───────────────────────────────────────
# Appended block. Does not modify any existing endpoint, table, or import above.

from cmie_client import call_cmie_api, CmieApiError
from cmie_sync import sync_cmie_projects


def require_cmie_access(token: str):
    """
    Access control for all CMIE endpoints.
    Allows admins, and users whose sbu_profile contains 'Civil'
    (case-insensitive, comma-separated safe).
    """
    user = get_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired session")

    if user.get("is_admin"):
        return user

    sbu_profile = (user.get("sbu_profile") or "")
    sbus = [s.strip().lower() for s in sbu_profile.split(",") if s.strip()]
    if "civil" in sbus:
        return user

    raise HTTPException(status_code=403, detail="Civil SBU access required")


@app.get("/api/cmie/summary")
def cmie_summary(token: str):
    """Civil/Admin only. High-level CMIE CapEx dashboard summary."""
    require_cmie_access(token)

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) AS c FROM cmie_projects")
        total_projects = cur.fetchone()["c"]

        cur.execute("SELECT COALESCE(SUM(project_cost), 0) AS s FROM cmie_projects")
        total_project_cost = safe_float(cur.fetchone()["s"])

        cur.execute("""
            SELECT COUNT(*) AS c FROM cmie_projects
            WHERE project_status IS NOT NULL
            AND lower(project_status) NOT IN ('completed', 'closed', 'shelved', 'abandoned')
        """)
        active_projects = cur.fetchone()["c"]

        cur.execute("SELECT MAX(last_synced_at) AS m FROM cmie_projects")
        recently_synced_row = cur.fetchone()
        recently_synced_at = (
            recently_synced_row["m"].isoformat() if recently_synced_row and recently_synced_row["m"] else None
        )

        cur.execute("""
            SELECT COALESCE(project_status, 'Unknown') AS key, COUNT(*) AS c
            FROM cmie_projects GROUP BY project_status ORDER BY c DESC
        """)
        by_status = {r["key"]: r["c"] for r in cur.fetchall()}

        cur.execute("""
            SELECT COALESCE(state, 'Unknown') AS key, COUNT(*) AS c
            FROM cmie_projects GROUP BY state ORDER BY c DESC
        """)
        by_state = {r["key"]: r["c"] for r in cur.fetchall()}

        cur.execute("""
            SELECT COALESCE(industry, 'Unknown') AS key, COUNT(*) AS c
            FROM cmie_projects GROUP BY industry ORDER BY c DESC
        """)
        by_industry = {r["key"]: r["c"] for r in cur.fetchall()}

        cur.close()

        return {
            "status": "success",
            "total_projects": total_projects,
            "total_project_cost": total_project_cost,
            "active_projects": active_projects,
            "recently_synced_at": recently_synced_at,
            "by_status": by_status,
            "by_state": by_state,
            "by_industry": by_industry,
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


@app.get("/api/cmie/projects")
def cmie_projects(
    token: str,
    status: str = None,
    state: str = None,
    industry: str = None,
    promoter: str = None,
    q: str = None,
    limit: int = 100,
    offset: int = 0,
):
    """Civil/Admin only. Paginated, filterable list of CMIE projects."""
    require_cmie_access(token)

    conn = None
    try:
        limit = max(1, min(int(limit or 100), 500))
        offset = max(0, int(offset or 0))

        where_clauses = []
        params = []

        if status:
            where_clauses.append("project_status = %s")
            params.append(status)
        if state:
            where_clauses.append("state = %s")
            params.append(state)
        if industry:
            where_clauses.append("industry = %s")
            params.append(industry)
        if promoter:
            where_clauses.append("promoter_name = %s")
            params.append(promoter)
        if q:
            where_clauses.append("""(
                project_name ILIKE %s OR
                promoter_name ILIKE %s OR
                location ILIKE %s OR
                state ILIKE %s OR
                industry ILIKE %s
            )""")
            like_q = f"%{q}%"
            params.extend([like_q, like_q, like_q, like_q, like_q])

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(f"SELECT COUNT(*) AS c FROM cmie_projects {where_sql}", params)
        total = cur.fetchone()["c"]

        cur.execute(f"""
            SELECT id, cmie_project_id, project_name, promoter_name, project_cost,
                   project_status, industry, sector, ownership, state, district,
                   location, expected_completion, latest_event_date, latest_event,
                   last_synced_at, created_at, updated_at
            FROM cmie_projects
            {where_sql}
            ORDER BY updated_at DESC
            LIMIT %s OFFSET %s
        """, params + [limit, offset])

        rows = cur.fetchall()
        cur.close()

        projects = []
        for r in rows:
            projects.append({
                "id": r.get("id"),
                "cmie_project_id": r.get("cmie_project_id"),
                "project_name": r.get("project_name"),
                "promoter_name": r.get("promoter_name"),
                "project_cost": safe_float(r.get("project_cost")),
                "project_status": r.get("project_status"),
                "industry": r.get("industry"),
                "sector": r.get("sector"),
                "ownership": r.get("ownership"),
                "state": r.get("state"),
                "district": r.get("district"),
                "location": r.get("location"),
                "expected_completion": r.get("expected_completion"),
                "latest_event_date": r.get("latest_event_date"),
                "latest_event": r.get("latest_event"),
                "last_synced_at": r.get("last_synced_at").isoformat() if r.get("last_synced_at") else None,
                "created_at": r.get("created_at").isoformat() if r.get("created_at") else None,
                "updated_at": r.get("updated_at").isoformat() if r.get("updated_at") else None,
            })

        return {
            "status": "success",
            "total": total,
            "limit": limit,
            "offset": offset,
            "count": len(projects),
            "projects": projects,
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


@app.get("/api/cmie/projects/{project_id}")
def cmie_project_detail(project_id: str, token: str):
    """Civil/Admin only. Single project lookup by cmie_project_id."""
    require_cmie_access(token)

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, cmie_project_id, project_name, promoter_name, project_cost,
                   project_status, industry, sector, ownership, state, district,
                   location, expected_completion, latest_event_date, latest_event,
                   raw_payload, last_synced_at, created_at, updated_at
            FROM cmie_projects
            WHERE cmie_project_id = %s
        """, (project_id,))
        row = cur.fetchone()
        cur.close()

        if not row:
            raise HTTPException(status_code=404, detail="Project not found")

        return {
            "status": "success",
            "project": {
                "id": row.get("id"),
                "cmie_project_id": row.get("cmie_project_id"),
                "project_name": row.get("project_name"),
                "promoter_name": row.get("promoter_name"),
                "project_cost": safe_float(row.get("project_cost")),
                "project_status": row.get("project_status"),
                "industry": row.get("industry"),
                "sector": row.get("sector"),
                "ownership": row.get("ownership"),
                "state": row.get("state"),
                "district": row.get("district"),
                "location": row.get("location"),
                "expected_completion": row.get("expected_completion"),
                "latest_event_date": row.get("latest_event_date"),
                "latest_event": row.get("latest_event"),
                "raw_payload": row.get("raw_payload"),
                "last_synced_at": row.get("last_synced_at").isoformat() if row.get("last_synced_at") else None,
                "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
                "updated_at": row.get("updated_at").isoformat() if row.get("updated_at") else None,
            }
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


@app.post("/api/admin/cmie/sync")
def cmie_admin_sync(token: str, setid: str = None, batchid: str = None, reporttype: str = None):
    """Admin only (not just Civil). Triggers a CMIE CapEx sync."""
    user = get_user_from_token(token)
    if not user or not user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Admin access required")

    effective_setid = setid or CMIE_SET_ID
    effective_batchid = batchid or CMIE_BATCH_ID
    effective_reporttype = reporttype or CMIE_DEFAULT_REPORTTYPE

    if not effective_setid and not effective_batchid:
        raise HTTPException(
            status_code=400,
            detail="Provide setid or batchid, or configure CMIE_SET_ID / CMIE_BATCH_ID"
        )

    result = sync_cmie_projects(
        setid=effective_setid,
        batchid=effective_batchid,
        reporttype=effective_reporttype,
    )

    if result["status"] != "success":
        raise HTTPException(status_code=502, detail={
            "status": result["status"],
            "records_in": result["records_in"],
            "records_upserted": result["records_upserted"],
            "error": result["error_message"],
        })

    return {
        "status": result["status"],
        "records_in": result["records_in"],
        "records_upserted": result["records_upserted"],
    }


@app.get("/api/admin/cmie/sync-runs")
def cmie_admin_sync_runs(token: str):
    """Admin only. Last 20 CMIE sync run log rows."""
    user = get_user_from_token(token)
    if not user or not user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Admin access required")

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, sync_type, setid, batchid, reporttype, status,
                   records_in, records_upserted, error_message, started_at, ended_at
            FROM cmie_sync_runs
            ORDER BY started_at DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        cur.close()

        runs = []
        for r in rows:
            runs.append({
                "id": r.get("id"),
                "sync_type": r.get("sync_type"),
                "setid": r.get("setid"),
                "batchid": r.get("batchid"),
                "reporttype": r.get("reporttype"),
                "status": r.get("status"),
                "records_in": r.get("records_in"),
                "records_upserted": r.get("records_upserted"),
                "error_message": r.get("error_message"),
                "started_at": r.get("started_at").isoformat() if r.get("started_at") else None,
                "ended_at": r.get("ended_at").isoformat() if r.get("ended_at") else None,
            })

        return {"status": "success", "runs": runs}
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
