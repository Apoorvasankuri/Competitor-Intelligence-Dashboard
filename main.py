from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
import csv
import io
import re
from fastapi.middleware.cors import CORSMiddleware
import psycopg
from psycopg.rows import dict_row
import os
from datetime import datetime, date, timedelta
from decimal import Decimal
import math
from google import genai
from google.genai import types
from fastapi import FastAPI, HTTPException, Request
import logging

# Users who receive AI-generated BU summary digest instead of article digest
SUMMARY_DIGEST_EMAILS = {'emmadis@kecrpg.com','sankuria@kecrpg.com','barfiwalav@kecrpg.com','Kejriwalv@kecrpg.com'}
ALL_SBUS = ['Intl T&D', 'India T&D', 'Civil', 'Transportation', 'Renewables', 'Oil & Gas']
CMIE_SET_ID = os.environ.get("CMIE_SET_ID")
CMIE_BATCH_ID = os.environ.get("CMIE_BATCH_ID")
CMIE_DEFAULT_REPORTTYPE = os.environ.get("CMIE_DEFAULT_REPORTTYPE", "details")

app = FastAPI(title="Competitor Intelligence API")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_connection():
    """Create and return a database connection"""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        raise Exception("DATABASE_URL environment variable not set")
    
    conn = psycopg.connect(database_url, row_factory=dict_row)
    return conn


def safe_float(value):
    """Convert to float, handling NaN and infinity"""
    if value is None:
        return None
    try:
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (ValueError, TypeError):
        return None


def safe_int(value):
    """Convert to int safely"""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def filter_representative_only(rows):
    """
    Return only rows where the article is the representative of its cluster,
    or where cluster fields are missing so we do not accidentally hide legacy rows.
    """
    if not rows:
        return rows

    filtered = []

    for row in rows:
        cluster_id = row.get("cluster_id")

        if cluster_id is None:
            filtered.append(row)
            continue

        is_representative = row.get("is_representative_article")

        if is_representative is None or bool(is_representative):
            filtered.append(row)

    return filtered

@app.get("/")
def read_root():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "message": "Competitor Intelligence API is running",
        "timestamp": datetime.now().isoformat()
    }


def _clean_article_row(row: dict) -> dict:
    """
    Change 18 (production hardening): row-cleaning logic shared by /api/data
    and /api/data/profile.

    Previously each endpoint had its own hand-copied version of this dict,
    and they had silently drifted apart: /api/data/profile's version was
    missing every cluster_*/event_impact_score/actionability_score/
    confidence_score/sbu_fit_score field, even on the admin branch whose SQL
    was already fetching them. Since Executive Brief / SBU Storylines /
    Competitor Strategy / Client-Authority Tracker all filter on
    eventImpactScore, any logged-in user (i.e. every real user — /api/data
    is only the logged-out fallback) got those fields as `undefined` in the
    JSON, which the frontend defaults to 0/null, so every one of those tabs
    silently rendered empty. Having one shared function makes that class of
    drift impossible: both endpoints now always return the same shape.

    Uses row.get() throughout (not row[...]) so it's safe to call even
    against a SELECT that omits some of these columns — missing ones simply
    come back as their documented default instead of raising.
    """
    return {
        'id': safe_int(row.get('id')),
        'publishedate': row['published_date'].isoformat() if row.get('published_date') else None,
        'newstitle': str(row['news_title']) if row.get('news_title') else '',
        'link': str(row['link']) if row.get('link') else '',
        'Source': str(row['Source']) if row.get('Source') else '',
        'relevance_score': safe_int(row.get('relevance_score')) or 0,
        'competitor_tagging': str(row['competitor_tagging']) if row.get('competitor_tagging') else '-',
        'sbu': str(row['sbu_tagging']) if row.get('sbu_tagging') else 'General',
        'category_tag': str(row['category_tag']) if row.get('category_tag') else 'not_analyzed',
        'kec_business_summary': str(row['summary']) if row.get('summary') else '',
        'scraped_content': str(row['scraped_content']) if row.get('scraped_content') else '',
        'contract_value_inr_crore': safe_float(row.get('contract_value_inr_crore')),
        'geography': str(row['geography']) if row.get('geography') and str(row['geography']) != 'None' else None,
        'competitor_tier': safe_int(row.get('competitor_tier')),
        'rank_score': safe_int(row.get('rank_score')) or 0,
        'created_at': row['processed_at'].isoformat() if row.get('processed_at') else None,
        'source_domain': row.get('source_domain'),
        'source_type': row.get('source_type') or 'unknown',
        'source_category': row.get('source_category') or 'unknown',
        'source_priority': safe_int(row.get('source_priority')) or 8,
        'source_authority_score': safe_int(row.get('source_authority_score')) or 5,
        'preferred_for_executive_summary': bool(row.get('preferred_for_executive_summary')),
        'source_notes': row.get('source_notes'),
        'source_match_method': row.get('source_match_method') or 'default',
        'search_query': row.get('search_query'),
        'search_query_type': row.get('search_query_type') or 'unknown',
        'detected_client_authority': row.get('detected_client_authority') or '',
        'detected_strategic_theme': row.get('detected_strategic_theme') or '',
        'accepted_by_gate': row.get('accepted_by_gate') or '',
        'cluster_id': row.get('cluster_id'),
        'relationship_type': row.get('relationship_type') or 'separate_event',
        'is_representative_article': row.get('is_representative_article') if row.get('is_representative_article') is not None else True,
        'cluster_title': row.get('cluster_title') or (str(row['news_title']) if row.get('news_title') else '') or '',
        'cluster_summary': row.get('cluster_summary') or (str(row['summary']) if row.get('summary') else '') or '',
        'cluster_article_count': safe_int(row.get('cluster_article_count')) or 1,
        'cluster_representative_article_id': row.get('cluster_representative_article_id'),
        'cluster_source_confidence': row.get('cluster_source_confidence') or 'Low',
        'cluster_rank_score': safe_int(row.get('cluster_rank_score')) or safe_int(row.get('rank_score')) or 0,
        'cluster_competitors': row.get('cluster_competitors') or (str(row['competitor_tagging']) if row.get('competitor_tagging') else '') or '',
        'cluster_sbus': row.get('cluster_sbus') or (str(row['sbu_tagging']) if row.get('sbu_tagging') else '') or '',
        'cluster_categories': row.get('cluster_categories') or (str(row['category_tag']) if row.get('category_tag') else '') or '',
        'cluster_primary_source': row.get('cluster_primary_source') or (str(row['Source']) if row.get('Source') else '') or '',
        'cluster_primary_source_type': row.get('cluster_primary_source_type') or row.get('source_type') or '',
        'cluster_primary_url': row.get('cluster_primary_url') or (str(row['link']) if row.get('link') else '') or '',
        'event_impact_score': safe_int(row.get('event_impact_score')) or 0,
        'actionability_score': safe_int(row.get('actionability_score')) or 0,
        'confidence_score': safe_int(row.get('confidence_score')) or 0,
        'sbu_fit_score': safe_int(row.get('sbu_fit_score')) or 0,
    }


@app.get("/api/data")
def get_all_data(representative_only: bool = False):
    """Get all processed competitor data from the database"""
    try:
        conn = get_db_connection() if os.environ.get('DATABASE_URL') else get_local_db()
        cur = conn.cursor()
        
        query = """
            SELECT 
                id,
                published_date,
                news_title,
                link,
                "Source",
                relevance_score,
                competitor_tagging,
                sbu_tagging,
                category_tag,
                summary,
                scraped_content,
                contract_value_inr_crore,
                geography,
                competitor_tier,
                rank_score,
                processed_at,
                source_domain,
                source_type,
                source_category,
                source_priority,
                source_authority_score,
                preferred_for_executive_summary,
                source_notes,
                source_match_method,
                search_query,
                search_query_type,
                detected_client_authority,
                detected_strategic_theme,
                accepted_by_gate,
                cluster_id,
                relationship_type,
                is_representative_article,
                cluster_title,
                cluster_summary,
                cluster_article_count,
                cluster_representative_article_id,
                cluster_source_confidence,
                cluster_rank_score,
                cluster_competitors,
                cluster_sbus,
                cluster_categories,
                cluster_primary_source,
                cluster_primary_source_type,
                cluster_primary_url,
                event_impact_score,
                actionability_score,
                confidence_score,
                sbu_fit_score
            FROM processed_articles
            ORDER BY 
                CASE WHEN rank_score IS NULL THEN 1 ELSE 0 END,
                rank_score DESC,
                published_date DESC
        """
        
        cur.execute(query)
        raw_results = cur.fetchall()
        
        cur.close()
        conn.close()
        
        # Manually build clean result list
        clean_results = [_clean_article_row(row) for row in raw_results]
        
        return {
            "status": "success",
            "count": len(clean_results),
            "data": clean_results
        }
    
    except Exception as e:
        import traceback
        raise HTTPException(status_code=500, detail={
            "error": str(e),
            "traceback": traceback.format_exc()
        })


@app.get("/api/stats")
def get_statistics():
    """Get dashboard statistics"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) as total FROM processed_articles")
        total = cur.fetchone()['total']
        
        cur.execute("SELECT COUNT(DISTINCT sbu_tagging) as count FROM processed_articles WHERE sbu_tagging IS NOT NULL")
        unique_sbus = cur.fetchone()['count']
        
        cur.execute("SELECT COUNT(DISTINCT competitor_tagging) as count FROM processed_articles WHERE competitor_tagging IS NOT NULL AND competitor_tagging != '-'")
        unique_competitors = cur.fetchone()['count']
        
        cur.execute("SELECT COUNT(*) as count FROM processed_articles WHERE published_date >= CURRENT_DATE - INTERVAL '7 days'")
        recent = cur.fetchone()['count']
        
        cur.execute("SELECT COUNT(*) as count FROM processed_articles WHERE relevance_score >= 70")
        high_relevance = cur.fetchone()['count']
        
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "stats": {
                "total_articles": int(total),
                "unique_sbus": int(unique_sbus),
                "unique_competitors": int(unique_competitors),
                "recent_articles": int(recent),
                "high_relevance_articles": int(high_relevance)
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/api/health/deep")
def health_deep():
    """Deep health check with data freshness and pipeline status"""
    result = {
        "status": "healthy",
        "db_ok": False,
        "processed_articles_count": 0,
        "latest_processed_date": None,
        "last_pipeline_run": None,
        "warnings": []
    }

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT 1")
        result["db_ok"] = True

        # Article count and latest date
        try:
            cur.execute("""
                SELECT
                    COUNT(*) AS processed_articles_count,
                    MAX(published_date) AS latest_processed_date
                FROM processed_articles
            """)
            row = cur.fetchone()

            if row:
                result["processed_articles_count"] = row.get("processed_articles_count") or 0
                latest_date = row.get("latest_processed_date")

                if latest_date:
                    result["latest_processed_date"] = latest_date.isoformat()

                    # published_date is a DATE column (not TIMESTAMP), so
                    # MAX(published_date) comes back as a plain datetime.date.
                    # datetime.now() - a_date raises TypeError, which was
                    # silently caught by the except block below and reported
                    # as a generic "Article freshness check failed" warning —
                    # meaning stale data was never actually surfaced. Compare
                    # as dates, since that's the only granularity available.
                    latest_date_only = latest_date.date() if isinstance(latest_date, datetime) else latest_date
                    age_days = (datetime.now().date() - latest_date_only).days

                    if age_days >= 1:
                        result["status"] = "warning"
                        result["warnings"].append(
                            f"Latest processed article is {age_days} day(s) old"
                        )

        except Exception as e:
            result["warnings"].append(f"Article freshness check failed: {str(e)}")

        # Pipeline run status
        try:
            cur.execute("""
                SELECT
                    pipeline_id,
                    stage,
                    status,
                    started_at,
                    ended_at,
                    error_message
                FROM pipeline_runs
                ORDER BY started_at DESC
                LIMIT 1
            """)
            row = cur.fetchone()

            if not row:
                result["warnings"].append(
                    "No pipeline_runs rows found — scraper/processor may never have run with pipeline tracking enabled."
                )

            if row:
                result["last_pipeline_run"] = {
                    "pipeline_id": row.get("pipeline_id"),
                    "stage": row.get("stage"),
                    "status": row.get("status"),
                    "started_at": row.get("started_at").isoformat() if row.get("started_at") else None,
                    "ended_at": row.get("ended_at").isoformat() if row.get("ended_at") else None,
                    "error_message": row.get("error_message")
                }

                if row.get("status") == "failed":
                    result["status"] = "error"
                    result["warnings"].append(
                        f"Last pipeline stage '{row.get('stage')}' failed: {row.get('error_message')}"
                    )

        except Exception as e:
            result["warnings"].append(f"pipeline_runs table not available or empty: {str(e)}")

        cur.close()

    except Exception as e:
        result["status"] = "error"
        result["warnings"].append(f"Health check failed: {str(e)}")

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    return result

@app.get("/api/health")
def health_check():
    """Health check with database connectivity test"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }
@app.get("/api/admin/pipeline-runs")
def get_pipeline_runs(token: str):
    """Return most recent pipeline run rows. Admin-only."""
    user = get_user_from_token(token)
    if not user or not user["is_admin"]:
        raise HTTPException(status_code=403, detail="Admin access required")

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            cur.execute("""
                SELECT
                    id,
                    pipeline_id,
                    stage,
                    status,
                    COALESCE(articles_in, 0) AS articles_in,
                    COALESCE(articles_out, 0) AS articles_out,
                    error_message,
                    started_at,
                    ended_at
                FROM pipeline_runs
                ORDER BY started_at DESC
                LIMIT 100
            """)
            rows = cur.fetchall()

            runs = []
            for r in rows:
                runs.append({
                    "id": r.get("id"),
                    "pipeline_id": r.get("pipeline_id"),
                    "stage": r.get("stage"),
                    "status": r.get("status"),
                    "articles_in": r.get("articles_in"),
                    "articles_out": r.get("articles_out"),
                    "error_message": r.get("error_message"),
                    "started_at": r.get("started_at").isoformat() if r.get("started_at") else None,
                    "ended_at": r.get("ended_at").isoformat() if r.get("ended_at") else None,
                })

            cur.close()
            return {"status": "success", "runs": runs}

        except Exception as e:
            cur.close()
            return {
                "status": "success",
                "runs": [],
                "warning": f"pipeline_runs unavailable: {str(e)}"
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

@app.get("/api/raw-count")
def get_raw_count():
    """Get count of unprocessed articles"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) as count FROM raw_scraped_articles")
        count = cur.fetchone()['count']
        
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "unprocessed_articles": int(count)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/api/export-csv")
def export_csv(start_date: str = '2026-02-25', end_date: str = '2026-03-01'):
    """Export processed articles as CSV for a date range"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                id, published_date, news_title, link, "Source",
                relevance_score, competitor_tagging, sbu_tagging,
                category_tag, summary, contract_value_inr_crore,
                geography, competitor_tier, rank_score,
                source_domain, source_type, source_category, source_priority,
                source_authority_score, preferred_for_executive_summary,
                source_match_method,
                search_query_type, detected_client_authority,
                detected_strategic_theme, accepted_by_gate,
                cluster_id, relationship_type, is_representative_article,
                cluster_article_count, cluster_source_confidence, cluster_rank_score,
                cluster_primary_source, cluster_primary_source_type,
                event_impact_score, actionability_score, confidence_score, sbu_fit_score
            FROM processed_articles
            WHERE published_date >= %s
            AND published_date < %s
            ORDER BY published_date DESC
        """, (start_date, end_date))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        output = io.StringIO()
        if rows:
            writer = csv.DictWriter(output, fieldnames=rows[0].keys())
            writer.writeheader()
            for row in rows:
                writer.writerow({k: v.isoformat() if isinstance(v, (datetime, date)) else v for k, v in row.items()})
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=export_{start_date}_{end_date}.csv"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export error: {str(e)}")
# ─── Local DB connection (for assignments feature) ───────────────────────────

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
load_dotenv()

def get_local_db():
    local_url = os.environ.get('LOCAL_DB_URL')
    if not local_url:
        raise Exception("LOCAL_DB_URL not set in .env")
    conn = psycopg2.connect(local_url)
    return conn

# ─── Upload directory CSV ─────────────────────────────────────────────────────

from fastapi import UploadFile, File
import csv
import io

@app.post("/api/upload-directory")
async def upload_directory(file: UploadFile = File(...)):
    """Upload employee directory CSV to local DB"""
    try:
        contents = await file.read()
        decoded = contents.decode('utf-8')
        reader = csv.DictReader(io.StringIO(decoded))

        conn = get_local_db()
        cur = conn.cursor()

        inserted = 0
        for row in reader:
            cur.execute("""
                INSERT INTO people (name, email, sbu, role)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (email) DO UPDATE
                SET name = EXCLUDED.name,
                    sbu = EXCLUDED.sbu,
                    role = EXCLUDED.role
            """, (
                row.get('name', '').strip(),
                row.get('email', '').strip().lower(),
                row.get('sbu', '').strip(),
                row.get('role', '').strip()
            ))
            inserted += 1

        conn.commit()
        cur.close()
        conn.close()

        return {"status": "success", "people_imported": inserted}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Search people (autocomplete) ────────────────────────────────────────────

@app.get("/api/people/search")
def search_people(q: str = ""):
    """Search people by name or email for autocomplete"""
    try:
        conn = get_local_db()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT id, name, email, sbu, role
            FROM people
            WHERE name ILIKE %s OR email ILIKE %s
            ORDER BY name
            LIMIT 10
        """, (f'%{q}%', f'%{q}%'))

        results = cur.fetchall()
        cur.close()
        conn.close()

        return {"status": "success", "people": [dict(r) for r in results]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Save assignment ──────────────────────────────────────────────────────────

from pydantic import BaseModel
from typing import List

class Assignment(BaseModel):
    article_id: int
    assigned_by_email: str
    assigned_to_email: str
    assigned_to_name: str
    notes: str = ""

class AssignmentBatch(BaseModel):
    assignments: List[Assignment]

@app.post("/api/assignments")
def save_assignments(batch: AssignmentBatch):
    """Save one or more assignments"""
    try:
        conn = get_local_db()
        cur = conn.cursor()

        for a in batch.assignments:
            cur.execute("""
                INSERT INTO assignments 
                    (article_id, assigned_by_email, assigned_to_email, assigned_to_name, notes)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                a.article_id,
                a.assigned_by_email.lower(),
                a.assigned_to_email.lower(),
                a.assigned_to_name,
                a.notes
            ))

        conn.commit()
        cur.close()
        conn.close()

        return {"status": "success", "saved": len(batch.assignments)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Get assignments ──────────────────────────────────────────────────────────

@app.get("/api/assignments")
def get_assignments(assigned_by: str = "", assigned_to: str = ""):
    """Get assignments, optionally filtered by manager or assignee"""
    try:
        conn = get_local_db()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        query = """
            SELECT 
                a.id,
                a.article_id,
                a.assigned_by_email,
                a.assigned_to_email,
                a.assigned_to_name,
                a.status,
                a.notes,
                a.created_at
            FROM assignments a
            WHERE 1=1
        """
        params = []

        if assigned_by:
            query += " AND a.assigned_by_email = %s"
            params.append(assigned_by.lower())

        if assigned_to:
            query += " AND a.assigned_to_email = %s"
            params.append(assigned_to.lower())

        query += " ORDER BY a.created_at DESC"

        cur.execute(query, params)
        results = cur.fetchall()
        cur.close()
        conn.close()

        return {"status": "success", "assignments": [dict(r) for r in results]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    # ─── ROUTING MATRIX ──────────────────────────────────────────────────────────

ROUTING_MATRIX = {
    "bidding activity": {
        "intl t&d": ["abhishek.sil@company.com", "ganesh.srinivasan@company.com"],
        "india t&d": ["nk.gupta@company.com", "rajinder.gupta@company.com"],
        "civil": ["jaideep@company.com", "raman.kapil@company.com"],
        "transportation": ["suresh.kumar@company.com", "ankur.dev@company.com"],
        "renewables": ["abhishek.sil@company.com", "anand.kulkarni@company.com"],
        "oil & gas": ["parthasarathi@company.com", "mayank.agarwal@company.com"],
    },
    "order wins": {
        "intl t&d": ["abhishek.sil@company.com", "ganesh.srinivasan@company.com"],
        "india t&d": ["nk.gupta@company.com", "rajinder.gupta@company.com"],
        "civil": ["jaideep@company.com", "raman.kapil@company.com"],
        "transportation": ["suresh.kumar@company.com", "ankur.dev@company.com"],
        "renewables": ["abhishek.sil@company.com", "anand.kulkarni@company.com"],
        "oil & gas": ["parthasarathi@company.com", "mayank.agarwal@company.com"],
    },
    "capacity expansion": {
        "intl t&d": ["abhishek.sil@company.com", "ganesh.srinivasan@company.com"],
        "india t&d": ["nk.gupta@company.com", "rajinder.gupta@company.com"],
    },
    "new market entry": {
        "intl t&d": ["vishal@company.com", "abhishek.sil@company.com", "ganesh.srinivasan@company.com"],
        "india t&d": ["vishal@company.com", "nk.gupta@company.com", "rajinder.gupta@company.com"],
        "civil": ["vishal@company.com", "jaideep@company.com", "raman.kapil@company.com"],
        "transportation": ["vishal@company.com", "suresh.kumar@company.com", "ankur.dev@company.com"],
        "renewables": ["vishal@company.com", "abhishek.sil@company.com", "anand.kulkarni@company.com"],
        "oil & gas": ["vishal@company.com", "parthasarathi@company.com", "mayank.agarwal@company.com"],
    },
    "partnerships & alliances": {
        "intl t&d": ["abhishek.sil@company.com", "ganesh.srinivasan@company.com"],
        "india t&d": ["nk.gupta@company.com", "rajinder.gupta@company.com"],
        "civil": ["jaideep@company.com", "raman.kapil@company.com"],
        "transportation": ["suresh.kumar@company.com", "ankur.dev@company.com"],
        "renewables": ["abhishek.sil@company.com", "anand.kulkarni@company.com"],
        "oil & gas": ["parthasarathi@company.com", "mayank.agarwal@company.com"],
    },
    "mergers & acquisitions": {
        "intl t&d": ["vishal@company.com", "abhishek.sil@company.com", "ganesh.srinivasan@company.com"],
        "india t&d": ["vishal@company.com", "nk.gupta@company.com", "rajinder.gupta@company.com"],
        "civil": ["vishal@company.com", "jaideep@company.com", "raman.kapil@company.com"],
        "transportation": ["vishal@company.com", "suresh.kumar@company.com", "ankur.dev@company.com"],
        "renewables": ["vishal@company.com", "abhishek.sil@company.com", "anand.kulkarni@company.com"],
        "oil & gas": ["vishal@company.com", "parthasarathi@company.com", "mayank.agarwal@company.com"],
    },
    "regulatory & policy": {
        "intl t&d": ["abhishek.sil@company.com", "sbu.head.intltd@company.com"],
        "india t&d": ["nk.gupta@company.com", "sbu.head.indiatd@company.com"],
        "civil": ["jaideep@company.com", "sbu.head.civil@company.com"],
        "transportation": ["suresh.kumar@company.com", "sbu.head.transportation@company.com"],
        "renewables": ["abhishek.sil@company.com", "sbu.head.renewables@company.com"],
        "oil & gas": ["parthasarathi@company.com", "sbu.head.oilgas@company.com"],
    },
    "project execution": {
        "intl t&d": ["abhishek.sil@company.com", "ganesh.srinivasan@company.com"],
        "india t&d": ["nk.gupta@company.com", "rajinder.gupta@company.com"],
        "civil": ["jaideep@company.com", "raman.kapil@company.com"],
        "transportation": ["suresh.kumar@company.com", "ankur.dev@company.com"],
        "renewables": ["abhishek.sil@company.com", "anand.kulkarni@company.com"],
        "oil & gas": ["parthasarathi@company.com", "mayank.agarwal@company.com"],
    },
}

PEOPLE_NAMES = {
    "abhishek.sil@company.com": "Abhishek Sil",
    "nk.gupta@company.com": "NK Gupta",
    "jaideep@company.com": "Jaideep",
    "suresh.kumar@company.com": "Suresh Kumar",
    "parthasarathi@company.com": "Parthasarathi",
    "vishal@company.com": "Vishal",
    "ganesh.srinivasan@company.com": "Ganesh Srinivasan",
    "rajinder.gupta@company.com": "Rajinder Gupta",
    "raman.kapil@company.com": "Raman Kapil",
    "ankur.dev@company.com": "Ankur Dev",
    "anand.kulkarni@company.com": "Anand Kulkarni",
    "mayank.agarwal@company.com": "Mayank Agarwal",
    "sbu.head.intltd@company.com": "SBU Head (Intl T&D)",
    "sbu.head.indiatd@company.com": "SBU Head (India T&D)",
    "sbu.head.civil@company.com": "SBU Head (Civil)",
    "sbu.head.transportation@company.com": "SBU Head (Transportation)",
    "sbu.head.renewables@company.com": "SBU Head (Renewables)",
    "sbu.head.oilgas@company.com": "SBU Head (Oil & Gas)",
}


def get_recipients_for_article(category: str, sbus: list) -> list:
    """Get unique list of recipient emails for a given category and SBU list"""
    category_lower = category.lower().strip()
    matrix = ROUTING_MATRIX.get(category_lower, {})
    recipients = set()
    for sbu in sbus:
        sbu_lower = sbu.lower().strip()
        emails = matrix.get(sbu_lower, [])
        recipients.update(emails)
    return list(recipients)


@app.get("/api/routing/preview")
def preview_routing():
    """Preview who would receive emails based on current articles"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, news_title, category_tag, sbu_tagging
            FROM processed_articles
            WHERE published_date >= CURRENT_DATE - INTERVAL '7 days'
            AND category_tag IS NOT NULL
            ORDER BY published_date DESC
        """)
        articles = cur.fetchall()
        cur.close()
        conn.close()

        recipient_map = {}
        for article in articles:
            category = article.get('category_tag', '')
            sbus = [s.strip() for s in (article.get('sbu_tagging') or '').split(',') if s.strip()]
            recipients = get_recipients_for_article(category, sbus)
            for email in recipients:
                if email not in recipient_map:
                    recipient_map[email] = {
                        "name": PEOPLE_NAMES.get(email, email),
                        "email": email,
                        "articles": []
                    }
                recipient_map[email]["articles"].append({
                    "id": article.get('id'),
                    "title": article.get('news_title'),
                    "category": category,
                    "sbu": article.get('sbu_tagging'),
                })

        return {
            "status": "success",
            "total_recipients": len(recipient_map),
            "preview": list(recipient_map.values())
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    # ─── EMAIL SENDING ────────────────────────────────────────────────────────────

import resend

def deduplicate_articles(articles):
    """Same deduplication logic as frontend Executive Summary"""
    stop_words = {'a','an','the','and','or','in','on','at','to','for','of','with','by','is','was','has','this','that','their','these','will','been','also','from'}
    
    def jaccard(text1, text2):
        w1 = [w for w in text1.lower().split() if len(w) > 3 and w not in stop_words]
        w2 = [w for w in text2.lower().split() if len(w) > 3 and w not in stop_words]
        if not w1 or not w2:
            return 0
        set2 = set(w2)
        intersection = len([w for w in w1 if w in set2])
        union = len(set(w1 + w2))
        return intersection / union if union > 0 else 0

    deduped = []
    for article in articles:
        is_duplicate = False
        for kept in deduped:
            s1 = article.get('summary') or article.get('title') or ''
            s2 = kept.get('summary') or kept.get('title') or ''
            j = jaccard(s1, s2)

            if j > 0.35:
                is_duplicate = True
                break

            # Check shared competitors
            c1 = set(article.get('competitors', []))
            c2 = set(kept.get('competitors', []))
            shared = c1 & c2

            if not shared:
                continue

            v1 = article.get('contract_value')
            v2 = kept.get('contract_value')
            if v1 and v2 and abs(v1 - v2) / max(v1, v2) < 0.10:
                is_duplicate = True
                break

            if shared and j > 0.20:
                is_duplicate = True
                break

            g1 = (article.get('geography') or '').lower()
            g2 = (kept.get('geography') or '').lower()
            if shared and g1 and g2 and g1 == g2:
                is_duplicate = True
                break

        if not is_duplicate:
            deduped.append(article)

    return deduped


def group_articles_for_email(articles):
    """Group by category, deduplicate, max 5 per category, in correct order"""
    CATEGORY_ORDER = [
        'order wins', 'bidding activity', 'project execution',
        'mergers & acquisitions', 'partnerships & alliances',
        'financial', 'new market entry', 'capacity expansion',
        'regulatory & policy', 'partnerships & alliances'
    ]
    EXCLUDE_CATEGORIES = {'stock market', 'industry trends'}

    grouped = {}
    for a in articles:
        cat = (a.get('category') or 'general').lower().strip()
        if cat in EXCLUDE_CATEGORIES:
            continue
        if cat not in grouped:
            grouped[cat] = []
        grouped[cat].append(a)

    # Sort each category by rank_score then date, deduplicate, limit to 5
    result = {}
    for cat, items in grouped.items():
        items.sort(key=lambda x: (-(x.get('rank_score') or 0), x.get('date') or ''), )
        items = deduplicate_articles(items)
        result[cat] = items[:5]

    # Order categories
    ordered = {}
    for cat in CATEGORY_ORDER:
        if cat in result:
            ordered[cat] = result[cat]
    for cat in sorted(result.keys()):
        if cat not in ordered:
            ordered[cat] = result[cat]

    return ordered


def _get_executive_cluster_list(articles: list) -> list:
    """Port of the frontend's getExecutiveClusterList: keeps one
    representative article per cluster_id, preferring the row already
    flagged is_representative_article. Articles with no cluster_id pass
    through untouched (handled by the Jaccard fallback next)."""
    result = []
    cluster_index = {}
    for article in articles:
        cid = article.get('cluster_id')
        if cid is None or cid == '':
            result.append(article)
            continue
        if cid not in cluster_index:
            cluster_index[cid] = len(result)
            result.append(article)
        else:
            pos = cluster_index[cid]
            existing = result[pos]
            if existing.get('is_representative_article') is False and article.get('is_representative_article') is not False:
                result[pos] = article
    return result


_STOPWORDS = {'a', 'an', 'the', 'and', 'or', 'in', 'on', 'at', 'to', 'for', 'of',
              'with', 'by', 'is', 'was', 'has', 'this', 'that', 'their', 'these',
              'will', 'been', 'also', 'from'}


def _jaccard_similarity(text1: str, text2: str) -> float:
    w1 = {w for w in re.split(r'\W+', text1.lower()) if len(w) > 3 and w not in _STOPWORDS}
    w2 = {w for w in re.split(r'\W+', text2.lower()) if len(w) > 3 and w not in _STOPWORDS}
    if not w1 or not w2:
        return 0.0
    return len(w1 & w2) / len(w1 | w2)


def _dedupe_similar_articles(articles: list) -> list:
    """Port of the frontend's dedupeSimilarArticles Jaccard fallback, for
    articles with no cluster_id to group by."""
    kept = []
    for article in articles:
        has_cluster = article.get('cluster_id') not in (None, '')
        if has_cluster:
            kept.append(article)
            continue
        is_duplicate = False
        for k in kept:
            summary1 = article.get('summary') or article.get('title') or ''
            summary2 = k.get('summary') or k.get('title') or ''
            jaccard = _jaccard_similarity(summary1, summary2)
            if jaccard > 0.35:
                is_duplicate = True
                break
            shared_competitor = bool(article.get('competitors')) and bool(k.get('competitors')) and \
                any(c in k['competitors'] for c in article['competitors'])
            if not shared_competitor:
                continue
            val1, val2 = article.get('contract_value'), k.get('contract_value')
            if val1 and val2 and abs(val1 - val2) / max(val1, val2) < 0.10:
                is_duplicate = True
                break
            if jaccard > 0.20:
                is_duplicate = True
                break
            if article.get('geography') and k.get('geography') and \
               str(article['geography']).lower() == str(k['geography']).lower():
                is_duplicate = True
                break
        if not is_duplicate:
            kept.append(article)
    return kept


def get_deduped_event_list(articles: list) -> list:
    return _dedupe_similar_articles(_get_executive_cluster_list(articles))


def _tier_aware_sort_key(article: dict):
    tier = article.get('competitor_tier')
    tier = 4 if tier is None else tier
    impact = article.get('event_impact_score') or 0
    rank = article.get('rank_score') or 0
    date_obj = article.get('date_obj')
    # Sort: impact desc, tier asc (1 = most important), rank desc, date desc
    return (-impact, tier, -rank, -(date_obj.toordinal() if date_obj else 0))


def _crisp_bullet_rows(articles: list) -> str:
    """Render a list of articles as bare-fact bullet rows: summary text,
    contract value if present, date, and a read-more link. No source badge,
    no confidence labels, no strategic commentary — matches the dashboard's
    bare-facts-only tabs."""
    if not articles:
        return '<tr><td style="padding:12px 16px;color:#666666;font-size:13px;font-family:Arial,sans-serif;font-style:italic;">No high-impact events this week.</td></tr>'

    rows = ''
    for idx, article in enumerate(articles):
        summary = article.get('summary', '') or article.get('title', '')
        competitors = article.get('competitors', [])
        for c in sorted(competitors, key=len, reverse=True):
            if c and c != '-':
                summary = summary.replace(c, f'<strong>{c}</strong>')

        try:
            fd = datetime.fromisoformat(article.get('date', '')).strftime('%b %d')
        except Exception:
            fd = (article.get('date') or '')[:10]

        value_line = ''
        if article.get('contract_value'):
            value_line = f'<p style="margin:2px 0 0 0;font-size:13px;color:#0F2B4C;font-weight:bold;font-family:Arial,sans-serif;">₹{article["contract_value"]:,.0f} Cr</p>'

        border_top = 'border-top:1px solid #E5E2D0;' if idx > 0 else ''

        rows += f"""
        <tr>
          <td style="padding:14px 16px;{border_top}background:#FFFFFF;">
            <p style="margin:0;font-size:14px;color:#333333;line-height:1.7;font-family:Arial,sans-serif;">{summary}</p>
            {value_line}
            <p style="margin:6px 0 0 0;font-size:12px;color:#666666;font-family:Arial,sans-serif;">
              {fd}
              &nbsp;&nbsp;<a href="{article.get('link', '#')}" style="color:#2E6EB5;font-weight:bold;text-decoration:none;">Open →</a>
            </p>
          </td>
        </tr>"""
    return rows


def _crisp_section_header(label: str) -> str:
    return f"""
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:8px;">
      <tr>
        <td style="background:#1A3D6D;padding:12px 20px;border-radius:6px;">
          <p style="margin:0;font-size:13px;font-weight:bold;color:#C9A84C;letter-spacing:2px;text-transform:uppercase;font-family:Arial,sans-serif;">{label}</p>
        </td>
      </tr>
    </table>"""


def _crisp_email_shell(recipient_name: str, intro: str, sections_html: str) -> str:
    """Shared HTML shell for both crisp digest variants (multi-SBU and
    single-SBU/category-wise) — same visual language as build_email_html,
    kept as one shell so both variants stay visually consistent."""
    return f"""<!DOCTYPE html>
<html xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office">
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0;padding:0;background-color:#F9F8F3;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#F9F8F3;">
    <tr>
      <td align="center" style="padding:0;">
        <table width="800" cellpadding="0" cellspacing="0" border="0" style="max-width:800px;width:100%;">
          <tr>
            <td style="background:#0F2B4C;padding:32px;text-align:center;">
              <h1 style="margin:0;font-size:24px;color:#FFFFFF;font-family:Georgia,serif;font-weight:bold;">Competitor Intelligence</h1>
              <p style="margin:8px 0 0 0;font-size:12px;color:#C9A84C;letter-spacing:3px;font-family:Arial,sans-serif;">WEEKLY BRIEF</p>
            </td>
          </tr>
          <tr>
            <td style="background:#FFFFFF;padding:32px;">
              <p style="margin:0 0 8px 0;font-size:15px;color:#333333;font-family:Arial,sans-serif;">Hi <strong>{recipient_name}</strong>,</p>
              <p style="margin:0 0 28px 0;font-size:14px;color:#666666;line-height:1.6;font-family:Arial,sans-serif;">{intro}</p>
              {sections_html}
              <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border-left:4px solid #C9A84C;background:#F9F8F3;margin-top:16px;">
                <tr>
                  <td style="padding:14px 16px;">
                    <p style="margin:0;font-size:13px;color:#666666;font-family:Arial,sans-serif;">
                      Log in to the <a href="https://competitor-intelligence-dashboard-u.vercel.app/index.html" style="color:#FFFFFF;font-weight:bold;text-decoration:none;">KEC Intel Platform</a> for full details.
                    </p>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
          <tr>
            <td style="background:#0F2B4C;padding:16px;text-align:center;">
              <p style="margin:0;font-size:12px;color:rgba(255,255,255,0.5);font-family:Arial,sans-serif;">KEC Competitor Intelligence Platform · Weekly Digest</p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


# These mirror the frontend's tab constants exactly (index.html:
# BRIEF_MIN_IMPACT_SCORE/BRIEF_MAX_PER_CATEGORY for Executive Brief,
# STORYLINE_MIN_IMPACT_SCORE/STORYLINE_MAX_EVENTS_PER_SBU for SBU
# Storylines, CLIENT_TRACKER_LOOKBACK_DAYS for the authority sub-section)
# so the email and dashboard never diverge again.
DIGEST_BRIEF_MIN_IMPACT_SCORE = 150
DIGEST_BRIEF_LOOKBACK_DAYS = 14
DIGEST_BRIEF_MAX_PER_CATEGORY = 5

DIGEST_STORYLINE_MIN_IMPACT_SCORE = 120
DIGEST_STORYLINE_LOOKBACK_DAYS = 14
DIGEST_STORYLINE_MAX_EVENTS_PER_SBU = 6

DIGEST_AUTHORITY_LOOKBACK_DAYS = 90
DIGEST_AUTHORITY_MAX_ITEMS_PER_SBU = 3
DIGEST_AUTHORITY_QUERY_TYPES = {'site_client_authority', 'site_government_policy'}


def _within_lookback(article: dict, days: int) -> bool:
    d = article.get('date_obj')
    if not d:
        return False
    cutoff = date.today() - timedelta(days=days)
    d_date = d.date() if isinstance(d, datetime) else d
    return d_date >= cutoff


def _get_authority_items_for_sbu(articles: list, sbu: str, exclude_links: set) -> list:
    """Port of the frontend's getAuthorityItemsForSBU: authority-sourced
    items (authority query lens OR a detected client/authority), scoped to
    this SBU, excluding anything already shown in the main events list for
    the same SBU (fixes the duplicate-between-sections bug)."""
    sbu_lower = sbu.lower()
    items = []
    for a in articles:
        if not _within_lookback(a, DIGEST_AUTHORITY_LOOKBACK_DAYS):
            continue
        if a['link'] in exclude_links:
            continue
        has_authority_lens = a.get('search_query_type') in DIGEST_AUTHORITY_QUERY_TYPES
        has_detected_authority = bool((a.get('detected_client_authority') or '').strip())
        if not (has_authority_lens or has_detected_authority):
            continue
        sbus = [s.strip().lower() for s in (a.get('sbu_tagging') or '').split(',') if s.strip()]
        if sbu_lower not in sbus:
            continue
        items.append(a)
    items.sort(key=lambda a: (-(a.get('event_impact_score') or 0), -(a['date_obj'].toordinal() if a.get('date_obj') else 0)))
    return get_deduped_event_list(items)[:DIGEST_AUTHORITY_MAX_ITEMS_PER_SBU]


def _crisp_subsection_label(text: str) -> str:
    return f'<div style="margin-top:16px;padding-top:12px;border-top:1px dashed #E5E2D0;font-size:11px;font-weight:700;color:#666666;text-transform:uppercase;letter-spacing:0.5px;">{text}</div>'


def build_multi_sbu_crisp_html(recipient_name: str, articles_by_sbu: dict) -> str:
    """Multi-SBU users (including admins): mirrors the SBU Storylines tab —
    per-SBU sections, 14-day lookback, event_impact_score >= 120, deduped,
    tier-aware sorted, capped at 6 events, plus each SBU's Client/Authority
    Activity sub-section (90-day lookback, deduped against the main list)."""
    sections = ''
    for sbu, articles in articles_by_sbu.items():
        pool = [a for a in articles if _within_lookback(a, DIGEST_STORYLINE_LOOKBACK_DAYS)
                and (a.get('event_impact_score') or 0) >= DIGEST_STORYLINE_MIN_IMPACT_SCORE]
        pool.sort(key=_tier_aware_sort_key)
        main_events = get_deduped_event_list(pool)[:DIGEST_STORYLINE_MAX_EVENTS_PER_SBU]
        exclude_links = {a['link'] for a in main_events}
        authority_items = _get_authority_items_for_sbu(articles, sbu, exclude_links)

        rows = _crisp_bullet_rows(main_events)
        sections += _crisp_section_header(sbu)
        sections += f"""
        <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border:1px solid #E5E2D0;margin-bottom:8px;">
          {rows}
        </table>"""
        if authority_items:
            sections += _crisp_subsection_label('Client / Authority Activity')
            sections += f"""
            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border:1px solid #E5E2D0;margin-bottom:32px;">
              {_crisp_bullet_rows(authority_items)}
            </table>"""
        else:
            sections += '<div style="margin-bottom:32px;"></div>'
    intro = "Here are this week's high-impact events across your business units, matching your SBU Storylines dashboard view."
    return _crisp_email_shell(recipient_name, intro, sections)


def build_single_sbu_category_crisp_html(recipient_name: str, sbu: str, articles: list) -> str:
    """Single-SBU users: mirrors the Executive Brief tab, scoped to their
    one SBU — 14-day lookback, event_impact_score >= 150, deduped,
    tier-aware sorted, capped at 5 events per category."""
    pool = [a for a in articles if _within_lookback(a, DIGEST_BRIEF_LOOKBACK_DAYS)
            and (a.get('event_impact_score') or 0) >= DIGEST_BRIEF_MIN_IMPACT_SCORE]

    grouped = {}
    for a in pool:
        cat = (a.get('category') or 'general').lower()
        grouped.setdefault(cat, []).append(a)

    sections = ''
    for cat, items in grouped.items():
        items.sort(key=_tier_aware_sort_key)
        capped = get_deduped_event_list(items)[:DIGEST_BRIEF_MAX_PER_CATEGORY]
        rows = _crisp_bullet_rows(capped)
        sections += _crisp_section_header(cat)
        sections += f"""
        <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border:1px solid #E5E2D0;margin-bottom:32px;">
          {rows}
        </table>"""
    if not sections:
        sections = _crisp_section_header(sbu) + f"""
        <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border:1px solid #E5E2D0;margin-bottom:32px;">
          {_crisp_bullet_rows([])}
        </table>"""
    intro = f"Here are this week's high-impact {sbu} events, matching your Executive Brief dashboard view."
    return _crisp_email_shell(recipient_name, intro, sections)


def build_email_html(recipient_name: str, articles_by_sbu: dict) -> str:
    def render_articles(sbu, articles):
        grouped = group_articles_for_email(articles)
        if not grouped:
            return f'<tr><td style="padding:16px;color:#666666;font-size:14px;font-family:Arial,sans-serif;">No articles this week for {sbu}.</td></tr>'

        content = ''
        for cat, items in grouped.items():
            # Category header row
            content += f"""
            <tr>
              <td style="background:#0F2B4C;color:#FFFFFF;padding:10px 16px;font-size:11px;font-weight:bold;letter-spacing:1px;font-family:Arial,sans-serif;">
                {cat.upper()}
              </td>
            </tr>"""

            for idx, article in enumerate(items):
                title = article.get('title', '')
                summary = article.get('summary', '') or title
                link = article.get('link', '#')
                date = article.get('date', '')
                source = article.get('source', '')
                competitors = article.get('competitors', [])

                # Bold competitor names
                for c in sorted(competitors, key=len, reverse=True):
                    if c and c != '-':
                        summary = summary.replace(c, f'<strong>{c}</strong>')

                try:
                    from datetime import datetime
                    fd = datetime.fromisoformat(date).strftime('%b %d')
                except:
                    fd = date[:10] if date else ''

                border_top = 'border-top:1px solid #E5E2D0;' if idx > 0 else ''

                competitor_text = '  ·  '.join([c for c in competitors if c and c != '-'])

                meta_line = fd
                if source:
                    meta_line += f'  ·  {source}'
                if competitor_text:
                    meta_line += f'  ·  {competitor_text}'

                content += f"""
                <tr>
                  <td style="padding:14px 16px;{border_top}background:#FFFFFF;">
                    <p style="margin:0 0 6px 0;font-size:14px;color:#333333;line-height:1.7;font-family:Arial,sans-serif;">{summary}</p>
                    <p style="margin:0;font-size:12px;color:#666666;font-family:Arial,sans-serif;">
                      {meta_line}
                      &nbsp;&nbsp;<a href="{link}" style="color:#2E6EB5;font-weight:bold;text-decoration:none;">Read more →</a>
                    </p>
                  </td>
                </tr>"""

            # Spacer between categories
            content += '<tr><td style="padding:8px 0;background:#F9F8F3;"></td></tr>'

        return content

    # Build full email body
    sbu_sections = ''
    for sbu, articles in articles_by_sbu.items():
        rows = render_articles(sbu, articles)
        sbu_sections += f"""
        <!--  SBU HEADER  -->
        <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:8px;">
          <tr>
            <td style="background:#1A3D6D;padding:12px 20px;border-radius:6px;">
              <p style="margin:0;font-size:13px;font-weight:bold;color:#C9A84C;letter-spacing:2px;text-transform:uppercase;font-family:Arial,sans-serif;">{sbu}</p>
            </td>
          </tr>
        </table>

        <!--  SBU ARTICLES  -->
        <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border:1px solid #E5E2D0;margin-bottom:32px;">
          {rows}
        </table>"""

    return f"""<!DOCTYPE html>
<html xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office">
<head>
  <!--[if mso]>
  <xml><o:OfficeDocumentSettings><o:AllowPNG/><o:PixelsPerInch>96</o:PixelsPerInch></o:OfficeDocumentSettings></xml>
  <![endif]-->
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0;padding:0;background-color:#F9F8F3;">

  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#F9F8F3;">
    <tr>
      <td align="center" style="padding:0;">

        <!-- WRAPPER -->
        <table width="800" cellpadding="0" cellspacing="0" border="0" style="max-width:800px;width:100%;">

          <!-- HEADER -->
          <tr>
            <td style="background:#0F2B4C;padding:32px;text-align:center;">
              <h1 style="margin:0;font-size:24px;color:#FFFFFF;font-family:Georgia,serif;font-weight:bold;">Competitor Intelligence</h1>
              <p style="margin:8px 0 0 0;font-size:12px;color:#C9A84C;letter-spacing:3px;font-family:Arial,sans-serif;">WEEKLY ACTION DIGEST</p>
            </td>
          </tr>
          <!-- BANNER IMAGE -->
          <!-- <tr>
            <td style="padding:0;line-height:0;">
              <img src="cid:digest_banner@kec" alt="KEC Intel Banner" width="800"
                   style="display:block;width:100%;max-width:800px;border:0;" />
            </td>
          </tr> --> 
          <!-- BODY -->
          <tr>
            <td style="background:#FFFFFF;padding:32px;">

              <p style="margin:0 0 8px 0;font-size:15px;color:#333333;font-family:Arial,sans-serif;">Hi <strong>{recipient_name}</strong>,</p>
              <p style="margin:0 0 28px 0;font-size:14px;color:#666666;line-height:1.6;font-family:Arial,sans-serif;">Here are this week's competitor intelligence highlights relevant to your business unit.</p>

              {sbu_sections}

              <!-- FOOTER NOTE -->
              <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border-left:4px solid #C9A84C;background:#F9F8F3;margin-top:16px;">
                <tr>
                  <td style="padding:14px 16px;">
                    <p style="margin:0;font-size:13px;color:#666666;font-family:Arial,sans-serif;">
                                          Log in to the <a href="https://competitor-intelligence-dashboard-u.vercel.app/index.html" style="color:#FFFFFF;font-weight:bold;text-decoration:none;">KEC Intel Platform</a> for full details.
                    </p>
                  </td>
                </tr>
              </table>

            </td>
          </tr>

          <!-- FOOTER -->
          <tr>
            <td style="background:#0F2B4C;padding:16px;text-align:center;">
              <p style="margin:0;font-size:12px;color:rgba(255,255,255,0.5);font-family:Arial,sans-serif;">KEC Competitor Intelligence Platform · Weekly Digest</p>
            </td>
          </tr>

        </table>
        <!-- END WRAPPER -->

      </td>
    </tr>
  </table>

</body>
</html>"""
    
def generate_bu_summary(sbu: str, articles: list) -> str:
    """Generate a single AI paragraph summarising the week's competitor activity for one BU"""
    if not articles:
        return f"No significant competitor activity recorded in the {sbu} segment this week."

    # Build article context
    article_text = ""
    for i, a in enumerate(articles[:15]):  # cap at 15 articles per BU
        competitors = ', '.join(a.get('competitors', [])) or 'Unknown'
        article_text += (
            f"\n[{i+1}] Category: {a.get('category', '')} | "
            f"Competitor: {competitors} | "
            f"Date: {a.get('date', '')} | "
            f"Summary: {a.get('summary', a.get('title', ''))}"
        )

    
    prompt = f"""You are a competitive intelligence analyst at KEC International.

Below are this week's intelligence articles for the {sbu} business unit. Convert each distinct news item into a bullet point.

Format
- Competitor did X for Y project — ₹Z Cr / Geography.
- Next competitor did A for B — ₹C Cr / Geography.

Strict rules:
1. One bullet per distinct news item. Do not merge multiple articles into one bullet.
2. Every bullet MUST be a complete sentence. Never end mid-word or mid-sentence.
3. Include ₹ value and geography in every bullet where the data provides it.
4. Do NOT add any introduction, heading, summary, or closing line. Start directly with the first bullet.
5. Do NOT add strategic commentary like "this threatens KEC" or "KEC should watch".
6. If an article lacks enough detail for a meaningful bullet, skip it silently.

Articles:
{article_text}

Bullets:""" 

    try:
        api_key = os.environ.get('GEMINI_API_KEY')
        if not api_key:
            return f"AI summary unavailable for {sbu} this week."

        client = genai.Client(api_key=api_key)

        models_to_try = ["gemini-2.5-flash", "gemini-2.0-flash"]
        last_error = None
        for model_name in models_to_try:
            try:
                response = client.models.generate_content(
                    model=model_name,
                    contents=[types.Content(
                        role="user",
                        parts=[types.Part(text=prompt)]
                    )],
                    config=types.GenerateContentConfig(
                        temperature=0.3,
                        max_output_tokens=5000,
                    )
                )
                return response.text.strip()
            except Exception as e:
                last_error = e
                if '503' in str(e) or 'UNAVAILABLE' in str(e):
                    continue
                raise
        raise last_error

    except Exception as e:
        logging.error(f"BU summary generation failed for {sbu}: {e}")
        return f"AI summary unavailable for {sbu} this week."


def build_summary_digest_html(recipient_name: str, all_articles: list, sbu_alias_map: dict) -> str:
    """Build the executive AI-summary email for senior leadership"""

    sbu_sections = ''
    for sbu in ALL_SBUS:
        aliases = sbu_alias_map.get(sbu.lower(), [sbu.lower()])
        sbu_articles = [
            a for a in all_articles
            if any(alias in (a.get('sbu_tagging') or '').lower() for alias in aliases)
        ]

        MANUAL_OVERRIDES = {
            'Intl T&D': '• L&T won $992M energy infrastructure development contracts in Kuwait.',
            'India T&D': '• Texmaco Rail & Engineering Ltd received orders worth ₹11.65 Cr from Odisha Power Transmission.<br>• HG Infra Engineering received LOI from REC Power for Jharkhand transmission project.<br>• Tata Power received LOI from RECPDCL for development of 250-km transmission network across Karnataka, featuring 400 kV double-circuit lines, 220 kV lines and 220 kV underground cable systems.',
        }

        if sbu in MANUAL_OVERRIDES:
            summary_text = MANUAL_OVERRIDES[sbu]
        else:
            sbu_articles = deduplicate_articles(sbu_articles)
            summary_text = generate_bu_summary(sbu, sbu_articles)
            summary_text = summary_text.replace('*', '•').replace('\n', '<br>')
        article_count = len(sbu_articles)

        sbu_sections += f"""
        <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:28px;">
          <tr>
            <td style="background:#1A3D6D;padding:12px 20px;border-radius:6px 6px 0 0;">
              <p style="margin:0;font-size:12px;font-weight:bold;color:#C9A84C;letter-spacing:2px;text-transform:uppercase;font-family:Arial,sans-serif;">{sbu}</p>
              <p style="margin:4px 0 0;font-size:11px;color:rgba(255,255,255,0.45);font-family:Arial,sans-serif;">{article_count} article{"s" if article_count != 1 else ""} this week</p>
            </td>
          </tr>
          <tr>
            <td style="background:#FFFFFF;padding:20px 24px;border:1px solid #E5E2D0;border-top:none;border-radius:0 0 6px 6px;">
              <p style="margin:0;font-size:14px;color:#333333;line-height:1.8;font-family:Arial,sans-serif;">{summary_text}</p>
            </td>
          </tr>
        </table>"""

    return f"""<!DOCTYPE html>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0;padding:0;background-color:#F9F8F3;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#F9F8F3;">
    <tr>
      <td align="center" style="padding:0;">
        <table width="800" cellpadding="0" cellspacing="0" border="0" style="max-width:800px;width:100%;">

          <!-- HEADER -->
          <tr>
            <td style="background:#0F2B4C;padding:32px;text-align:center;">
              <h1 style="margin:0;font-size:24px;color:#FFFFFF;font-family:Georgia,serif;font-weight:bold;">Competitor Intelligence</h1>
              <p style="margin:8px 0 0;font-size:12px;color:#C9A84C;letter-spacing:3px;font-family:Arial,sans-serif;">WEEKLY EXECUTIVE BRIEF</p>
            </td>
          </tr>

          <!-- BODY -->
          <tr>
            <td style="background:#FFFFFF;padding:32px;">
              <p style="margin:0 0 6px;font-size:15px;color:#333333;font-family:Arial,sans-serif;">Hi <strong>{recipient_name}</strong>,</p>
              <p style="margin:0 0 28px;font-size:14px;color:#666666;line-height:1.6;font-family:Arial,sans-serif;">
                Here is your weekly executive summary of competitor activity across all six business units.
                Each section reflects the most significant developments from this week's intelligence database.
              </p>

              {sbu_sections}

              <!-- FOOTER NOTE -->
              <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border-left:4px solid #C9A84C;background:#F9F8F3;margin-top:8px;">
                <tr>
                  <td style="padding:14px 16px;">
                    <p style="margin:0;font-size:13px;color:#666666;font-family:Arial,sans-serif;">
                      Log in to the <a href="https://competitor-intelligence-dashboard-u.vercel.app/index.html" style="color:#2E6EB5;font-weight:bold;text-decoration:none;">KEC Intel Platform</a> for the full article breakdown.
                    </p>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- FOOTER -->
          <tr>
            <td style="background:#0F2B4C;padding:16px;text-align:center;">
              <p style="margin:0;font-size:12px;color:rgba(255,255,255,0.4);font-family:Arial,sans-serif;">KEC Competitor Intelligence Platform · Weekly Executive Brief</p>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""

@app.post("/api/send-digest")
def send_weekly_digest(token: str = ""):
    """Send weekly digest emails based on user SBU profiles"""
    try:
        if token:
            user = get_user_from_token(token)
            if not user or not user['is_admin']:
                raise HTTPException(status_code=403, detail="Admin access required")

        resend.api_key = os.environ.get('RESEND_API_KEY')
        from_email = os.environ.get('RESEND_FROM', 'onboarding@resend.dev')
        import base64
        banner_b64 = None
        try:
            banner_path = os.path.join(os.path.dirname(__file__), 'assets', 'banner.jpg')
            with open(banner_path, 'rb') as f:
                banner_b64 = base64.b64encode(f.read()).decode('utf-8')
        except Exception:
            pass

        # ── Step 1: Get all active users ──────────────────────────────────────
        local_conn = get_local_db()
        local_cur = local_conn.cursor(cursor_factory=RealDictCursor)
        local_cur.execute("""
            SELECT id, name, email, sbu_profile, is_admin
            FROM users WHERE is_active = TRUE
        """)
        users = local_cur.fetchall()
        local_cur.close()
        local_conn.close()

        # ── Step 2: Get this week's articles ──────────────────────────────────
        conn = get_db_connection()
        cur = conn.cursor()
        # Email digest uses the same high-impact threshold as the dashboard's
        # Executive Brief (event_impact_score >= 150, same excluded
        # categories) so the two surfaces never disagree on what counts as
        # "worth a CEO's time." No LLM call needed here — event_impact_score
        # and summary are already computed by the processor.
        # Change (digest rework): pull a wide 90-day pool once, unfiltered by
        # threshold/lookback — each builder below applies its OWN threshold
        # and lookback to exactly match its corresponding dashboard tab
        # (Executive Brief: 150 / 14 days; SBU Storylines: 120 / 14 days;
        # authority sub-section: no threshold / 90 days). Filtering once in
        # SQL with a single threshold/window was the root cause of the email
        # not matching what the dashboard actually shows.
        cur.execute("""
            SELECT id, news_title, category_tag, sbu_tagging,
                   summary, link, published_date, competitor_tagging,
                   contract_value_inr_crore, geography, rank_score,
                   event_impact_score, competitor_tier, "Source",
                   cluster_id, is_representative_article, cluster_summary,
                   cluster_article_count, search_query_type, detected_client_authority
            FROM processed_articles
            WHERE published_date >= CURRENT_DATE - INTERVAL '90 days'
              AND category_tag IS NOT NULL
              AND category_tag NOT IN ('stock market', 'industry trends', 'leadership/management')
            ORDER BY event_impact_score DESC NULLS LAST,
                     rank_score DESC NULLS LAST, published_date DESC
        """)
        raw_articles = cur.fetchall()
        cur.close()
        conn.close()

        # Normalize articles
        all_articles = []
        for a in raw_articles:
            competitors = [
                c.strip() for c in (a.get('competitor_tagging') or '').split(',')
                if c.strip() and c.strip() != '-'
            ]
            all_articles.append({
                'id': a.get('id'),
                'title': a.get('news_title', ''),
                'category': a.get('category_tag', ''),
                'sbu_tagging': a.get('sbu_tagging', ''),
                'summary': a.get('summary', ''),
                'cluster_summary': a.get('cluster_summary', ''),
                'link': a.get('link', '#'),
                'date_obj': a.get('published_date'),
                'date': a.get('published_date').isoformat() if a.get('published_date') else '',
                'source': a.get('Source', ''),
                'competitors': competitors,
                'contract_value': safe_float(a.get('contract_value_inr_crore')),
                'geography': a.get('geography'),
                'rank_score': a.get('rank_score') or 0,
                'event_impact_score': a.get('event_impact_score') or 0,
                'competitor_tier': a.get('competitor_tier'),
                'cluster_id': a.get('cluster_id'),
                'is_representative_article': a.get('is_representative_article'),
                'cluster_article_count': a.get('cluster_article_count') or 1,
                'search_query_type': a.get('search_query_type') or '',
                'detected_client_authority': a.get('detected_client_authority') or '',
            })

        # ── Step 3: Send to each user ─────────────────────────────────────────
        sent = []
        failed = []
        skipped = []

        log_conn = get_local_db()
        log_cur = log_conn.cursor()

        for u in users:
            sbu_profile = (u.get('sbu_profile') or '').strip()
            is_admin = u.get('is_admin', False)

            # Build articles_by_sbu dict
            if is_admin or sbu_profile == 'Admin':
                # Admin sees all SBUs
                sbus = ['Intl T&D', 'India T&D', 'Civil', 'Transportation', 'Renewables', 'Oil & Gas']
                articles_by_sbu = {}
                SBU_ALIAS_MAP = {
                    'intl t&d': ['intl t&d', 'international t&d'],
                    'india t&d': ['india t&d'],
                    'civil': ['civil'],
                    'transportation': ['transportation'],
                    'renewables': ['renewables'],
                    'oil & gas': ['oil & gas'],
                }
                for sbu in sbus:
                    aliases = SBU_ALIAS_MAP.get(sbu.lower(), [sbu.lower()])
                    sbu_articles = [
                        a for a in all_articles
                        if any(
                            alias in (a.get('sbu_tagging') or '').lower()
                            for alias in aliases
                        )
                    ]
                    if sbu_articles:
                        articles_by_sbu[sbu] = sbu_articles
            else:
                # Map user SBU names to article SBU names
                SBU_ALIAS_MAP = {
                    'intl t&d': ['intl t&d', 'international t&d'],
                    'india t&d': ['india t&d'],
                    'civil': ['civil'],
                    'transportation': ['transportation'],
                    'renewables': ['renewables'],
                    'oil & gas': ['oil & gas'],
                }

                sbus = [s.strip() for s in sbu_profile.split(',') if s.strip()]
                articles_by_sbu = {}
                for sbu in sbus:
                    aliases = SBU_ALIAS_MAP.get(sbu.lower(), [sbu.lower()])
                    sbu_articles = [
                        a for a in all_articles
                        if any(
                            alias in (a.get('sbu_tagging') or '').lower()
                            for alias in aliases
                        )
                    ]
                    if sbu_articles:
                        articles_by_sbu[sbu] = sbu_articles

            if not articles_by_sbu:
                skipped.append(u['email'])
                continue

            # Profile-driven branching (retires the old hardcoded
            # SUMMARY_DIGEST_EMAILS allowlist): admins and anyone mapped to
            # more than one SBU get a crisp SBU-wise brief; users mapped to
            # exactly one SBU get a crisp category-wise brief within that SBU.
            is_multi_sbu = is_admin or sbu_profile == 'Admin' or len(sbus) > 1

            try:
                if is_multi_sbu:
                    html = build_multi_sbu_crisp_html(u['name'], articles_by_sbu)
                    subject = "[KEC Intel] Weekly Brief — " + (", ".join(sbus) if not (is_admin or sbu_profile == 'Admin') else "All SBUs")
                else:
                    single_sbu = sbus[0] if sbus else sbu_profile
                    single_sbu_articles = articles_by_sbu.get(single_sbu, [])
                    html = build_single_sbu_category_crisp_html(u['name'], single_sbu, single_sbu_articles)
                    subject = f"[KEC Intel] Weekly Brief — {single_sbu}"
                to_email = os.environ.get('TEST_EMAIL', u['email']) \
                    if os.environ.get('TEST_MODE') == 'true' else u['email']

                resend.Emails.send({
                    "from": from_email,
                    "to": [to_email],
                    "subject": subject,
                    "html": html,
                    **({"attachments": [{"filename": "banner.jpg", "content": banner_b64, "content_type": "image/jpeg", "content_id": "digest_banner@kec"}]} if banner_b64 else {})
                })
                sent.append(u['email'])
                log_cur.execute("""
                    INSERT INTO email_log (recipient_email, email_type, subject, status)
                    VALUES (%s, %s, %s, %s)
                """, (u['email'], 'weekly_digest', f"Weekly Digest — {sbu_profile}", 'sent'))

            except Exception as e:
                failed.append({"email": u['email'], "error": str(e)})
                log_cur.execute("""
                    INSERT INTO email_log (recipient_email, email_type, subject, status)
                    VALUES (%s, %s, %s, %s)
                """, (u['email'], 'weekly_digest', f"Weekly Digest — {sbu_profile}", 'failed'))

        log_conn.commit()
        log_cur.close()
        log_conn.close()

        return {
            "status": "success",
            "sent": len(sent),
            "skipped": len(skipped),
            "failed": len(failed),
            "sent_to": sent,
            "skipped_users": skipped,
            "failures": failed
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    # ─── AUTH & PROFILE SYSTEM ───────────────────────────────────────────────────

import secrets
from datetime import timedelta
import hashlib
from pydantic import BaseModel as PydanticBase

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(plain: str, hashed: str) -> bool:
    return hashlib.sha256(plain.encode()).hexdigest() == hashed

SBU_PROFILES = [
    "Intl T&D",
    "India T&D", 
    "Civil",
    "Transportation",
    "Renewables",
    "Oil & Gas",
    "Admin"
]

# ─── Auth helpers ─────────────────────────────────────────────────────────────


def create_session_token(user_id: int) -> str:
    token = secrets.token_urlsafe(32)
    conn = get_local_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sessions (user_id, token, expires_at)
        VALUES (%s, %s, NOW() + INTERVAL '7 days')
    """, (user_id, token))
    conn.commit()
    cur.close()
    conn.close()
    return token

def get_user_from_token(token: str):
    conn = get_local_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT u.id, u.name, u.email, u.sbu_profile, u.is_admin
        FROM sessions s
        JOIN users u ON u.id = s.user_id
        WHERE s.token = %s 
        AND s.expires_at > NOW()
        AND u.is_active = TRUE
    """, (token,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    return dict(user) if user else None

# ─── Auth endpoints ───────────────────────────────────────────────────────────

class LoginRequest(PydanticBase):
    email: str
    password: str

class CreateUserRequest(PydanticBase):
    name: str
    email: str
    password: str = ""
    sbu_profile: str
    is_admin: bool = False

@app.post("/api/auth/login")
def login(req: LoginRequest):
    """Login with email and password"""
    try:
        conn = get_local_db()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        email = req.email.lower().strip()
        if '@' not in email:
            email = email + '@kecrpg.com'
        cur.execute("SELECT * FROM users WHERE email = %s AND is_active = TRUE", (email,))
        user = cur.fetchone()
        cur.close()

        if not user:
            raise HTTPException(status_code=401, detail="Email not found. Please contact your administrator.")

        # Admin accounts require password
        if user['is_admin']:
            if not req.password or not verify_password(req.password, user['password_hash']):
                raise HTTPException(status_code=401, detail="Invalid password.")

        # Update last login
        cur = conn.cursor()
        cur.execute("UPDATE users SET last_login = NOW() WHERE id = %s", (user['id'],))
        conn.commit()
        cur.close()
        conn.close()

        token = create_session_token(user['id'])

        return {
            "status": "success",
            "token": token,
            "user": {
                "id": user['id'],
                "name": user['name'],
                "email": user['email'],
                "sbu_profile": user['sbu_profile'],
                "is_admin": user['is_admin']
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/auth/logout")
def logout(token: str):
    """Invalidate session token"""
    try:
        conn = get_local_db()
        cur = conn.cursor()
        cur.execute("DELETE FROM sessions WHERE token = %s", (token,))
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/auth/me")
def get_me(token: str):
    """Get current user from token"""
    user = get_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired session")
    return {"status": "success", "user": user}


@app.post("/api/auth/create-user")
def create_user(req: CreateUserRequest):
    """Create a new user (admin only in production — open for setup now)"""
    try:
        # Support comma-separated SBUs e.g. "Intl T&D,Civil"
        selected_sbus = [s.strip() for s in req.sbu_profile.split(',') if s.strip()]
        invalid = [s for s in selected_sbus if s not in SBU_PROFILES]
        if not selected_sbus or invalid:
            raise HTTPException(status_code=400, 
                detail=f"Invalid SBU profile. Must be one of: {SBU_PROFILES}")

        conn = get_local_db()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if email already exists
        cur.execute("SELECT id FROM users WHERE email = %s", (req.email.lower().strip(),))
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="Email already exists")

        cur = conn.cursor()
        cur.execute("""
            INSERT INTO users (name, email, password_hash, sbu_profile, is_admin)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (
            req.name,
            req.email.lower().strip(),
            hash_password(req.password),    
            req.sbu_profile,
            req.is_admin
        ))
        user_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()

        return {"status": "success", "user_id": user_id, "message": f"User {req.name} created"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/auth/users")
def list_users(token: str):
    """List all users — admin only"""
    user = get_user_from_token(token)
    if not user or not user['is_admin']:
        raise HTTPException(status_code=403, detail="Admin access required")
    try:
        conn = get_local_db()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT id, name, email, sbu_profile, is_admin, is_active, 
                   created_at, last_login 
            FROM users ORDER BY sbu_profile, name
        """)
        users = cur.fetchall()
        cur.close()
        conn.close()
        return {"status": "success", "users": [dict(u) for u in users]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/auth/users/{user_id}")
def update_user(user_id: int, req: CreateUserRequest, token: str):
    """Update a user — admin only"""
    admin = get_user_from_token(token)
    if not admin or not admin['is_admin']:
        raise HTTPException(status_code=403, detail="Admin access required")
    try:
        selected_sbus = [s.strip() for s in req.sbu_profile.split(',') if s.strip()]
        invalid = [s for s in selected_sbus if s not in SBU_PROFILES]
        if not selected_sbus or invalid:
            raise HTTPException(status_code=400, detail=f"Invalid SBU profile. Must be one of: {SBU_PROFILES}")

        conn = get_local_db()  # rest stays the same
        conn = get_local_db()
        cur = conn.cursor()
        cur.execute("""
            UPDATE users 
            SET name = %s, email = %s, sbu_profile = %s, is_admin = %s
            WHERE id = %s
        """, (req.name, req.email.lower(), req.sbu_profile, req.is_admin, user_id))
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success", "message": "User updated"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/auth/users/{user_id}")
def deactivate_user(user_id: int, token: str):
    """Deactivate a user — admin only"""
    admin = get_user_from_token(token)
    if not admin or not admin['is_admin']:
        raise HTTPException(status_code=403, detail="Admin access required")
    try:
        conn = get_local_db()
        cur = conn.cursor()
        cur.execute("UPDATE users SET is_active = FALSE WHERE id = %s", (user_id,))
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success", "message": "User deactivated"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/auth/users/{user_id}/activate")
def activate_user(user_id: int, token: str):
    """Activate a deactivated user — admin only"""
    admin = get_user_from_token(token)
    if not admin or not admin['is_admin']:
        raise HTTPException(status_code=403, detail="Admin access required")
    try:
        conn = get_local_db()
        cur = conn.cursor()
        cur.execute("UPDATE users SET is_active = TRUE WHERE id = %s", (user_id,))
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success", "message": "User activated"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/auth/users/{user_id}/delete")
def delete_user(user_id: int, token: str):
    """Permanently delete a user — admin only"""
    admin = get_user_from_token(token)
    if not admin or not admin['is_admin']:
        raise HTTPException(status_code=403, detail="Admin access required")
    try:
        conn = get_local_db()
        cur = conn.cursor()
        cur.execute("DELETE FROM sessions WHERE user_id = %s", (user_id,))
        cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success", "message": "User deleted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Profile-filtered data endpoint ──────────────────────────────────────────

@app.get("/api/data/profile")
def get_profile_data(token: str):
    """Get articles filtered to the user's SBU profile"""
    user = get_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired session")

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Change 18: full column list, matching /api/data exactly (both
        # branches) — see _clean_article_row for why parity here matters.
        #
        # Admin sees everything, SBU users see only their SBU.
        if user['is_admin']:
            # Change 18 FIX: this was a plain (non-f) triple-quoted string
            # containing a literal "{conditions}" placeholder that was never
            # substituted — every admin call sent invalid SQL
            # ("WHERE {conditions}") straight to Postgres and got a
            # guaranteed syntax-error 500. Admin has no filter to apply, so
            # there's simply no WHERE clause here.
            query = """
                SELECT id, published_date, news_title, link, "Source",
                    relevance_score, competitor_tagging, sbu_tagging,
                    category_tag, summary, scraped_content, contract_value_inr_crore,
                    geography, competitor_tier, rank_score, processed_at,
                    source_domain, source_type, source_category, source_priority,
                    source_authority_score, preferred_for_executive_summary,
                    source_notes, source_match_method,
                    search_query, search_query_type, detected_client_authority,
                    detected_strategic_theme, accepted_by_gate,
                    cluster_id, relationship_type, is_representative_article,
                    cluster_title, cluster_summary, cluster_article_count,
                    cluster_representative_article_id, cluster_source_confidence,
                    cluster_rank_score, cluster_competitors, cluster_sbus,
                    cluster_categories, cluster_primary_source,
                    cluster_primary_source_type, cluster_primary_url,
                    event_impact_score,
                    actionability_score, confidence_score, sbu_fit_score
                FROM processed_articles
                ORDER BY
                    CASE WHEN rank_score IS NULL THEN 1 ELSE 0 END,
                    rank_score DESC, published_date DESC
            """
            cur.execute(query)
        else:
            sbus = [s.strip() for s in user['sbu_profile'].split(',') if s.strip()]
            conditions = " OR ".join(["sbu_tagging ILIKE %s" for _ in sbus])
            params = [f"%{sbu}%" for sbu in sbus]
            # Change 18: widened to match /api/data's full column list — this
            # SELECT was previously missing scraped_content, search_query,
            # search_query_type, detected_client_authority,
            # detected_strategic_theme, event_impact_score,
            # actionability_score, confidence_score, sbu_fit_score, meaning
            # every SBU-profile (non-admin) user was missing the exact fields
            # Executive Brief / SBU Storylines / Competitor Strategy /
            # Client-Authority Tracker filter and rank on.
            query = f"""
                SELECT id, published_date, news_title, link, "Source",
                    relevance_score, competitor_tagging, sbu_tagging,
                    category_tag, summary, scraped_content, contract_value_inr_crore,
                    geography, competitor_tier, rank_score, processed_at,
                    source_domain, source_type, source_category, source_priority,
                    source_authority_score, preferred_for_executive_summary,
                    source_notes, source_match_method,
                    search_query, search_query_type, detected_client_authority,
                    detected_strategic_theme, accepted_by_gate,
                    cluster_id, relationship_type, is_representative_article,
                    cluster_title, cluster_summary, cluster_article_count,
                    cluster_representative_article_id, cluster_source_confidence,
                    cluster_rank_score, cluster_competitors, cluster_sbus,
                    cluster_categories, cluster_primary_source,
                    cluster_primary_source_type, cluster_primary_url,
                    event_impact_score,
                    actionability_score, confidence_score, sbu_fit_score
                FROM processed_articles
                WHERE {conditions}
                ORDER BY 
                    CASE WHEN rank_score IS NULL THEN 1 ELSE 0 END,
                    rank_score DESC, published_date DESC
            """
            cur.execute(query, params)

        raw_results = cur.fetchall()
        cur.close()
        conn.close()

        # Change 18: use the same shared helper as /api/data — see
        # _clean_article_row's docstring for why this endpoint silently
        # dropped every cluster/event-impact field before.
        clean_results = [_clean_article_row(row) for row in raw_results]

        return {
            "status": "success",
            "count": len(clean_results),
            "sbu_profile": user['sbu_profile'],
            "data": clean_results
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def preprocess_chat_query(message: str):
    """Extract search keywords, category filter, and date filter from natural language."""
    msg = message.lower().strip()

    # Detect time references
    days = None
    time_patterns = {
        'today': 1, 'yesterday': 2,
        'this week': 7, 'past week': 7, 'last week': 14,
        'this month': 30, 'past month': 30, 'last month': 60,
        'this quarter': 90, 'last quarter': 180,
        'recent': 7, 'latest': 7, 'last few days': 5,
    }
    for phrase, d in time_patterns.items():
        if phrase in msg:
            days = d
            msg = msg.replace(phrase, '').strip()
            break

    # Detect category references
    category = None
    category_map = {
        'bidding activity': 'bidding activity', 'bidding': 'bidding activity',
        'bids': 'bidding activity', 'bid': 'bidding activity',
        'order wins': 'order wins', 'order win': 'order wins',
        'wins': 'order wins', 'won': 'order wins', 'bags': 'order wins', 'awarded': 'order wins',
        'merger': 'mergers & acquisitions', 'acquisition': 'mergers & acquisitions', 'm&a': 'mergers & acquisitions',
        'partnership': 'partnerships & alliances', 'alliance': 'partnerships & alliances',
        'jv': 'partnerships & alliances', 'joint venture': 'partnerships & alliances',
        'financial': 'financial', 'results': 'financial', 'revenue': 'financial',
        'quarterly': 'financial', 'profit': 'financial',
        'project execution': 'project execution', 'execution': 'project execution',
        'commissioned': 'project execution', 'completed': 'project execution',
        'stock': 'stock market', 'share price': 'stock market',
        'regulation': 'regulatory & policy', 'policy': 'regulatory & policy',
        'industry': 'industry trends', 'trend': 'industry trends',
    }
    for phrase, cat in sorted(category_map.items(), key=lambda x: -len(x[0])):
        if phrase in msg:
            category = cat
            break

    # Remove filler words from keywords
    filler = {'what', 'are', 'the', 'is', 'any', 'show', 'me', 'tell', 'about',
              'find', 'get', 'give', 'list', 'all', 'of', 'for', 'in', 'by',
              'from', 'to', 'a', 'an', 'do', 'does', 'has', 'have', 'been',
              'their', 'there', 'how', 'much', 'many', 'can', 'you', 'please',
              'among', 'amongst', 'between', 'with', 'did', 'was', 'were'}
    words = [w for w in msg.split() if w not in filler and len(w) > 2]
    keywords = ' '.join(words).strip()

    if not keywords and category:
        keywords = category

    return {"keywords": keywords, "category": category, "days": days}

# ─── CHAT GROUNDING HELPERS (Change 13) ─────────────────────────────────────────
CHAT_COMPETITOR_HINTS = {
    "l&t", "larsen", "toubro", "kalpataru", "kptl", "sterlite", "tata projects",
    "adani", "techno electric", "skipper", "bajaj", "siemens", "power mech",
    "afcons", "gr infra", "ncc", "megha", "ge vernova", "hitachi", "rvnl",
    "irb", "dilip buildcon", "patel engineering", "isgec",
}

def _chat_query_is_specific(parsed: dict, message: str) -> bool:
    """Specific = names both a category AND a competitor → tighter (top-15) set."""
    if not parsed or not parsed.get("category"):
        return False
    msg = (message or "").lower()
    return any(hint in msg for hint in CHAT_COMPETITOR_HINTS)

def _chat_fmt(value, fallback):
    """Safe display value: None/blank → fallback."""
    if value is None:
        return fallback
    text = str(value).strip()
    return text if text else fallback

def _build_cluster_context_block(row: dict) -> str:
    """Compact executive cluster-context block for one representative event."""
    return (
        f"[Event Cluster ID: {_chat_fmt(row.get('cluster_id'), 'n/a')}]\n"
        f"Category: {_chat_fmt(row.get('category_tag'), 'n/a')}\n"
        f"SBU: {_chat_fmt(row.get('sbu_tagging'), 'n/a')}\n"
        f"Competitors: {_chat_fmt(row.get('cluster_competitors') or row.get('competitor_tagging'), 'n/a')}\n"
        f"Client/Authority: {_chat_fmt(row.get('detected_client_authority'), 'n/a')}\n"
        f"Geography: {_chat_fmt(row.get('geography'), 'n/a')}\n"
        f"Value (INR crore): {_chat_fmt(row.get('contract_value_inr_crore'), 'n/a')}\n"
        f"Cluster size: {_chat_fmt(row.get('cluster_article_count'), '1')}\n"
        f"Confidence: {_chat_fmt(row.get('cluster_source_confidence'), 'Low')}\n"
        f"Impact score: {_chat_fmt(row.get('event_impact_score'), '0')}\n"
        f"Source: {_chat_fmt(row.get('cluster_primary_source') or row.get('Source'), 'n/a')} "
        f"({_chat_fmt(row.get('cluster_primary_source_type') or row.get('source_type'), 'n/a')})\n"
        f"Title: {_chat_fmt(row.get('news_title'), 'n/a')}\n"
        f"Executive summary: {_chat_fmt(row.get('cluster_summary') or row.get('summary'), 'n/a')}\n"
        f"Article link: {_chat_fmt(row.get('cluster_primary_url') or row.get('link'), 'n/a')}\n"
    )

def _build_chat_source(row: dict, impact: int) -> dict:
    """Cluster-first source object (superset of legacy keys so UI is unaffected)."""
    date = row.get("published_date")
    date_str = date.isoformat() if hasattr(date, "isoformat") else (str(date) if date else "")
    return {
        # legacy keys kept for existing frontend rendering
        "title": row.get("cluster_title") or row.get("news_title") or "",
        "link": row.get("cluster_primary_url") or row.get("link") or "",
        "date": date_str,
        "type": "database",
        "source_type": row.get("cluster_primary_source_type") or row.get("source_type") or "unknown",
        "source_authority_score": safe_int(row.get("source_authority_score")) or 5,
        # richer cluster-first fields
        "article_id": row.get("id"),
        "cluster_id": row.get("cluster_id"),
        "source": row.get("cluster_primary_source") or row.get("Source") or "unknown",
        "cluster_article_count": safe_int(row.get("cluster_article_count")) or 1,
        "cluster_source_confidence": row.get("cluster_source_confidence") or "Low",
        "event_impact_score": impact,
        "category_tag": row.get("category_tag") or "",
        "sbu_tagging": row.get("sbu_tagging") or "",
    }

# ─── CHATBOT ──────────────────────────────────────────────────────────────────

class ChatRequest(PydanticBase):
    message: str
    token: str
    conversation_history: list = []

@app.post("/api/chat")
def chat(req: ChatRequest):
    try:
        user = get_user_from_token(req.token)
        if not user:
            raise HTTPException(status_code=401, detail="Invalid or expired session")

        sbu_profile = user['sbu_profile']
        is_admin = user['is_admin']

        # ── Smart database search ─────────────────────────────────────────────
        parsed = preprocess_chat_query(req.message)
        search_keywords = parsed["keywords"]
        search_category = parsed["category"]
        search_days = parsed["days"]

        conn = get_db_connection()
        cur = conn.cursor()

        # Build dynamic WHERE clauses
        where_clauses = []
        params = []

        # SBU filter (non-admin only)
        if not is_admin:
            sbus = [s.strip() for s in sbu_profile.split(',') if s.strip()]
            sbu_conditions = " OR ".join(["sbu_tagging ILIKE %s" for _ in sbus])
            where_clauses.append(f"({sbu_conditions})")
            params.extend([f"%{sbu}%" for sbu in sbus])

        # Category filter (if detected)
        if search_category:
            where_clauses.append("category_tag ILIKE %s")
            params.append(f"%{search_category}%")

        # Date filter (if detected)
        if search_days:
            where_clauses.append("published_date >= CURRENT_DATE - (%s * INTERVAL '1 day')")
            params.append(search_days)

        # Full-text keyword search (if we have meaningful keywords beyond category)
        if search_keywords and search_keywords != search_category:
            where_clauses.append("""
                to_tsvector('english', COALESCE(news_title,'') || ' ' || COALESCE(summary,''))
                @@ plainto_tsquery('english', %s)
            """)
            params.append(search_keywords)

        # ── Cluster-first retrieval (Change 13) ───────────────────────────────
        # Ground only on representative articles (or legacy rows with no cluster).
        base_clause = "(is_representative_article = TRUE OR cluster_id IS NULL)"
        if where_clauses:
            where_sql = base_clause + " AND " + " AND ".join(where_clauses)
        else:
            where_sql = base_clause + " AND published_date >= CURRENT_DATE - INTERVAL '7 days'"

        # Specific (competitor + category) → tighter cluster set; else default top-25.
        row_limit = 15 if _chat_query_is_specific(parsed, req.message) else 25

        query = f"""
            SELECT
                id,
                published_date,
                news_title,
                link,
                "Source",
                relevance_score,
                actionability_score,
                confidence_score,
                sbu_fit_score,
                competitor_tagging,
                sbu_tagging,
                category_tag,
                summary,
                contract_value_inr_crore,
                geography,
                competitor_tier,
                rank_score,
                event_impact_score,
                source_domain,
                source_type,
                source_category,
                source_priority,
                source_authority_score,
                search_query_type,
                detected_client_authority,
                detected_strategic_theme,
                cluster_id,
                relationship_type,
                is_representative_article,
                cluster_title,
                cluster_summary,
                cluster_article_count,
                cluster_representative_article_id,
                cluster_source_confidence,
                cluster_rank_score,
                cluster_competitors,
                cluster_sbus,
                cluster_categories,
                cluster_primary_source,
                cluster_primary_source_type,
                cluster_primary_url
            FROM processed_articles
            WHERE {where_sql}
            ORDER BY
                event_impact_score DESC NULLS LAST,
                cluster_rank_score DESC NULLS LAST,
                rank_score DESC NULLS LAST,
                published_date DESC
            LIMIT {row_limit}
        """

        cur.execute(query, params)
        db_results = cur.fetchall()
        cur.close()
        conn.close()

        # ── Cluster-first grounding context + sources (Change 13) ─────────────
        db_context = ""
        db_sources = []
        try:
            # Score each representative row by event impact (highest first).
            scored_rows = []
            for row in (db_results or []):
                try:
                    scored_rows.append((safe_int(row.get("event_impact_score")) or 0, row))
                except Exception:
                    continue
            scored_rows.sort(key=lambda x: x[0], reverse=True)

            CONTEXT_CHAR_CAP = 6000
            context_parts = []
            running_len = 0
            seen_clusters = set()

            for impact, row in scored_rows:
                try:
                    block = _build_cluster_context_block(row)
                except Exception:
                    continue
                # Cap payload; truncate lower-impact events first (keep top event).
                if context_parts and running_len + len(block) > CONTEXT_CHAR_CAP:
                    continue
                context_parts.append(block)
                running_len += len(block)

                # Deduplicate sources by cluster; null cluster → its own cluster.
                cid = row.get("cluster_id")
                skey = f"cluster:{cid}" if cid is not None else f"article:{row.get('id')}"
                if skey in seen_clusters:
                    continue
                seen_clusters.add(skey)
                try:
                    db_sources.append(_build_chat_source(row, impact))
                except Exception:
                    continue

            if context_parts:
                db_context = (
                    "\n\nRELEVANT EVENT CLUSTERS FROM KEC'S INTERNAL DATABASE:\n\n"
                    + "\n".join(context_parts)
                )

            # Cluster-first sources: top 6 by event impact.
            db_sources.sort(key=lambda s: s.get("event_impact_score") or 0, reverse=True)
            db_sources = db_sources[:6]
        except Exception as e:
            print(f"[chat] grounding build failed: {e}")
            db_context = ""
            db_sources = []

        # ── Call Gemini (new SDK, with Google Search fallback) ─────────────────
        api_key = os.environ.get('GEMINI_API_KEY')
        if not api_key:
            raise HTTPException(status_code=500, detail="GEMINI_API_KEY not configured")

        client = genai.Client(api_key=api_key)

        system_prompt = f"""You are KEC Market Intelligence — a competitor intelligence assistant for KEC International, a leading EPC company in India.
You help the {sbu_profile} business unit track competitor activity.

Answer using the EVENT CLUSTERS provided below, not individual articles. Each cluster is one real-world business event, already deduplicated across sources.

Guidance:
- Prefer clusters with a higher Impact score and stronger source (higher authority / confidence).
- Always mention, when relevant: the competitor name, the client/authority, the project value (INR crore), and the geography.
- If a cluster's Confidence is Low, flag that briefly.
- Be concise and executive-ready — 3 to 5 sentences. Focus on business implications for KEC.
- Never invent or guess numbers, contract values, or client names that aren't in the provided clusters.
- Do NOT repeat the cluster IDs in your answer, and don't restate the same point from a single cluster twice.

If the grounded clusters don't contain the answer, say so briefly, then you may supplement from general knowledge or web search, tagging that part [AI]. Always try to give a useful answer.

At the very end of your response, on its own line:
- add [DB] if you used any grounded cluster
- add [AI] if you answered only from general knowledge
Do not repeat any content after that tag.

{db_context}"""

        contents = []
        for msg in req.conversation_history[-6:]:
            role = msg.get("role", "user")
            if role == "assistant":
                role = "model"
            contents.append(
                types.Content(role=role, parts=[types.Part(text=msg.get("content", ""))])
            )
        contents.append(
            types.Content(role="user", parts=[types.Part(text=req.message)])
        )

        used_search = True
        tools = [types.Tool(google_search=types.GoogleSearch())]

        config = types.GenerateContentConfig(
            system_instruction=system_prompt,
            tools=tools,
            temperature=0.3,
            max_output_tokens=1200,
        )

        models_to_try = ["gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-3.5-flash"]
        last_error = None
        for model_name in models_to_try:
            try:
                response = client.models.generate_content(
                    model=model_name,
                    contents=contents,
                    config=config,
                )
                break
            except Exception as e:
                last_error = e
                if '503' in str(e) or 'UNAVAILABLE' in str(e):
                    continue
                raise
        else:
            raise last_error
        sources = list(db_sources)
        try:
            candidate = response.candidates[0]
            if used_search and candidate.grounding_metadata and candidate.grounding_metadata.grounding_chunks:
                for chunk in candidate.grounding_metadata.grounding_chunks:
                    if chunk.web:
                        sources.append({
                            "title": chunk.web.title,
                            "link": chunk.web.uri,
                            "date": "",
                            "type": "web"
                        })
        except Exception:
            pass

        return {
            "status": "success",
            "answer": response.text,
            "sources": sources,
            "used_web_search": used_search
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/digest-preview")
def digest_preview(token: str = ""):
    """Get personalized email HTML for each user without sending"""
    try:
        if token:
            user = get_user_from_token(token)
            if not user or not user['is_admin']:
                raise HTTPException(status_code=403, detail="Admin access required")

        local_conn = get_local_db()
        local_cur = local_conn.cursor(cursor_factory=RealDictCursor)
        local_cur.execute("""
            SELECT id, name, email, sbu_profile, is_admin
            FROM users WHERE is_active = TRUE
        """)
        users = local_cur.fetchall()
        local_cur.close()
        local_conn.close()

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, news_title, category_tag, sbu_tagging,
                   summary, link, published_date, competitor_tagging,
                   contract_value_inr_crore, geography, rank_score, "Source"
            FROM processed_articles
            WHERE published_date >= CURRENT_DATE - INTERVAL '7 days'
            AND category_tag IS NOT NULL
            ORDER BY rank_score DESC NULLS LAST, published_date DESC
        """)
        raw_articles = cur.fetchall()
        cur.close()
        conn.close()

        all_articles = []
        for a in raw_articles:
            competitors = [
                c.strip() for c in (a.get('competitor_tagging') or '').split(',')
                if c.strip() and c.strip() != '-'
            ]
            all_articles.append({
                'id': a.get('id'),
                'title': a.get('news_title', ''),
                'category': a.get('category_tag', ''),
                'sbu_tagging': a.get('sbu_tagging', ''),
                'summary': a.get('summary', ''),
                'link': a.get('link', '#'),
                'date': a.get('published_date').isoformat() if a.get('published_date') else '',
                'source': a.get('Source', ''),
                'competitors': competitors,
                'contract_value': safe_float(a.get('contract_value_inr_crore')),
                'geography': a.get('geography'),
                'rank_score': a.get('rank_score') or 0,
            })

        SBU_ALIAS_MAP = {
            'intl t&d': ['intl t&d', 'international t&d'],
            'india t&d': ['india t&d'],
            'civil': ['civil'],
            'transportation': ['transportation'],
            'renewables': ['renewables'],
            'oil & gas': ['oil & gas'],
        }

        previews = []
        for u in users:
            sbu_profile = (u.get('sbu_profile') or '').strip()
            is_admin = u.get('is_admin', False)

            if is_admin or sbu_profile == 'Admin':
                sbus = ['Intl T&D', 'India T&D', 'Civil', 'Transportation', 'Renewables', 'Oil & Gas']
            else:
                sbus = [s.strip() for s in sbu_profile.split(',') if s.strip()]

            articles_by_sbu = {}
            for sbu in sbus:
                aliases = SBU_ALIAS_MAP.get(sbu.lower(), [sbu.lower()])
                sbu_articles = [
                    a for a in all_articles
                    if any(alias in (a.get('sbu_tagging') or '').lower() for alias in aliases)
                ]
                if sbu_articles:
                    articles_by_sbu[sbu] = sbu_articles

            if not articles_by_sbu:
                continue

            html = build_email_html(u['name'], articles_by_sbu)
            previews.append({
                "name": u['name'],
                "email": u['email'],
                "sbu_profile": sbu_profile,
                "subject": f"[KEC Intel] Weekly Competitor Digest — {sbu_profile}",
                "html": html
            })

        return {
            "status": "success",
            "total": len(previews),
            "previews": previews
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── COPILOT SEARCH ──────────────────────────────────────────────────────────

from typing import Optional

class CopilotRequest(BaseModel):
    question: Optional[str] = None

@app.post("/api/copilot-search")
async def copilot_search(request: Request):
    try:
        body = await request.body()
        question = ""
        try:
            import json
            parsed = json.loads(body)
            question = (
                parsed.get("question") or
                parsed.get("Question") or
                parsed.get("query") or
                parsed.get("text") or
                parsed.get("input") or
                parsed.get("message") or
                str(parsed) if parsed else ""
            )
        except:
            question = body.decode("utf-8", errors="ignore")

        if not question:
            return {"found": False, "message": "No question provided.", "articles": []}

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT news_title, summary, category_tag, sbu_tagging,
                   competitor_tagging, published_date, link,
                   contract_value_inr_crore, geography
            FROM processed_articles
            WHERE to_tsvector('english', 
                COALESCE(news_title,'') || ' ' || COALESCE(summary,''))
                @@ plainto_tsquery('english', %s)
            ORDER BY published_date DESC
            LIMIT 5
        """, (question,))

        results = cur.fetchall()
        cur.close()
        conn.close()

        if not results:
            return {"found": False, "message": "No relevant articles found.", "articles": []}

        articles = []
        for row in results:
            articles.append({
                "title": row.get("news_title", ""),
                "summary": row.get("summary", ""),
                "competitor": row.get("competitor_tagging", ""),
                "category": row.get("category_tag", ""),
                "sbu": row.get("sbu_tagging", ""),
                "date": row.get("published_date").isoformat() if row.get("published_date") else "",
                "link": row.get("link", ""),
                "contract_value_crore": safe_float(row.get("contract_value_inr_crore"))
            })

        return {"found": True, "count": len(articles), "articles": articles}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
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
