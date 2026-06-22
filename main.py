from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
import csv
import io
from fastapi.middleware.cors import CORSMiddleware
import psycopg
from psycopg.rows import dict_row
import os
from datetime import datetime, date
from decimal import Decimal
import math
from google import genai
from google.genai import types
from fastapi import FastAPI, HTTPException, Request


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


@app.get("/")
def read_root():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "message": "Competitor Intelligence API is running",
        "timestamp": datetime.now().isoformat()
    }


@app.get("/api/data")
def get_all_data():
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
                processed_at
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
        clean_results = []
        for row in raw_results:
            clean_row = {
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
                'created_at': row['processed_at'].isoformat() if row.get('processed_at') else None
            }
            clean_results.append(clean_row)
        
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
                geography, competitor_tier, rank_score
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
                'link': a.get('link', '#'),
                'date': a.get('published_date').isoformat() if a.get('published_date') else '',
                'source': a.get('Source', ''),
                'competitors': competitors,
                'contract_value': safe_float(a.get('contract_value_inr_crore')),
                'geography': a.get('geography'),
                'rank_score': a.get('rank_score') or 0,
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

            try:
                html = build_email_html(u['name'], articles_by_sbu)
                to_email = os.environ.get('TEST_EMAIL', u['email']) \
                    if os.environ.get('TEST_MODE') == 'true' else u['email']

                resend.Emails.send({
                    "from": from_email,
                    "to": [to_email],
                    "subject": f"[KEC Intel] Weekly Competitor Digest — {sbu_profile}",
                    "html": html,
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

        # Admin sees everything, SBU users see only their SBU
        if user['is_admin']:
            query = """
                SELECT id, published_date, news_title, link, "Source",
                    relevance_score, competitor_tagging, sbu_tagging,
                    category_tag, summary, contract_value_inr_crore,
                    geography, competitor_tier, rank_score, processed_at
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
            query = f"""
                SELECT id, published_date, news_title, link, "Source",
                    relevance_score, competitor_tagging, sbu_tagging,
                    category_tag, summary, contract_value_inr_crore,
                    geography, competitor_tier, rank_score, processed_at
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

        clean_results = []
        for row in raw_results:
            clean_row = {
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
                'contract_value_inr_crore': safe_float(row.get('contract_value_inr_crore')),
                'geography': str(row['geography']) if row.get('geography') and str(row['geography']) != 'None' else None,
                'rank_score': safe_int(row.get('rank_score')) or 0,
            }
            clean_results.append(clean_row)

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
            where_clauses.append("published_date >= CURRENT_DATE - INTERVAL '%s days'")
            params.append(search_days)

        # Full-text keyword search (if we have meaningful keywords beyond category)
        if search_keywords and search_keywords != search_category:
            where_clauses.append("""
                to_tsvector('english', COALESCE(news_title,'') || ' ' || COALESCE(summary,''))
                @@ plainto_tsquery('english', %s)
            """)
            params.append(search_keywords)

        # Combine — if no filters at all, fall back to recent articles
        if where_clauses:
            where_sql = " AND ".join(where_clauses)
        else:
            where_sql = "published_date >= CURRENT_DATE - INTERVAL '7 days'"

        query = f"""
            SELECT news_title, summary, category_tag, sbu_tagging,
                   competitor_tagging, published_date, link, geography,
                   contract_value_inr_crore
            FROM processed_articles
            WHERE {where_sql}
            ORDER BY rank_score DESC NULLS LAST, published_date DESC
            LIMIT 8
        """

        cur.execute(query, params)
        db_results = cur.fetchall()
        cur.close()
        conn.close()

        # Format DB results
        db_context = ""
        db_sources = []
        if db_results:
            db_context = "\n\nRELEVANT ARTICLES FROM KEC'S INTERNAL DATABASE:\n"
            for i, row in enumerate(db_results):
                title = row.get('news_title', '')
                summary = row.get('summary', '')
                date = row.get('published_date', '')
                link = row.get('link', '')
                category = row.get('category_tag', '')
                competitor = row.get('competitor_tagging', '')
                date_str = date.isoformat() if hasattr(date, 'isoformat') else str(date)
                db_context += f"\n[DB Article {i+1}]\nTitle: {title}\nSummary: {summary}\nCategory: {category}\nCompetitor: {competitor}\nDate: {date_str}\nLink: {link}\n"
                db_sources.append({"title": title, "link": link, "date": date_str, "type": "database"})

        # ── Call Gemini (new SDK, with Google Search fallback) ─────────────────
        api_key = os.environ.get('GEMINI_API_KEY')
        if not api_key:
            raise HTTPException(status_code=500, detail="GEMINI_API_KEY not configured")

        client = genai.Client(api_key=api_key)

        system_prompt = f"""You are KEC Market Intelligence — a competitor intelligence assistant for KEC International, a leading EPC company in India.
You help the {sbu_profile} business unit track competitor activity.

When the user asks about competitors, order wins, bidding activity, market trends, contracts, or any competitor news, use the database results provided below to answer.

Always mention, when the information is available:
- The competitor name
- The contract value (if available)
- The date of the news
- Cite it as [DB]

If no relevant results were found in the database, say so honestly, then answer from your general knowledge, citing it as [AI].

Be concise — 3 to 5 sentences. Focus on business implications for KEC.
Never invent or guess at specific numbers or contract values that aren't in the source material.

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

        used_search = False
        tools = None
        if not db_results:
            tools = [types.Tool(google_search=types.GoogleSearch())]
            used_search = True

        config = types.GenerateContentConfig(
            system_instruction=system_prompt,
            tools=tools,
            temperature=0.3,
            max_output_tokens=1200,
        )

        models_to_try = ["gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-3-flash", "gemini-3.5-flash"]
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