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
        conn = get_db_connection()  if os.environ.get('DATABASE_URL') else get_local_db()
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

def build_email_html(recipient_name: str, articles: list) -> str:
    """Build a clean HTML email for a recipient"""
    
    grouped = {}
    for article in articles:
        cat = article.get('category', 'General').title()
        sbu = article.get('sbu', '')
        key = f"{cat} — {sbu}"
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(article)
    
    html = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 700px; margin: 0 auto; background: #f9f8f3;">
        <div style="background: linear-gradient(135deg, #0F2B4C, #1A3D6D); padding: 32px; text-align: center;">
            <h1 style="color: white; font-size: 24px; margin: 0;">Competitor Intelligence</h1>
            <p style="color: #C9A84C; margin: 8px 0 0 0; font-size: 13px; letter-spacing: 2px; text-transform: uppercase;">Weekly Action Digest</p>
        </div>
        <div style="padding: 32px; background: white;">
            <p style="color: #333; font-size: 15px;">Hi <strong>{recipient_name}</strong>,</p>
            <p style="color: #666; font-size: 14px; line-height: 1.6;">Here are this week's competitor intelligence items that require your attention. Please review and forward to relevant team members as needed.</p>
    """
    
    for group_key, group_articles in grouped.items():
        html += f"""
            <div style="margin: 24px 0;">
                <div style="background: #0F2B4C; color: white; padding: 10px 16px; border-radius: 6px 6px 0 0; font-size: 12px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase;">{group_key}</div>
                <div style="border: 1px solid #E5E2D0; border-top: none; border-radius: 0 0 6px 6px;">
        """
        for i, article in enumerate(group_articles):
            border = "border-bottom: 1px solid #E5E2D0;" if i < len(group_articles) - 1 else ""
            html += f"""
                    <div style="padding: 16px; {border}">
                        <a href="{article.get('link', '#')}" style="color: #0F2B4C; font-weight: 600; font-size: 14px; text-decoration: none; line-height: 1.4;">{article.get('title', '')}</a>
                        <p style="color: #666; font-size: 13px; margin: 8px 0 0 0; line-height: 1.6;">{article.get('summary', '')[:300]}...</p>
                        <p style="color: #999; font-size: 11px; margin: 6px 0 0 0;">{article.get('date', '')}</p>
                    </div>
            """
        html += "</div></div>"
    
    html += f"""
            <div style="margin-top: 32px; padding: 16px; background: #f9f8f3; border-radius: 6px; border-left: 4px solid #C9A84C;">
                <p style="color: #666; font-size: 13px; margin: 0;">To assign any item to a team member, simply <strong>forward this email</strong> to them with your instructions.</p>
            </div>
        </div>
        <div style="background: #0F2B4C; padding: 16px; text-align: center;">
            <p style="color: rgba(255,255,255,0.5); font-size: 12px; margin: 0;">KEC Competitor Intelligence Platform · Weekly Digest</p>
        </div>
    </div>
    """
    return html


@app.post("/api/send-digest")
def send_weekly_digest():
    """Send weekly digest emails to all recipients based on routing matrix"""
    try:
        resend.api_key = os.environ.get('RESEND_API_KEY')
        from_email = os.environ.get('RESEND_FROM', 'onboarding@resend.dev')

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, news_title, category_tag, sbu_tagging, summary, link, published_date
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
                    "summary": article.get('summary', ''),
                    "link": article.get('link', '#'),
                    "date": article.get('published_date', '').isoformat() if article.get('published_date') else '',
                })

        sent = []
        failed = []

        for email, data in recipient_map.items():
            try:
                html = build_email_html(data['name'], data['articles'])
                resend.Emails.send({
                    "from": from_email,
                    "to": [os.environ.get('TEST_EMAIL', email)] if os.environ.get('TEST_MODE') == 'true' else [email],
                    "subject": f"[KEC Intel] Your Weekly Competitor Digest — {len(data['articles'])} items",
                    "html": html,
                })
                sent.append(email)

                local_conn = get_local_db()
                local_cur = local_conn.cursor()
                local_cur.execute("""
                    INSERT INTO email_log (recipient_email, email_type, subject, status)
                    VALUES (%s, %s, %s, %s)
                """, (email, 'weekly_digest', f"Weekly Digest — {len(data['articles'])} items", 'sent'))
                local_conn.commit()
                local_cur.close()
                local_conn.close()

            except Exception as e:
                failed.append({"email": email, "error": str(e)})

        return {
            "status": "success",
            "sent": len(sent),
            "failed": len(failed),
            "sent_to": sent,
            "failures": failed
        }

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
        if req.sbu_profile not in SBU_PROFILES:
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
            query = """
                SELECT id, published_date, news_title, link, "Source",
                    relevance_score, competitor_tagging, sbu_tagging,
                    category_tag, summary, contract_value_inr_crore,
                    geography, competitor_tier, rank_score, processed_at
                FROM processed_articles
                WHERE sbu_tagging ILIKE %s
                ORDER BY 
                    CASE WHEN rank_score IS NULL THEN 1 ELSE 0 END,
                    rank_score DESC, published_date DESC
            """
            cur.execute(query, (f"%{user['sbu_profile']}%",))

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
    
    # ─── CHATBOT ──────────────────────────────────────────────────────────────────

import google.generativeai as genai

class ChatRequest(PydanticBase):
    message: str
    token: str
    conversation_history: list = []

@app.post("/api/chat")
def chat(req: ChatRequest):
    """Chatbot endpoint — searches DB, web, and uses Gemini for answers"""
    try:
        # ── Auth ──────────────────────────────────────────────────────────────
        user = get_user_from_token(req.token)
        if not user:
            raise HTTPException(status_code=401, detail="Invalid or expired session")

        sbu_profile = user['sbu_profile']
        is_admin = user['is_admin']

        # ── Step 1: Search database ───────────────────────────────────────────
        conn = get_db_connection()
        cur = conn.cursor()

        search_terms = req.message.lower().split()
        search_query = ' | '.join(search_terms)

        if is_admin:
            cur.execute("""
                SELECT news_title, summary, category_tag, sbu_tagging,
                       competitor_tagging, published_date, link, geography,
                       contract_value_inr_crore
                FROM processed_articles
                WHERE to_tsvector('english', COALESCE(news_title,'') || ' ' || COALESCE(summary,''))
                      @@ plainto_tsquery('english', %s)
                ORDER BY published_date DESC
                LIMIT 8
            """, (req.message,))
        else:
            cur.execute("""
                SELECT news_title, summary, category_tag, sbu_tagging,
                       competitor_tagging, published_date, link, geography,
                       contract_value_inr_crore
                FROM processed_articles
                WHERE sbu_tagging ILIKE %s
                AND to_tsvector('english', COALESCE(news_title,'') || ' ' || COALESCE(summary,''))
                    @@ plainto_tsquery('english', %s)
                ORDER BY published_date DESC
                LIMIT 8
            """, (f'%{sbu_profile}%', req.message))

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

        # ── Step 2: Web search via Gemini grounding ───────────────────────────
        web_context = ""
        web_sources = []
        try:
            genai.configure(api_key=os.environ.get('GEMINI_API_KEY'))
            search_model = genai.GenerativeModel('gemini-1.5-flash')
            web_search_prompt = f"Search for recent news about: {req.message} in the context of Indian infrastructure, construction, power transmission, railways, or renewables industry. Provide 3-4 key facts with sources."
            web_response = search_model.generate_content(
                web_search_prompt,
                tools=[{"google_search": {}}] if hasattr(genai, 'protos') else []
            )
            if web_response.text:
                web_context = f"\n\nWEB SEARCH RESULTS:\n{web_response.text[:1500]}"
                web_sources.append({"title": "Web Search Results", "link": "", "type": "web"})
        except Exception:
            web_context = ""

        # ── Step 3: Build prompt and call Gemini ──────────────────────────────
        genai.configure(api_key=os.environ.get('GEMINI_API_KEY'))
        model = genai.GenerativeModel('gemini-1.5-flash')

        system_prompt = f"""You are a competitor intelligence assistant for KEC International, a leading EPC company in India.
You help the {sbu_profile} business unit track competitor activity.

INSTRUCTIONS:
- Answer questions about competitor activity, market trends, order wins, bidding, partnerships, M&A
- Always cite your sources using [DB], [WEB], or [AI] tags
- [DB] = from KEC's internal news database
- [WEB] = from web search
- [AI] = from your general knowledge
- Be concise — 3-5 sentences unless asked for detail
- Focus on business implications for KEC
- If information is from the database, mention the competitor name and date
- Never make up specific numbers or contract values

{db_context}
{web_context}

USER QUESTION: {req.message}

Provide a helpful, concise answer with source citations."""

        # Build conversation history
        history = []
        for msg in req.conversation_history[-6:]:
            history.append({
                "role": msg.get("role", "user"),
                "parts": [msg.get("content", "")]
            })

        if history:
            chat_session = model.start_chat(history=history)
            response = chat_session.send_message(system_prompt)
        else:
            response = model.generate_content(system_prompt)

        answer = response.text

        # ── Step 4: Return response with sources ──────────────────────────────
        all_sources = db_sources + web_sources
        has_db = len(db_sources) > 0
        has_web = len(web_sources) > 0

        return {
            "status": "success",
            "answer": answer,
            "sources": all_sources,
            "source_summary": {
                "database": len(db_sources),
                "web": has_web,
                "ai_knowledge": True
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
