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
        conn = get_db_connection()
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
