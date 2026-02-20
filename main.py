from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg
from psycopg.rows import dict_row
import os
from datetime import datetime
from typing import List, Dict, Any

app = FastAPI(title="Competitor Intelligence API")

# CORS middleware to allow your frontend to access this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with your Vercel domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection function
def get_db_connection():
    """Create and return a database connection"""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        raise Exception("DATABASE_URL environment variable not set")
    
    # CHANGED: psycopg2.connect → psycopg.connect
    # CHANGED: cursor_factory → row_factory
    conn = psycopg.connect(database_url)
    return conn

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
    """
    Get all processed competitor data from the database
    This endpoint returns all processed records for the dashboard
    """
    try:
        conn = get_db_connection()
        # CHANGED: Add row_factory=dict_row here
        cur = conn.cursor(row_factory=dict_row)
        
        query = """
            SELECT 
                id,
                published_date AS publishedate,
                news_title AS newstitle,
                link,
                "Source",
                relevance_score,
                competitor_tagging,
                sbu_tagging AS sbu,
                category_tag,
                summary AS kec_business_summary,
                scraped_content,
                contract_value_inr_crore,
                geography,
                competitor_tier,
                rank_score,
                processed_at AS created_at
            FROM processed_articles
            ORDER BY rank_score DESC, published_date DESC
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        cur.close()
        conn.close()
        
        # Convert date objects to strings for JSON serialization
        for row in results:
            if row['publishedate']:
                row['publishedate'] = row['publishedate'].isoformat()
            if row['created_at']:
                row['created_at'] = row['created_at'].isoformat()
        
        return {
            "status": "success",
            "count": len(results),
            "data": results
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/api/stats")
def get_statistics():
    """
    Get dashboard statistics
    Returns summary stats for the overview page
    """
    try:
        conn = get_db_connection()
        # CHANGED: Add row_factory=dict_row here
        cur = conn.cursor(row_factory=dict_row)
        
        # Total articles
        cur.execute("SELECT COUNT(*) as total FROM processed_articles")
        total = cur.fetchone()['total']
        
        # Unique SBUs
        cur.execute("SELECT COUNT(DISTINCT sbu_tagging) as count FROM processed_articles WHERE sbu_tagging IS NOT NULL")
        unique_sbus = cur.fetchone()['count']
        
        # Unique Competitors
        cur.execute("SELECT COUNT(DISTINCT competitor_tagging) as count FROM processed_articles WHERE competitor_tagging IS NOT NULL AND competitor_tagging != '-'")
        unique_competitors = cur.fetchone()['count']
        
        # Recent articles (last 7 days)
        cur.execute("""
            SELECT COUNT(*) as count 
            FROM processed_articles 
            WHERE published_date >= CURRENT_DATE - INTERVAL '7 days'
        """)
        recent = cur.fetchone()['count']
        
        # High relevance articles (score >= 70)
        cur.execute("""
            SELECT COUNT(*) as count 
            FROM processed_articles 
            WHERE relevance_score >= 70
        """)
        high_relevance = cur.fetchone()['count']
        
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "stats": {
                "total_articles": total,
                "unique_sbus": unique_sbus,
                "unique_competitors": unique_competitors,
                "recent_articles": recent,
                "high_relevance_articles": high_relevance
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/api/health")
def health_check():
    """
    Health check with database connectivity test
    """
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
    """
    Get count of unprocessed articles in raw_scraped_articles
    Useful for monitoring the pipeline
    """
    try:
        conn = get_db_connection()
        # CHANGED: Add row_factory=dict_row here
        cur = conn.cursor(row_factory=dict_row)
        
        cur.execute("SELECT COUNT(*) as count FROM raw_scraped_articles")
        count = cur.fetchone()['count']
        
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "unprocessed_articles": count
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
