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
    
    conn = psycopg.connect(database_url, row_factory=dict_row)
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
    Get all competitor data from the database
    This endpoint returns all records for the dashboard
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        query = """
            SELECT 
                id,
                keyword,
                newstitle,
                source,
                link,
                publishedate,
                sbu,
                competitor,
                matched_sbu,
                relevance_score,
                confidance_score,
                scraped_content,
                sbu_tagging,
                category_tag,
                kec_business_summary,
                created_at
            FROM competitor_data
            ORDER BY publishedate DESC
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
        cur = conn.cursor()
        
        # Total articles
        cur.execute("SELECT COUNT(*) as total FROM competitor_data")
        total = cur.fetchone()['total']
        
        # Unique SBUs
        cur.execute("SELECT COUNT(DISTINCT sbu) as count FROM competitor_data WHERE sbu IS NOT NULL")
        unique_sbus = cur.fetchone()['count']
        
        # Unique Competitors
        cur.execute("SELECT COUNT(DISTINCT competitor) as count FROM competitor_data WHERE competitor IS NOT NULL")
        unique_competitors = cur.fetchone()['count']
        
        # Recent articles (last 7 days)
        cur.execute("""
            SELECT COUNT(*) as count 
            FROM competitor_data 
            WHERE publishedate >= CURRENT_DATE - INTERVAL '7 days'
        """)
        recent = cur.fetchone()['count']
        
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "stats": {
                "total_articles": total,
                "unique_sbus": unique_sbus,
                "unique_competitors": unique_competitors,
                "recent_articles": recent
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
