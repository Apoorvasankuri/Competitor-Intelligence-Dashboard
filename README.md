# Competitor Intelligence API Backend

This is the FastAPI backend that serves data from PostgreSQL to your frontend dashboard.

## Files Included:
- `main.py` - FastAPI application with endpoints
- `requirements.txt` - Python dependencies
- `render.yaml` - Render deployment configuration (optional)

## API Endpoints:

### 1. GET `/`
Health check endpoint
Returns: API status and timestamp

### 2. GET `/api/data`
Get all competitor data from database
Returns: JSON with all records

### 3. GET `/api/stats`
Get dashboard statistics
Returns: Total articles, unique SBUs, competitors, recent articles

### 4. GET `/api/health`
Database connectivity check
Returns: Database connection status

## Environment Variables Needed:
- `DATABASE_URL` - Your PostgreSQL connection string from Render

## Local Testing (Optional):
```bash
pip install -r requirements.txt
export DATABASE_URL="your_postgres_url_here"
uvicorn main:app --reload
```

Visit: http://localhost:8000/docs for interactive API documentation

## Deployment Instructions:
See the step-by-step guide to deploy this to Render.
