"""
Feature Delivery REST API Microservice (FastAPI)
Provides low-latency REST endpoints to deliver quarterly feature aggregations
to downstream machine learning training applications.
"""

import os
import logging
from typing import List, Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor

# Configure application logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("feature_api")

# Configuration

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "feature_store")
POSTGRES_USER = os.getenv("POSTGRES_USER", "db_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "db_password")

db_pool: Optional[ThreadedConnectionPool] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initializes database connection pool on startup and closes it on shutdown."""
    global db_pool
    logger.info("[STARTUP] Initializing PostgreSQL ThreadedConnectionPool (min=1, max=10)...")
    db_pool = ThreadedConnectionPool(
        minconn=1,
        maxconn=10,
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        cursor_factory=RealDictCursor
    )
    yield
    logger.info("[SHUTDOWN] Closing PostgreSQL ThreadedConnectionPool...")
    if db_pool:
        db_pool.closeall()

app = FastAPI(
    title="DLMDSEDE02 - Feature Store Delivery API",
    description="Delivers quarterly batch feature aggregations to ML model training applications.",
    version="1.0.0",
    lifespan=lifespan
)

class FeatureAggregationSchema(BaseModel):
    vendor_id: int
    year_quarter: str
    total_trips: int
    avg_trip_distance_miles: float
    avg_fare_amount: float
    avg_tip_amount: float
    total_revenue: float

def get_pooled_connection():
    if not db_pool:
        raise RuntimeError("Database connection pool is not initialized.")
    return db_pool.getconn()

def release_pooled_connection(conn):
    if db_pool and conn:
        db_pool.putconn(conn)

@app.get("/health", tags=["Health"])
def health_check():
    """Health check endpoint for Docker container probes using connection pool."""
    conn = None
    try:
        conn = get_pooled_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1;")
        return {"status": "HEALTHY", "database": "CONNECTED"}
    except Exception as e:
        logger.error(f"[HEALTH CHECK FAILED] Database ping error: {e}")
        raise HTTPException(status_code=500, detail="Service unhealthy: Database connection failure.")
    finally:
        if conn:
            release_pooled_connection(conn)

@app.get("/api/v1/features", response_model=List[FeatureAggregationSchema], tags=["Features"])
def get_quarterly_features(
    vendor_id: Optional[int] = Query(None, description="Filter features by vendor ID (e.g. 1, 2, 3)"),
    quarter: Optional[str] = Query(None, description="Filter features by quarter (e.g. '2023-Q1')"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of feature records to return"),
    offset: int = Query(0, ge=0, description="Offset for query result pagination")
):
    """
    Retrieves quarterly aggregated feature vectors for ML model training with pagination support.
    """
    conn = None
    try:
        conn = get_pooled_connection()
        cursor = conn.cursor()
        
        query = "SELECT vendor_id, year_quarter, total_trips, avg_trip_distance_miles, avg_fare_amount, avg_tip_amount, total_revenue FROM quarterly_feature_aggregations WHERE 1=1"
        params = []
        
        if vendor_id is not None:
            query += " AND vendor_id = %s"
            params.append(vendor_id)
            
        if quarter is not None:
            query += " AND year_quarter = %s"
            params.append(quarter)
            
        query += " ORDER BY year_quarter ASC, vendor_id ASC LIMIT %s OFFSET %s;"
        params.extend([limit, offset])
        
        cursor.execute(query, params)
        records = cursor.fetchall()
        cursor.close()
        
        return records
    except Exception as e:
        logger.error(f"[QUERY ERROR] Exception in get_quarterly_features: {e}")
        raise HTTPException(status_code=500, detail="Internal server error executing feature query.")
    finally:
        if conn:
            release_pooled_connection(conn)

@app.get("/api/v1/ingestion-logs", tags=["Audit"])
def get_ingestion_logs(
    limit: int = Query(10, ge=1, le=100, description="Maximum log records to return")
):
    """Returns batch ingestion audit logs with pagination limit."""
    conn = None
    try:
        conn = get_pooled_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT log_id, batch_id, records_ingested, s3_path, status, ingested_at FROM ingestion_logs ORDER BY ingested_at DESC LIMIT %s;", (limit,))
        logs = cursor.fetchall()
        cursor.close()
        return logs
    except Exception as e:
        logger.error(f"[AUDIT QUERY ERROR] Exception in get_ingestion_logs: {e}")
        raise HTTPException(status_code=500, detail="Internal server error executing audit query.")
    finally:
        if conn:
            release_pooled_connection(conn)
