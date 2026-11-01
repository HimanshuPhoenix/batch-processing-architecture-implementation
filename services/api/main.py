"""
Feature Delivery REST API Microservice (FastAPI)
Provides low-latency REST endpoints to deliver quarterly feature aggregations
to downstream machine learning training applications.
"""

import os
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(
    title="Feature Store Delivery API",
    description="Delivers quarterly batch feature aggregations to ML model training applications.",
    version="1.0.0"
)

# Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "feature_store")
POSTGRES_USER = os.getenv("POSTGRES_USER", "db_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "db_password")

class FeatureAggregationSchema(BaseModel):
    vendor_id: int
    year_quarter: str
    total_trips: int
    avg_trip_distance_miles: float
    avg_fare_amount: float
    avg_tip_amount: float
    total_revenue: float

def get_db_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        cursor_factory=RealDictCursor
    )

@app.get("/health", tags=["Health"])
def health_check():
    """Health check endpoint for Docker container probes."""
    try:
        conn = get_db_connection()
        conn.close()
        return {"status": "HEALTHY", "database": "CONNECTED"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

@app.get("/api/v1/features", response_model=List[FeatureAggregationSchema], tags=["Features"])
def get_quarterly_features(
    vendor_id: Optional[int] = Query(None, description="Filter features by vendor ID (e.g. 1, 2, 3)"),
    quarter: Optional[str] = Query(None, description="Filter features by quarter (e.g. '2023-Q1')")
):
    """
    Retrieves quarterly aggregated feature vectors for ML model training.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = "SELECT vendor_id, year_quarter, total_trips, avg_trip_distance_miles, avg_fare_amount, avg_tip_amount, total_revenue FROM quarterly_feature_aggregations WHERE 1=1"
        params = []
        
        if vendor_id is not None:
            query += " AND vendor_id = %s"
            params.append(vendor_id)
            
        if quarter is not None:
            query += " AND year_quarter = %s"
            params.append(quarter)
            
        query += " ORDER BY year_quarter ASC, vendor_id ASC;"
        
        cursor.execute(query, params)
        records = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return records
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution error: {str(e)}")

@app.get("/api/v1/ingestion-logs", tags=["Audit"])
def get_ingestion_logs():
    """Returns batch ingestion audit logs."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT log_id, batch_id, records_ingested, s3_path, status, ingested_at FROM ingestion_logs ORDER BY ingested_at DESC LIMIT 10;")
        logs = cursor.fetchall()
        cursor.close()
        conn.close()
        return logs
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Audit query error: {str(e)}")
