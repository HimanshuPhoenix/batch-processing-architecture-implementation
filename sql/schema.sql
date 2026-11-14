-- ==============================================================================
-- PostgreSQL Schema Definition
-- Database: feature_store
-- ==============================================================================

-- 1. Table for Ingestion Metadata & Audit Logs
CREATE TABLE IF NOT EXISTS ingestion_logs (
    log_id SERIAL PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL,
    records_ingested INT NOT NULL,
    s3_path VARCHAR(255) NOT NULL,
    file_format VARCHAR(20) DEFAULT 'parquet',
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'SUCCESS'
);

-- 2. Table for Quarterly Aggregated Features (Batch Layer - Delivered to ML Model)
CREATE TABLE IF NOT EXISTS quarterly_feature_aggregations (
    aggregation_id SERIAL PRIMARY KEY,
    vendor_id INT NOT NULL,
    year_quarter VARCHAR(10) NOT NULL, -- e.g. '2023-Q1'
    total_trips BIGINT NOT NULL,
    avg_trip_distance_miles NUMERIC(10, 2) NOT NULL,
    avg_fare_amount NUMERIC(10, 2) NOT NULL,
    avg_tip_amount NUMERIC(10, 2) NOT NULL,
    total_revenue NUMERIC(14, 2) NOT NULL,
    computed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_vendor_quarter UNIQUE (vendor_id, year_quarter)
);

-- 3. Table for Real-Time Sliding Window Features (Speed Layer - Streaming Aggregations)
CREATE TABLE IF NOT EXISTS realtime_feature_aggregations (
    id SERIAL PRIMARY KEY,
    vendor_id INT NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    live_trips INT NOT NULL,
    live_avg_fare NUMERIC(10, 2),
    live_avg_distance NUMERIC(10, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_vendor_window UNIQUE (vendor_id, window_start, window_end)
);

-- 4. Indexes for Query Performance & Low-Latency API Lookups
CREATE INDEX IF NOT EXISTS idx_quarterly_features_vendor ON quarterly_feature_aggregations(vendor_id);
CREATE INDEX IF NOT EXISTS idx_quarterly_features_quarter ON quarterly_feature_aggregations(year_quarter);
CREATE INDEX IF NOT EXISTS idx_ingestion_logs_batch ON ingestion_logs(batch_id);
CREATE INDEX IF NOT EXISTS idx_realtime_vendor_window ON realtime_feature_aggregations (vendor_id, window_end DESC);
