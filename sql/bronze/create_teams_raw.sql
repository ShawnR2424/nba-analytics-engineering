
-- NBA Analytics Engineering
-- Bronze: Raw Teams API responses
-- Grain: One row per distinct source response

CREATE TABLE IF NOT EXISTS nba_analytics.bronze.teams_raw (

    batch_id STRING,
    ingested_at TIMESTAMP,
    source_endpoint STRING,
    records_returned INT,
    payload STRING

)
USING DELTA
COMMENT 'Original BALLDONTLIE Teams API response snapshots';

