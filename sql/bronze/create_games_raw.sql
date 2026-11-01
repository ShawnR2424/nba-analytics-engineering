
-- NBA Analytics Engineering
-- Bronze: Raw Games API responses
-- Grain: One row per successful API page response

CREATE TABLE IF NOT EXISTS nba_analytics.bronze.games_raw (

    batch_id STRING,
    ingested_at TIMESTAMP,
    source_endpoint STRING,

    season INT,
    season_type STRING,

    request_cursor STRING,
    next_cursor STRING,
    per_page INT,

    records_returned INT,
    payload STRING

)
USING DELTA
COMMENT 'Raw paginated NBA Games API responses from BALLDONTLIE';

