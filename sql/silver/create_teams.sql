
-- NBA Analytics Engineering
-- Silver: Current NBA Teams
-- Grain: One row per team_id

CREATE TABLE IF NOT EXISTS nba_analytics.silver.teams (

    team_id INT NOT NULL,
    full_name STRING,
    abbreviation STRING,
    city STRING,
    name STRING,
    conference STRING,
    division STRING,
    source_ingested_at TIMESTAMP

)
USING DELTA
COMMENT 'Current NBA teams standardized from BALLDONTLIE';

