
-- Silver NBA Games
-- Grain: One row per unique game_id

CREATE TABLE IF NOT EXISTS nba_analytics.silver.games (
    game_id BIGINT,
    season INT,
    season_type STRING,
    game_date DATE,
    status STRING,
    home_team_id INT,
    visitor_team_id INT,
    home_team_score INT,
    visitor_team_score INT,
    source_ingested_at TIMESTAMP
)
USING DELTA
COMMENT 'Deduplicated NBA games with the latest ingested game snapshot';

