
-- Gold Team-Season Performance Mart
-- Grain: (season, season_type, team_id)

CREATE TABLE IF NOT EXISTS nba_analytics.gold.team_season (
    season INT,
    season_type STRING,
    team_id INT,
    team_name STRING,
    conference STRING,

    games_played INT,
    wins INT,
    losses INT,
    home_wins INT,
    away_wins INT,

    win_pct DOUBLE,

    total_points_for BIGINT,
    total_points_against BIGINT,
    point_differential BIGINT,

    points_per_game DOUBLE,
    points_allowed_per_game DOUBLE,

    source_ingested_at TIMESTAMP
)
USING DELTA
COMMENT 'Aggregated NBA team-season performance metrics';

