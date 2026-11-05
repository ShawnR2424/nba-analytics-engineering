
-- Gold NBA Team-Game Fact Table
-- Grain: One row per (game_id, team_id)

CREATE TABLE IF NOT EXISTS nba_analytics.gold.team_game (

    game_id BIGINT NOT NULL,
    team_id INT NOT NULL,
    opponent_team_id INT,

    season INT,
    season_type STRING,
    game_date DATE,
    is_home BOOLEAN,

    points_for INT,
    points_against INT,
    win INT,
    loss INT,
    point_diff INT,

    source_ingested_at TIMESTAMP

)
USING DELTA
COMMENT 'One team-perspective record per completed NBA game';

