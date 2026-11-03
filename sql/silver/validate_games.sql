
-- NBA Silver Data Quality
-- Target: 2024-25 regular season

WITH games AS (
    SELECT *
    FROM nba_analytics.silver.games
    WHERE season = 2024
      AND season_type = 'regular'
),

checks AS (
    SELECT
        COUNT(*) AS total_games,

        COUNT(DISTINCT game_id) AS unique_games,

        COALESCE(COUNT_IF(
            game_id IS NULL
            OR game_date IS NULL
            OR status IS NULL
            OR home_team_id IS NULL
            OR visitor_team_id IS NULL
            OR source_ingested_at IS NULL
        ), 0) AS missing_required_fields,

        COALESCE(COUNT_IF(
            home_team_id = visitor_team_id
        ), 0) AS same_team_errors,

        COALESCE(COUNT_IF(
            status LIKE 'Final%'
            AND (
                home_team_score IS NULL
                OR visitor_team_score IS NULL
                OR home_team_score < 0
                OR visitor_team_score < 0
                OR home_team_score = visitor_team_score
            )
        ), 0) AS invalid_final_scores,

        COALESCE(COUNT_IF(
            game_date IS NOT NULL
            AND YEAR(game_date) NOT IN (2024, 2025)
        ), 0) AS invalid_season_dates

    FROM games
)

SELECT
    *,
    CASE
        WHEN total_games = 1230
         AND unique_games = total_games
         AND missing_required_fields = 0
         AND same_team_errors = 0
         AND invalid_final_scores = 0
         AND invalid_season_dates = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS quality_status

FROM checks;

