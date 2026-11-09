-- NBA Year-over-Year Team Performance
-- Grain: One row per team_id
-- Comparison: 2023-24 vs 2024-25 regular seasons

WITH previous_season AS (
    SELECT *
    FROM nba_analytics.gold.team_season
    WHERE season = 2023
      AND season_type = 'regular'
),

current_season AS (
    SELECT *
    FROM nba_analytics.gold.team_season
    WHERE season = 2024
      AND season_type = 'regular'
)

SELECT
    curr.team_id,
    curr.team_name,
    curr.conference,

    '2023-24' AS previous_season_label,
    '2024-25' AS current_season_label,

    prev.wins AS wins_2023_24,
    curr.wins AS wins_2024_25,
    curr.wins - prev.wins AS wins_change,

    prev.losses AS losses_2023_24,
    curr.losses AS losses_2024_25,

    ROUND(prev.win_pct * 100, 1)
        AS win_pct_2023_24,

    ROUND(curr.win_pct * 100, 1)
        AS win_pct_2024_25,

    ROUND(
        (curr.win_pct - prev.win_pct) * 100,
        1
    ) AS win_pct_change_pp,

    prev.points_per_game AS ppg_2023_24,
    curr.points_per_game AS ppg_2024_25,

    ROUND(
        curr.points_per_game - prev.points_per_game,
        2
    ) AS ppg_change,

    ROUND(
        curr.points_allowed_per_game
        - prev.points_allowed_per_game,
        2
    ) AS points_allowed_change,

    curr.point_differential
        - prev.point_differential
        AS point_diff_change,

    ROUND(
        curr.point_differential * 1.0
            / curr.games_played
        - prev.point_differential * 1.0
            / prev.games_played,
        2
    ) AS avg_margin_change,

    CASE
        WHEN curr.wins > prev.wins
            THEN 'More wins'
        WHEN curr.wins < prev.wins
            THEN 'Fewer wins'
        ELSE 'Unchanged'
    END AS wins_direction

FROM current_season curr

INNER JOIN previous_season prev
    ON curr.team_id = prev.team_id
