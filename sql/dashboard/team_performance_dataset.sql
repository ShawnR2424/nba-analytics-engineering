-- NBA Team Performance Dashboard
-- Grain: One team per regular season
-- Source: nba_analytics.gold.team_season

SELECT
    season,

    CONCAT(
        CAST(season AS STRING),
        '-',
        RIGHT(CAST(season + 1 AS STRING), 2)
    ) AS season_label,

    team_id,
    team_name,
    conference,

    games_played,
    wins,
    losses,
    home_wins,
    away_wins,

    ROUND(win_pct * 100, 1) AS win_pct_percent,

    points_per_game,
    points_allowed_per_game,
    point_differential,

    ROUND(
        point_differential / games_played,
        2
    ) AS avg_point_margin

FROM nba_analytics.gold.team_season

WHERE season IN (2023, 2024)
  AND season_type = 'regular'
