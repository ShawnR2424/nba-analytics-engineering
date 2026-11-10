# NBA Dashboard Refresh Orchestration

## Objective

Refresh the published NBA Team Performance Intelligence
dashboard after all required data transformations succeed.

## Platform

Databricks Lakeflow Jobs and AI/BI Dashboards.

## Job

NBA Analytics - Season Transformation Pipeline

## Task Dependencies

1. `build_silver_games`
   - Reads Bronze NBA game responses.
   - Creates validated Silver game records.

2. `build_gold_team_game`
   - Depends on: `build_silver_games`
   - Creates validated team-game fact records.

3. `build_gold_team_season`
   - Depends on: `build_gold_team_game`
   - Aggregates season-level team KPIs.

4. `refresh_nba_dashboard`
   - Type: Dashboard
   - Depends on: `build_gold_team_season`
   - Run condition: All succeeded
   - Refreshes the published dashboard results.
   - Uses a supported Databricks SQL warehouse.

## Dashboard

NBA Team Performance Intelligence

### Pages

1. Single-Season Team Performance
2. Year-over-Year Performance

### Data Sources

- `nba_analytics.gold.team_season`
- `sql/dashboard/team_performance_dataset.sql`
- `sql/dashboard/team_yoy_dataset.sql`

## Parameters

The job supports completed NBA regular seasons:

- 2023
- 2024

The selected season controls the transformation notebooks.

The dashboard datasets report on both seasons.

## Validation

Expected per season:

- 1,230 Silver games
- 2,460 Gold team-game records
- 30 Gold team-season records

The dashboard refresh task must execute after the
successful Team-Season task.

## Current Limitations

- Bronze API ingestion remains a separate local process.
- Silver Teams is maintained separately.
- The workflow has no recurring schedule.
- Dashboard sharing is controlled through Databricks permissions.
- The job configuration is not yet managed as code.
