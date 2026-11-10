# NBA Analytics Pipeline Orchestration

## Platform

Databricks Lakeflow Jobs

## Job

NBA Analytics - Season Transformation Pipeline

## Execution graph

1. `build_silver_games`
   - Notebook: `notebooks/silver/01_build_games.py`
   - Reads Bronze game responses.
   - Parses, deduplicates, and validates game records.
   - Merges records into `nba_analytics.silver.games`.

2. `build_gold_team_game`
   - Depends on: `build_silver_games`
   - Notebook: `notebooks/gold/01_build_team_game.py`
   - Creates home and away team perspectives.
   - Validates game-level reconciliation.
   - Merges records into `nba_analytics.gold.team_game`.

3. `build_gold_team_season`
   - Depends on: `build_gold_team_game`
   - Notebook: `notebooks/gold/02_build_team_season.py`
   - Aggregates team-season performance.
   - Validates league and team totals.
   - Merges records into `nba_analytics.gold.team_season`.

## Configuration

- Job parameter: `season`
- Supported seasons: `2023`, `2024`
- Season type: `regular`
- Compute: Databricks serverless Spark
- Maximum concurrent runs: 1
- Trigger: Manual during initial validation
- Downstream tasks require upstream success.

## Prerequisites

- Bronze Games data for the selected season is complete.
- `nba_analytics.silver.teams` contains the 30 NBA teams.
- Existing Unity Catalog tables and permissions are available.

## Expected outputs per season

| Table | Expected records |
|---|---:|
| Silver Games | 1,230 |
| Gold Team-Game | 2,460 |
| Gold Team-Season | 30 |

## Current limitations

- Bronze API ingestion still runs separately on the local machine.
- The job processes completed regular seasons.
- Silver Teams is maintained separately.
- Recurring scheduling and dashboard refresh are not yet part of this job.
- The actual Databricks job definition is not yet managed as code.
