# NBA API Source Contract

## Provider
- Source: BALLDONTLIE NBA API
- Base URL: https://api.balldontlie.io/v1
- Authentication: API key in the Authorization header
- Plan: Free (5 requests/minute)

## Initial Scope
- 2023-24 season (2023)
- 2024-25 season (2024)
- 2025-26 season (2025)
- 2026-27 season (2026, ongoing)
- Regular season initially
- NBA Cup classification to be verified

## Source 1: Games
Endpoint: GET /games

Expected fields:
- id
- date
- datetime
- season
- status
- status_state
- postseason
- home_team
- visitor_team
- home_team_score
- visitor_team_score

Source grain:
One object per NBA game in the API response.

Silver target grain:
One row per unique game ID.

## Source 2: Teams
Endpoint: GET /teams

Expected fields:
- id
- conference
- division
- city
- name
- full_name
- abbreviation

Silver target grain:
One row per unique team ID.

## Ingestion Rules
- Preserve the original API response in Bronze.
- Record batch ID, ingestion timestamp, and endpoint.
- Support cursor-based pagination.
- Respect API rate limits.
- Keep API credentials outside Git.
- Allow previously ingested games to be refreshed.
- Verify actual fields and coverage before finalizing schemas.

## Data Quality Expectations
- Game IDs must be non-null and unique in Silver.
- Each game references valid team identifiers.
- Home and visitor teams must be different.
- Completed games must have valid scores.
- Incomplete games may have missing or provisional scores.

## Planned Bronze Tables
- nba_analytics.bronze.games_raw
- nba_analytics.bronze.teams_raw

## Access and Licensing
- Use official API endpoints within the free tier.
- Document applicable provider terms.
- Do not commit API keys or credentials.
- Keep raw API extracts out of Git initially.