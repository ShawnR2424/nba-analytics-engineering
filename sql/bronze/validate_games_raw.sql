
-- NBA Bronze Data Quality
-- Dataset: 2024-25 regular season

-- TEST 1: Raw response counts match metadata

WITH page_checks AS (
    SELECT
        records_returned,
        SIZE(
            FROM_JSON(
                payload,
                'STRUCT<data:ARRAY<STRUCT<id:BIGINT>>>'
            ).data
        ) AS parsed_game_count
    FROM nba_analytics.bronze.games_raw
    WHERE season = 2024
      AND season_type = 'regular'
      AND per_page = 50
)
SELECT
    COUNT(*) AS bronze_pages,
    SUM(records_returned) AS reported_games,
    SUM(COALESCE(parsed_game_count, 0)) AS parsed_games,
    SUM(
        CASE
            WHEN parsed_game_count IS NULL
              OR parsed_game_count <> records_returned
            THEN 1 ELSE 0
        END
    ) AS page_count_errors
FROM page_checks;


-- TEST 2: Check individual game ID uniqueness

WITH parsed AS (
    SELECT FROM_JSON(
        payload,
        'STRUCT<data:ARRAY<STRUCT<id:BIGINT>>>'
    ) AS body
    FROM nba_analytics.bronze.games_raw
    WHERE season = 2024
      AND season_type = 'regular'
      AND per_page = 50
),
games AS (
    SELECT game.id AS game_id
    FROM parsed p,
         LATERAL EXPLODE(p.body.data) AS e(game)
)
SELECT
    COUNT(*) AS game_records,
    COUNT(DISTINCT game_id) AS unique_game_ids,
    COUNT(*) - COUNT(DISTINCT game_id)
        AS duplicate_or_null_ids
FROM games;


-- TEST 3: Verify the pagination cursor chain

WITH ordered_pages AS (
    SELECT
        request_cursor,
        next_cursor,
        ROW_NUMBER() OVER (
            ORDER BY ingested_at, batch_id, request_cursor
        ) AS page_number,
        LAG(next_cursor) OVER (
            ORDER BY ingested_at, batch_id, request_cursor
        ) AS previous_next_cursor,
        COUNT(*) OVER () AS total_pages
    FROM nba_analytics.bronze.games_raw
    WHERE season = 2024
      AND season_type = 'regular'
      AND per_page = 50
)
SELECT
    COUNT(*) AS pages_checked,
    SUM(
        CASE
            WHEN page_number = 1
                 AND request_cursor IS NULL THEN 0
            WHEN page_number > 1
                 AND (request_cursor <=> previous_next_cursor)
                 THEN 0
            ELSE 1
        END
    ) AS cursor_chain_errors,
    MAX(
        CASE
            WHEN page_number = total_pages
                 AND next_cursor IS NULL
            THEN 1 ELSE 0
        END
    ) AS final_cursor_is_null
FROM ordered_pages;

