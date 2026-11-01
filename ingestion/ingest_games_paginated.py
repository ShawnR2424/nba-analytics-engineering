
import os
import time
import uuid
from pathlib import Path

import requests
from databricks import sql
from dotenv import load_dotenv


# -----------------------------------------
# 1. Configuration
# -----------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"

if not ENV_PATH.is_file():
    raise FileNotFoundError(f"Missing .env file: {ENV_PATH}")

load_dotenv(ENV_PATH)

API_KEY = os.getenv("BALLDONTLIE_API_KEY")
SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

if not all([API_KEY, SERVER_HOSTNAME, HTTP_PATH]):
    raise ValueError("Required credentials are missing")

API_URL = "https://api.balldontlie.io/v1/games"

SEASON = 2025
SEASON_TYPE = "regular"
PER_PAGE = 100
MAX_PAGES = 2
REQUEST_DELAY_SECONDS = 15

BATCH_ID = str(uuid.uuid4())


# -----------------------------------------
# 2. SQL insert statement
# -----------------------------------------


MERGE_SQL = """
MERGE INTO nba_analytics.bronze.games_raw AS target

USING (
    SELECT
        :batch_id AS batch_id,
        current_timestamp() AS ingested_at,
        :source_endpoint AS source_endpoint,
        CAST(:season AS INT) AS season,
        :season_type AS season_type,
        CAST(:request_cursor AS STRING) AS request_cursor,
        CAST(:next_cursor AS STRING) AS next_cursor,
        CAST(:per_page AS INT) AS per_page,
        CAST(:records_returned AS INT) AS records_returned,
        :payload AS payload
) AS source

ON target.source_endpoint = source.source_endpoint
AND target.season = source.season
AND target.season_type = source.season_type
AND target.request_cursor <=> source.request_cursor
AND sha2(target.payload, 256) = sha2(source.payload, 256)

WHEN NOT MATCHED THEN
INSERT (
    batch_id,
    ingested_at,
    source_endpoint,
    season,
    season_type,
    request_cursor,
    next_cursor,
    per_page,
    records_returned,
    payload
)
VALUES (
    source.batch_id,
    source.ingested_at,
    source.source_endpoint,
    source.season,
    source.season_type,
    source.request_cursor,
    source.next_cursor,
    source.per_page,
    source.records_returned,
    source.payload
)
"""


# -----------------------------------------
# 3. Connect to Databricks
# -----------------------------------------

print("Starting ingestion batch:", BATCH_ID)

with sql.connect(
    server_hostname=SERVER_HOSTNAME,
    http_path=HTTP_PATH,
    auth_type="databricks-oauth"
) as connection:

    with connection.cursor() as db_cursor:

        db_cursor.execute("SELECT 1")
        db_cursor.fetchone()

        print("Databricks connection successful")

        # ---------------------------------
        # 4. Pagination loop
        # ---------------------------------

        next_cursor = None
        seen_cursors = set()

        total_games = 0
        total_pages = 0

        for page_number in range(1, MAX_PAGES + 1):

            if page_number > 1:
                time.sleep(REQUEST_DELAY_SECONDS)

            request_cursor = next_cursor

            params = {
                "seasons[]": SEASON,
                "season_type": SEASON_TYPE,
                "per_page": PER_PAGE
            }

            if request_cursor is not None:
                params["cursor"] = request_cursor

            print(f"\nRequesting page {page_number}...")

            response = requests.get(
                API_URL,
                headers={"Authorization": API_KEY},
                params=params,
                timeout=30
            )

            if response.status_code == 429:
                raise RuntimeError(
                    "API rate limit reached. Stop and retry later."
                )

            response.raise_for_status()

            data = response.json()
            games = data["data"]
            next_cursor = data.get("meta", {}).get("next_cursor")

            if not games:
                print("No games returned. Stopping.")
                break

            if (
                next_cursor is not None
                and str(next_cursor) == str(request_cursor)
            ):
                raise ValueError("Pagination cursor did not advance")

            # ---------------------------------
            # 5. Save this API page to Bronze
            # ---------------------------------

            db_cursor.execute(
                MERGE_SQL,
                parameters={
                    "batch_id": BATCH_ID,
                    "source_endpoint": "/v1/games",
                    "season": SEASON,
                    "season_type": SEASON_TYPE,
                    "request_cursor": (
                        str(request_cursor)
                        if request_cursor is not None
                        else None
                    ),
                    "next_cursor": (
                        str(next_cursor)
                        if next_cursor is not None
                        else None
                    ),
                    "per_page": PER_PAGE,
                    "records_returned": len(games),
                    "payload": response.text
                }
            )

            total_games += len(games)
            total_pages += 1

            print("Games retrieved:", len(games))
            print("Bronze page processed successfully")
            print("Next cursor:", next_cursor)

            if next_cursor is None:
                print("No more pages available")
                break

            if str(next_cursor) in seen_cursors:
                raise ValueError("Repeated pagination cursor detected")

            seen_cursors.add(str(next_cursor))


# -----------------------------------------
# 6. Batch summary
# -----------------------------------------

print("\n----- INGESTION SUMMARY -----")
print("Batch ID:", BATCH_ID)
print("Pages loaded:", total_pages)
print("Games retrieved:", total_games)
print("Ingestion complete")
