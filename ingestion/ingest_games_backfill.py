
import argparse
import os
import time
import uuid
from pathlib import Path

import requests
from databricks import sql
from dotenv import load_dotenv


# ---------------------------------------
# 1. Explicit project configuration
# ---------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"

if not ENV_PATH.is_file():
    raise FileNotFoundError(f"Missing configuration: {ENV_PATH}")

load_dotenv(ENV_PATH)

API_KEY = os.getenv("BALLDONTLIE_API_KEY")
HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

if not all([API_KEY, HOSTNAME, HTTP_PATH]):
    raise ValueError("Required environment variables are missing")

API_URL = "https://api.balldontlie.io/v1/games"
REQUEST_DELAY = 15
MAX_ATTEMPTS = 4


# ---------------------------------------
# 2. Command-line arguments
# ---------------------------------------

parser = argparse.ArgumentParser(
    description="Ingest NBA Games API pages into Bronze"
)

parser.add_argument("--season", type=int, required=True)

parser.add_argument(
    "--season-type",
    choices=["regular", "ist", "playin", "playoffs"],
    default="regular"
)

parser.add_argument("--per-page", type=int, default=50)
parser.add_argument("--max-pages", type=int, default=2)
parser.add_argument("--resume-cursor", default=None)

args = parser.parse_args()

if not 1 <= args.per_page <= 100:
    parser.error("--per-page must be between 1 and 100")

if args.max_pages < 1:
    parser.error("--max-pages must be at least 1")


# ---------------------------------------
# 3. Retry temporary API failures
# ---------------------------------------

def fetch_page(session, params):

    for attempt in range(1, MAX_ATTEMPTS + 1):

        try:
            response = session.get(
                API_URL,
                headers={"Authorization": API_KEY},
                params=params,
                timeout=30
            )

        except (requests.Timeout, requests.ConnectionError):
            if attempt == MAX_ATTEMPTS:
                raise

            delay = REQUEST_DELAY * attempt
            print(f"Connection problem. Retrying in {delay}s")
            time.sleep(delay)
            continue

        if response.status_code == 429:
            if attempt == MAX_ATTEMPTS:
                response.raise_for_status()

            try:
                retry_after = float(
                    response.headers.get("Retry-After", "65")
                )
            except ValueError:
                retry_after = 65

            delay = max(65, retry_after)
            print(f"Rate limited. Retrying in {delay}s")
            time.sleep(delay)
            continue

        if 500 <= response.status_code < 600:
            if attempt == MAX_ATTEMPTS:
                response.raise_for_status()

            delay = REQUEST_DELAY * attempt
            print(f"Server error. Retrying in {delay}s")
            time.sleep(delay)
            continue

        response.raise_for_status()
        return response

    raise RuntimeError("API retry attempts exhausted")


# ---------------------------------------
# 4. Delta MERGE statement
# ---------------------------------------

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


# ---------------------------------------
# 5. Execute the ingestion
# ---------------------------------------

batch_id = str(uuid.uuid4())
next_cursor = args.resume_cursor
seen_cursors = set()

if next_cursor is not None:
    seen_cursors.add(str(next_cursor))

total_pages = 0
total_games = 0

print("Batch ID:", batch_id)
print("Season:", args.season)
print("Season type:", args.season_type)
print("Starting cursor:", next_cursor)

with requests.Session() as session:

    with sql.connect(
        server_hostname=HOSTNAME,
        http_path=HTTP_PATH,
        auth_type="databricks-oauth"
    ) as connection:

        with connection.cursor() as db_cursor:

            db_cursor.execute("SELECT 1")
            db_cursor.fetchone()
            print("Databricks connected")

            for page_number in range(1, args.max_pages + 1):

                if page_number > 1:
                    time.sleep(REQUEST_DELAY)

                request_cursor = next_cursor

                params = {
                    "seasons[]": args.season,
                    "season_type": args.season_type,
                    "per_page": args.per_page
                }

                if request_cursor is not None:
                    params["cursor"] = request_cursor

                print(f"\nRequesting page {page_number}")

                response = fetch_page(session, params)
                data = response.json()

                games = data["data"]
                next_cursor = data.get("meta", {}).get("next_cursor")

                if not games:
                    print("No games returned. Stopping.")
                    break

                if (
                    next_cursor is not None
                    and str(next_cursor) in seen_cursors
                ):
                    raise RuntimeError(
                        "Repeated pagination cursor detected"
                    )

                # Native SQL parameters have a size limit.
                if len(response.content) > 900_000:
                    raise ValueError(
                        "API response too large for this SQL method. "
                        "Restart with a smaller --per-page value."
                    )

                db_cursor.execute(
                    MERGE_SQL,
                    parameters={
                        "batch_id": batch_id,
                        "source_endpoint": "/v1/games",
                        "season": args.season,
                        "season_type": args.season_type,
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
                        "per_page": args.per_page,
                        "records_returned": len(games),
                        "payload": response.text
                    }
                )

                total_pages += 1
                total_games += len(games)

                print("Page processed:", page_number)
                print("Games:", len(games))
                print("Next cursor:", next_cursor)

                if next_cursor is None:
                    print("Reached final page")
                    break

                seen_cursors.add(str(next_cursor))


# ---------------------------------------
# 6. Ingestion summary
# ---------------------------------------

print("\n----- BATCH SUMMARY -----")
print("Batch ID:", batch_id)
print("Pages processed:", total_pages)
print("Games retrieved:", total_games)
print("Resume cursor:", next_cursor)
print("Run completed")
