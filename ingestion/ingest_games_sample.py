
import os
import uuid
from pathlib import Path

import requests
from databricks import sql
from dotenv import load_dotenv


# 1. Load local configuration
project_root = Path(__file__).resolve().parents[1]
load_dotenv(project_root / ".env")

api_key = os.getenv("BALLDONTLIE_API_KEY")
server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path = os.getenv("DATABRICKS_HTTP_PATH")

if not all([api_key, server_hostname, http_path]):
    raise ValueError("Required API or Databricks configuration is missing")


# 2. Define API request
url = "https://api.balldontlie.io/v1/games"

params = {
    "seasons[]": 2025,
    "season_type": "regular",
    "per_page": 3
}

headers = {"Authorization": api_key}


# 3. Define the parameterized INSERT
insert_sql = """
INSERT INTO nba_analytics.bronze.games_raw (
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
    :batch_id,
    current_timestamp(),
    :source_endpoint,
    :season,
    :season_type,
    NULL,
    :next_cursor,
    :per_page,
    :records_returned,
    :payload
)
"""


# 4. Connect to Databricks
with sql.connect(
    server_hostname=server_hostname,
    http_path=http_path,
    auth_type="databricks-oauth"
) as connection:

    with connection.cursor() as cursor:

        # Verify Databricks connectivity before calling the API
        cursor.execute("SELECT 1")
        cursor.fetchone()
        print("Databricks connection successful!")

        # 5. Extract one page of NBA games
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=30
        )
        response.raise_for_status()

        data = response.json()
        games = data["data"]
        next_cursor = data.get("meta", {}).get("next_cursor")

        if next_cursor is None:
            raise ValueError("Expected a pagination cursor for this sample")

        # 6. Generate ingestion metadata
        batch_id = str(uuid.uuid4())

        # 7. Insert the original API response into Bronze
        cursor.execute(
            insert_sql,
            parameters={
                "batch_id": batch_id,
                "source_endpoint": "/v1/games",
                "season": 2025,
                "season_type": "regular",
                "next_cursor": str(next_cursor),
                "per_page": 3,
                "records_returned": len(games),
                "payload": response.text
            }
        )

        print("Bronze insertion successful!")
        print("Batch ID:", batch_id)
        print("Games in API response:", len(games))
        print("Next cursor:", next_cursor)

