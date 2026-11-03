
import os
import uuid
from collections import Counter
from pathlib import Path

import requests
from databricks import sql
from dotenv import load_dotenv


# ---------------------------------------
# 1. Explicit configuration path
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


# ---------------------------------------
# 2. Extract all teams
# ---------------------------------------

API_URL = "https://api.balldontlie.io/v1/teams"

response = requests.get(
    API_URL,
    headers={"Authorization": API_KEY},
    timeout=30
)

response.raise_for_status()

data = response.json()
teams = data.get("data")

if not isinstance(teams, list) or not teams:
    raise ValueError("Expected a nonempty list of teams")

team_ids = [team["id"] for team in teams]

if len(team_ids) != len(set(team_ids)):
    raise ValueError("Duplicate team IDs detected in source response")

conference_counts = Counter(
    team.get("conference") for team in teams
)

print("Teams returned:", len(teams))
print("Conference distribution:", dict(conference_counts))


# ---------------------------------------
# 3. Delta MERGE
# ---------------------------------------

MERGE_SQL = """
MERGE INTO nba_analytics.bronze.teams_raw AS target

USING (
    SELECT
        :batch_id AS batch_id,
        current_timestamp() AS ingested_at,
        :source_endpoint AS source_endpoint,
        CAST(:records_returned AS INT) AS records_returned,
        :payload AS payload
) AS source

ON target.source_endpoint = source.source_endpoint
AND sha2(target.payload, 256) = sha2(source.payload, 256)

WHEN NOT MATCHED THEN
INSERT (
    batch_id,
    ingested_at,
    source_endpoint,
    records_returned,
    payload
)
VALUES (
    source.batch_id,
    source.ingested_at,
    source.source_endpoint,
    source.records_returned,
    source.payload
)
"""


# ---------------------------------------
# 4. Load to Databricks
# ---------------------------------------

batch_id = str(uuid.uuid4())

with sql.connect(
    server_hostname=HOSTNAME,
    http_path=HTTP_PATH,
    auth_type="databricks-oauth"
) as connection:

    with connection.cursor() as cursor:

        cursor.execute(
            MERGE_SQL,
            parameters={
                "batch_id": batch_id,
                "source_endpoint": "/v1/teams",
                "records_returned": len(teams),
                "payload": response.text
            }
        )

print("Teams Bronze MERGE completed!")
print("Batch ID:", batch_id)

