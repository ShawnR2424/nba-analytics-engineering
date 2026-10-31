
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

# Load API credentials
project_root = Path(__file__).resolve().parents[1]
load_dotenv(project_root / ".env")

api_key = os.getenv("BALLDONTLIE_API_KEY")

if not api_key:
    raise ValueError("BALLDONTLIE_API_KEY is missing")

# Define the endpoint
url = "https://api.balldontlie.io/v1/games"

headers = {
    "Authorization": api_key
}

# Request a small sample of 2025-26 regular-season games
params = {
    "seasons[]": 2025,
    "season_type": "regular",
    "per_page": 3
}

response = requests.get(
    url,
    headers=headers,
    params=params,
    timeout=30
)

response.raise_for_status()

data = response.json()

# Examine the response
print("Response keys:", list(data.keys()))
print("Number of games returned:", len(data["data"]))
print("Pagination metadata:", data.get("meta"))

for game in data["data"]:
    print("\nGame ID:", game["id"])
    print("Date:", game["date"])
    print("Home team:", game["home_team"]["full_name"])
    print("Visitor team:", game["visitor_team"]["full_name"])
    print("Score:", game["home_team_score"], "-", game["visitor_team_score"])
    print("Status:", game["status"])


# Retrieve the pagination cursor from page 1
next_cursor = data.get("meta", {}).get("next_cursor")

if next_cursor is not None:

    # Copy our original request parameters
    next_params = params.copy()

    # Add the cursor for page 2
    next_params["cursor"] = next_cursor

    # Request the next page
    response2 = requests.get(
        url,
        headers=headers,
        params=next_params,
        timeout=30
    )

    response2.raise_for_status()
    data2 = response2.json()

    # Examine the next page
    print("\n--- PAGE 2 ---")

    for game in data2["data"]:
        print(game["id"], game["date"])

    print("Next cursor:", data2.get("meta", {}).get("next_cursor"))

    # Check for duplicate game IDs across pages
    first_ids = {game["id"] for game in data["data"]}
    second_ids = {game["id"] for game in data2["data"]}

    print("Duplicate IDs across pages:", first_ids & second_ids)

else:
    print("No additional pages available.")

