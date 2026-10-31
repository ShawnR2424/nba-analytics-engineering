
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

# Step 1: Locate the project root
project_root = Path(__file__).resolve().parents[1]

# Step 2: Load variables from .env
load_dotenv(project_root / ".env")

# Step 3: Retrieve the API key
api_key = os.getenv("BALLDONTLIE_API_KEY")

if not api_key:
    raise ValueError("BALLDONTLIE_API_KEY is missing")

# Step 4: Define the API endpoint
url = "https://api.balldontlie.io/v1/teams"

# Step 5: Configure authentication
headers = {
    "Authorization": api_key
}

# Step 6: Request NBA team data
response = requests.get(
    url,
    headers=headers,
    timeout=30
)

# Step 7: Check for HTTP errors
response.raise_for_status()

# Step 8: Convert the JSON response to Python objects
data = response.json()

# Step 9: Examine the results
teams = data["data"]

print("Connection successful!")
print("Number of teams returned:", len(teams))

if teams:
    print("First team:", teams[0]["full_name"])

#Testing another prompt:
print("\nLast 15 teams returned:")

for team in teams[-15:]:
    print(team["id"], team["full_name"])
from collections import Counter

# Count teams by conference
conference_counts = Counter(
    team.get("conference") for team in teams
)

print("\nTeams by conference:")
for conference, count in conference_counts.items():
    print(conference, count)

# Identify potential active NBA teams
nba_teams = [
    team for team in teams
    if team.get("conference") in ["East", "West"]
]

print("\nPotential NBA teams:", len(nba_teams))
