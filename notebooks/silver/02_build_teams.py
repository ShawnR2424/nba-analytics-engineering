
# Databricks notebook source

from pyspark.sql import functions as F


# ---------------------------------------
# 1. Read the latest Bronze snapshot
# ---------------------------------------

latest_bronze = (
    spark.table("nba_analytics.bronze.teams_raw")
    .filter(F.col("source_endpoint") == "/v1/teams")
    .orderBy(
        F.col("ingested_at").desc(),
        F.col("batch_id").desc()
    )
    .limit(1)
)

if latest_bronze.count() != 1:
    raise ValueError("No Teams Bronze snapshot found")


# ---------------------------------------
# 2. Define the JSON schema
# ---------------------------------------

TEAMS_SCHEMA = """
STRUCT<
    data: ARRAY<STRUCT<
        id: INT,
        abbreviation: STRING,
        city: STRING,
        conference: STRING,
        division: STRING,
        full_name: STRING,
        name: STRING
    >>
>
"""


# ---------------------------------------
# 3. Parse and validate the source
# ---------------------------------------

parsed_df = latest_bronze.select(
    "records_returned",
    F.col("ingested_at").alias("source_ingested_at"),
    F.from_json("payload", TEAMS_SCHEMA).alias("body")
)

source_check = parsed_df.select(
    "records_returned",
    F.size("body.data").alias("parsed_teams")
).first()

if (
    source_check["parsed_teams"] is None
    or source_check["parsed_teams"]
       != source_check["records_returned"]
):
    raise ValueError("Teams JSON parsing or count check failed")

print("Source teams:", source_check["parsed_teams"])


# ---------------------------------------
# 4. Explode the team array
# ---------------------------------------

all_teams_df = parsed_df.select(
    "source_ingested_at",
    F.explode("body.data").alias("team")
)


# ---------------------------------------
# 5. Flatten and filter NBA teams
# ---------------------------------------

teams_df = (
    all_teams_df
    .select(
        F.col("team.id").alias("team_id"),
        F.col("team.full_name").alias("full_name"),
        F.col("team.abbreviation").alias("abbreviation"),
        F.col("team.city").alias("city"),
        F.col("team.name").alias("name"),
        F.col("team.conference").alias("conference"),
        F.col("team.division").alias("division"),
        F.col("source_ingested_at")
    )
    .filter(F.col("conference").isin("East", "West"))
)


# ---------------------------------------
# 6. Validate the 30-team reference data
# ---------------------------------------

total_teams = teams_df.count()

unique_team_ids = (
    teams_df
    .select("team_id")
    .distinct()
    .count()
)

conference_counts = {
    row["conference"]: row["count"]
    for row in teams_df.groupBy("conference").count().collect()
}

invalid_teams = teams_df.filter(
    F.col("team_id").isNull()
    | F.col("full_name").isNull()
    | (F.trim("full_name") == "")
    | F.col("abbreviation").isNull()
    | (F.trim("abbreviation") == "")
    | F.col("division").isNull()
    | F.col("source_ingested_at").isNull()
).count()

print("NBA teams:", total_teams)
print("Unique IDs:", unique_team_ids)
print("Conferences:", conference_counts)
print("Invalid records:", invalid_teams)

if total_teams != 30:
    raise ValueError("Expected exactly 30 NBA teams")

if unique_team_ids != total_teams:
    raise ValueError("Duplicate NBA team IDs")

if conference_counts != {"East": 15, "West": 15}:
    raise ValueError("Unexpected conference distribution")

if invalid_teams != 0:
    raise ValueError("Invalid team attributes detected")


# ---------------------------------------
# 7. Validate references from Silver Games
# ---------------------------------------

games_df = (
    spark.table("nba_analytics.silver.games")
    .filter(
        (F.col("season") == 2024)
        & (F.col("season_type") == "regular")
    )
)

game_team_ids = (
    games_df.select(
        F.col("home_team_id").alias("team_id")
    )
    .union(
        games_df.select(
            F.col("visitor_team_id").alias("team_id")
        )
    )
    .distinct()
)

missing_teams = game_team_ids.join(
    teams_df.select("team_id"),
    on="team_id",
    how="left_anti"
)

missing_count = missing_teams.count()

print("Unmatched team IDs:", missing_count)

if missing_count > 0:
    display(missing_teams)
    raise ValueError("Games reference unknown teams")


# ---------------------------------------
# 8. Persist validated teams into Silver
# ---------------------------------------

teams_df.createOrReplaceTempView("silver_teams_updates")

spark.sql("""
MERGE INTO nba_analytics.silver.teams AS target

USING silver_teams_updates AS source

ON target.team_id = source.team_id

WHEN MATCHED AND
    source.source_ingested_at > target.source_ingested_at
THEN UPDATE SET *

WHEN NOT MATCHED
THEN INSERT *
""")

print("Silver Teams MERGE completed!")

display(teams_df.orderBy("team_id"))

