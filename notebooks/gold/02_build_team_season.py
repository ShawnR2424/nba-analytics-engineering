# Databricks notebook source

from pyspark.sql import functions as F


# ---------------------------------------
# 1. Notebook configuration
# ---------------------------------------

dbutils.widgets.dropdown(
    "season",
    "2023",
    ["2023", "2024"],
    "NBA Season"
)

SEASON = int(dbutils.widgets.get("season"))
SEASON_TYPE = "regular"

EXPECTED_TEAMS = 30
EXPECTED_GAMES_PER_TEAM = 82
EXPECTED_GAMES = 1230
EXPECTED_TEAM_GAMES = 2460

print("--------------------------------")
print("NBA Gold Team-Season Transformation")
print("--------------------------------")
print(f"Season: {SEASON}-{str(SEASON + 1)[-2:]}")
print(f"Season type: {SEASON_TYPE}")


# ---------------------------------------
# 2. Read source tables
# ---------------------------------------

facts = (
    spark.table("nba_analytics.gold.team_game")
    .filter(
        (F.col("season") == SEASON)
        & (F.col("season_type") == SEASON_TYPE)
    )
)

teams = spark.table("nba_analytics.silver.teams")


# ---------------------------------------
# 3. Validate Gold source records
# ---------------------------------------

source_metrics = facts.agg(
    F.count("*").alias("team_game_rows"),
    F.countDistinct("game_id").alias("unique_games"),
    F.countDistinct("game_id", "team_id")
        .alias("unique_team_game_keys"),
    F.countDistinct("team_id").alias("unique_teams"),
    F.sum("win").alias("wins"),
    F.sum("loss").alias("losses"),
    F.sum("point_diff").alias("net_point_diff")
).first().asDict()

print("Source metrics:", source_metrics)

source_checks = {
    "expected_team_game_rows":
        source_metrics["team_game_rows"] == EXPECTED_TEAM_GAMES,

    "expected_unique_games":
        source_metrics["unique_games"] == EXPECTED_GAMES,

    "unique_team_game_keys":
        source_metrics["unique_team_game_keys"] == EXPECTED_TEAM_GAMES,

    "30_participating_teams":
        source_metrics["unique_teams"] == EXPECTED_TEAMS,

    "balanced_wins_losses":
        source_metrics["wins"] == EXPECTED_GAMES
        and source_metrics["losses"] == EXPECTED_GAMES,

    "zero_source_point_diff":
        source_metrics["net_point_diff"] == 0
}

invalid_source_rows = facts.filter(
    F.col("game_id").isNull()
    | F.col("team_id").isNull()
    | F.col("opponent_team_id").isNull()
    | F.col("game_date").isNull()
    | F.col("is_home").isNull()
    | F.col("win").isNull()
    | F.col("loss").isNull()
    | F.col("points_for").isNull()
    | F.col("points_against").isNull()
    | F.col("point_diff").isNull()
    | F.col("source_ingested_at").isNull()
    | (F.col("team_id") == F.col("opponent_team_id"))
    | (F.col("win") + F.col("loss") != 1)
    | (
        F.col("points_for") - F.col("points_against")
        != F.col("point_diff")
    )
).count()

source_checks["valid_source_fields"] = (
    invalid_source_rows == 0
)

print("\n----- SOURCE QUALITY REPORT -----")

for name, passed in source_checks.items():
    print(f"{name}: {'PASS' if passed else 'FAIL'}")

if not all(source_checks.values()):
    raise ValueError("Gold team-game source quality failed")


# ---------------------------------------
# 4. Validate team reference data
# ---------------------------------------

team_count = teams.count()
unique_team_ids = (
    teams.select("team_id").distinct().count()
)

if team_count != EXPECTED_TEAMS:
    raise ValueError("Expected 30 Silver teams")

if unique_team_ids != EXPECTED_TEAMS:
    raise ValueError("Duplicate or missing Silver team IDs")

team_attributes = teams.select(
    "team_id",
    F.col("full_name").alias("team_name"),
    "conference"
)

missing_teams = (
    facts.select("team_id").distinct()
    .join(
        team_attributes.select("team_id"),
        on="team_id",
        how="left_anti"
    )
)

missing_count = missing_teams.count()

print("Unmatched team IDs:", missing_count)

if missing_count > 0:
    display(missing_teams)
    raise ValueError("Unknown team IDs in Gold facts")


# ---------------------------------------
# 5. Aggregate season performance
# ---------------------------------------

season_stats = (
    facts
    .groupBy("season", "season_type", "team_id")
    .agg(
        F.count("*").cast("int")
            .alias("games_played"),

        F.sum("win").cast("int")
            .alias("wins"),

        F.sum("loss").cast("int")
            .alias("losses"),

        F.sum(
            F.when(
                F.col("is_home"),
                F.col("win")
            ).otherwise(0)
        ).cast("int").alias("home_wins"),

        F.sum(
            F.when(
                ~F.col("is_home"),
                F.col("win")
            ).otherwise(0)
        ).cast("int").alias("away_wins"),

        # Additional fields for validation.
        # These won't be saved in the final mart.

        F.sum(
            F.col("is_home").cast("int")
        ).cast("int").alias("home_games"),

        F.sum(
            (~F.col("is_home")).cast("int")
        ).cast("int").alias("away_games"),

        F.sum("points_for").cast("bigint")
            .alias("total_points_for"),

        F.sum("points_against").cast("bigint")
            .alias("total_points_against"),

        F.sum("point_diff").cast("bigint")
            .alias("point_differential"),

        F.max("source_ingested_at")
            .alias("source_ingested_at")
    )
)


# ---------------------------------------
# 6. Calculate performance KPIs
# ---------------------------------------

season_stats = (
    season_stats
    .withColumn(
        "win_pct",
        F.round(
            F.col("wins") / F.col("games_played"),
            4
        )
    )
    .withColumn(
        "points_per_game",
        F.round(
            F.col("total_points_for")
            / F.col("games_played"),
            2
        )
    )
    .withColumn(
        "points_allowed_per_game",
        F.round(
            F.col("total_points_against")
            / F.col("games_played"),
            2
        )
    )
)


# ---------------------------------------
# 7. Join descriptive team attributes
# ---------------------------------------

team_season_candidate = season_stats.join(
    team_attributes,
    on="team_id",
    how="inner"
)


# ---------------------------------------
# 8. Automated quality gate
# ---------------------------------------

metrics = team_season_candidate.agg(
    F.count("*").alias("team_rows"),

    F.countDistinct("team_id")
        .alias("unique_teams"),

    F.sum("games_played")
        .alias("team_games"),

    F.min("games_played")
        .alias("min_games"),

    F.max("games_played")
        .alias("max_games"),

    F.sum("wins")
        .alias("league_wins"),

    F.sum("losses")
        .alias("league_losses"),

    F.sum("home_games")
        .alias("league_home_games"),

    F.sum("away_games")
        .alias("league_away_games"),

    F.sum("total_points_for")
        .alias("league_points_for"),

    F.sum("total_points_against")
        .alias("league_points_against"),

    F.sum("point_differential")
        .alias("league_point_diff")
).first().asDict()


# Validate each team's arithmetic.
invalid_team_rows = team_season_candidate.filter(
    F.col("team_name").isNull()
    | (F.trim("team_name") == "")
    | F.col("conference").isNull()
    | (~F.col("conference").isin("East", "West"))
    | F.col("source_ingested_at").isNull()
    | (F.col("games_played") != EXPECTED_GAMES_PER_TEAM)
    | (
        F.col("wins") + F.col("losses")
        != F.col("games_played")
    )
    | (
        F.col("home_games") + F.col("away_games")
        != F.col("games_played")
    )
    | (
        F.col("home_wins") + F.col("away_wins")
        != F.col("wins")
    )
    | (
        F.col("total_points_for")
        - F.col("total_points_against")
        != F.col("point_differential")
    )
).count()


checks = {
    "30_unique_teams":
        metrics["team_rows"] == EXPECTED_TEAMS
        and metrics["unique_teams"] == EXPECTED_TEAMS,

    "2460_team_games":
        metrics["team_games"] == EXPECTED_TEAM_GAMES,

    "82_games_per_team":
        metrics["min_games"] == EXPECTED_GAMES_PER_TEAM
        and metrics["max_games"] == EXPECTED_GAMES_PER_TEAM,

    "balanced_wins_losses":
        metrics["league_wins"] == EXPECTED_GAMES
        and metrics["league_losses"] == EXPECTED_GAMES,

    "balanced_home_away_games":
        metrics["league_home_games"] == EXPECTED_GAMES
        and metrics["league_away_games"] == EXPECTED_GAMES,

    "balanced_scoring":
        metrics["league_points_for"]
        == metrics["league_points_against"],

    "zero_league_point_diff":
        metrics["league_point_diff"] == 0,

    "valid_team_level_metrics":
        invalid_team_rows == 0
}


print("\n----- TEAM-SEASON QUALITY REPORT -----")
print("Season:", SEASON)
print("Metrics:", metrics)

for name, passed in checks.items():
    print(f"{name}: {'PASS' if passed else 'FAIL'}")

failures = [
    name for name, passed in checks.items()
    if not passed
]

if failures:
    raise ValueError(
        f"Team-season quality gate FAILED: {failures}"
    )

print("\nTeam-season quality gate PASSED")


# ---------------------------------------
# 9. Select final reporting columns
# ---------------------------------------

team_season = team_season_candidate.drop(
    "home_games",
    "away_games"
)

# Make the DataFrame available to SQL.
team_season.createOrReplaceTempView(
    "team_season_updates"
)


# ---------------------------------------
# 10. MERGE into permanent Gold mart
# ---------------------------------------

spark.sql("""
MERGE INTO nba_analytics.gold.team_season AS target

USING team_season_updates AS source

ON target.season = source.season
AND target.season_type = source.season_type
AND target.team_id = source.team_id

WHEN MATCHED THEN UPDATE SET *

WHEN NOT MATCHED THEN INSERT *
""")

print("\nGold Team-Season MERGE completed!")


# ---------------------------------------
# 11. Validate persisted results
# ---------------------------------------

persisted = (
    spark.table("nba_analytics.gold.team_season")
    .filter(
        (F.col("season") == SEASON)
        & (F.col("season_type") == SEASON_TYPE)
    )
)

persisted_metrics = persisted.agg(
    F.count("*").alias("rows"),
    F.countDistinct("team_id").alias("unique_teams"),
    F.sum("games_played").alias("team_games"),
    F.sum("wins").alias("wins"),
    F.sum("losses").alias("losses"),
    F.sum("point_differential").alias("point_diff")
).first().asDict()

print("Persisted metrics:", persisted_metrics)

if (
    persisted_metrics["rows"] != EXPECTED_TEAMS
    or persisted_metrics["unique_teams"] != EXPECTED_TEAMS
    or persisted_metrics["team_games"] != EXPECTED_TEAM_GAMES
    or persisted_metrics["wins"] != EXPECTED_GAMES
    or persisted_metrics["losses"] != EXPECTED_GAMES
    or persisted_metrics["point_diff"] != 0
):
    raise ValueError(
        "Post-MERGE Team-Season validation failed"
    )

print("Post-MERGE quality check PASSED")


# ---------------------------------------
# 12. Display team standings
# ---------------------------------------

display(
    persisted.orderBy(
        F.col("wins").desc(),
        F.col("team_name")
    )
)

print(
    f"\nSeason {SEASON}-{str(SEASON + 1)[-2:]} "
    "Team-Season transformation completed!"
)
