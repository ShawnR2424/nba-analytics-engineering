
# Databricks notebook source

from pyspark.sql import functions as F

# ------------------------------------
# 1. Read Gold fact and Silver teams
# ------------------------------------

facts = (
    spark.table("nba_analytics.gold.team_game")
    .filter(
        (F.col("season") == 2024)
        & (F.col("season_type") == "regular")
    )
)

teams = spark.table("nba_analytics.silver.teams")

# Validate source prerequisites
if facts.count() != 2460:
    raise ValueError("Expected 2,460 team-game records")

if facts.select("game_id", "team_id").distinct().count() != 2460:
    raise ValueError("Duplicate team-game keys detected")

if teams.count() != 30 or teams.select("team_id").distinct().count() != 30:
    raise ValueError("Teams reference data is incomplete or duplicated")

missing_teams = (
    facts.select("team_id").distinct()
    .join(teams.select("team_id"), "team_id", "left_anti")
)

if missing_teams.count() != 0:
    raise ValueError("Gold facts reference unknown teams")


# ------------------------------------
# 2. Aggregate team-season statistics
# ------------------------------------

season_stats = (
    facts
    .groupBy("season", "season_type", "team_id")
    .agg(
        F.count("*").cast("int").alias("games_played"),

        F.sum("win").cast("int").alias("wins"),
        F.sum("loss").cast("int").alias("losses"),

        F.sum(
            F.when(F.col("is_home"), F.col("win")).otherwise(0)
        ).cast("int").alias("home_wins"),

        F.sum(
            F.when(~F.col("is_home"), F.col("win")).otherwise(0)
        ).cast("int").alias("away_wins"),

        F.sum("points_for").cast("bigint").alias("total_points_for"),

        F.sum("points_against").cast("bigint")
            .alias("total_points_against"),

        F.sum("point_diff").cast("bigint")
            .alias("point_differential"),

        F.max("source_ingested_at")
            .alias("source_ingested_at")
    )
    .withColumn(
        "win_pct",
        F.round(F.col("wins") / F.col("games_played"), 4)
    )
    .withColumn(
        "points_per_game",
        F.round(
            F.col("total_points_for") / F.col("games_played"), 2
        )
    )
    .withColumn(
        "points_allowed_per_game",
        F.round(
            F.col("total_points_against") / F.col("games_played"), 2
        )
    )
)


# ------------------------------------
# 3. Add team names and conferences
# ------------------------------------

team_attributes = teams.select(
    "team_id",
    F.col("full_name").alias("team_name"),
    "conference"
)

team_season = season_stats.join(
    team_attributes,
    on="team_id",
    how="inner"
)


# ------------------------------------
# 4. Automated data-quality gate
# ------------------------------------

metrics = team_season.agg(
    F.count("*").alias("teams"),
    F.countDistinct("team_id").alias("unique_teams"),
    F.sum("games_played").alias("team_games"),
    F.min("games_played").alias("min_games"),
    F.max("games_played").alias("max_games"),
    F.sum("wins").alias("total_wins"),
    F.sum("losses").alias("total_losses"),
    F.sum("point_differential").alias("league_point_diff")
).first().asDict()

invalid_splits = team_season.filter(
    (F.col("home_wins") + F.col("away_wins") != F.col("wins"))
    | (F.col("wins") + F.col("losses") != F.col("games_played"))
).count()

checks = {
    "30_unique_teams":
        metrics["teams"] == 30
        and metrics["unique_teams"] == 30,

    "2460_team_games":
        metrics["team_games"] == 2460,

    "82_games_per_team":
        metrics["min_games"] == 82
        and metrics["max_games"] == 82,

    "balanced_wins_losses":
        metrics["total_wins"] == 1230
        and metrics["total_losses"] == 1230,

    "zero_league_point_diff":
        metrics["league_point_diff"] == 0,

    "valid_win_loss_splits":
        invalid_splits == 0
}

print("----- TEAM-SEASON QUALITY REPORT -----")

for name, passed in checks.items():
    print(f"{name}: {'PASS' if passed else 'FAIL'}")

if not all(checks.values()):
    raise ValueError(f"Team-season validation failed: {metrics}")

print("Team-season quality gate PASSED")


# ------------------------------------
# 5. MERGE into permanent mart
# ------------------------------------

team_season.createOrReplaceTempView("team_season_updates")

spark.sql("""
MERGE INTO nba_analytics.gold.team_season AS target

USING team_season_updates AS source

ON target.season = source.season
AND target.season_type = source.season_type
AND target.team_id = source.team_id

WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print("Gold team-season MERGE completed!")

display(
    team_season
    .orderBy(F.col("wins").desc(), F.col("team_name"))
)

