
# Databricks notebook source

from pyspark.sql import functions as F


# ---------------------------------------
# 1. Read validated Silver games
# ---------------------------------------

games = (
    spark.table("nba_analytics.silver.games")
    .filter(
        (F.col("season") == 2024)
        & (F.col("season_type") == "regular")
    )
)

# Our first model uses a completed season.
if games.count() != 1230:
    raise ValueError("Expected 1,230 Silver games")


# ---------------------------------------
# 2. Validate source games before use
# ---------------------------------------

is_final = F.coalesce(
    F.upper(F.trim(F.col("status"))).startswith("FINAL"),
    F.lit(False)
)

invalid_source = games.filter(
    ~is_final
    | F.col("game_id").isNull()
    | F.col("home_team_id").isNull()
    | F.col("visitor_team_id").isNull()
    | F.col("source_ingested_at").isNull()
    | F.col("game_date").isNull()
    | (F.col("home_team_id") == F.col("visitor_team_id"))
    | F.col("home_team_score").isNull()
    | F.col("visitor_team_score").isNull()
    | (F.col("home_team_score") < 0)
    | (F.col("visitor_team_score") < 0)
    | (F.col("home_team_score") == F.col("visitor_team_score"))
).count()

if invalid_source != 0:
    raise ValueError("Invalid or incomplete Silver game records")


# ---------------------------------------
# 3. Create home-team perspective
# ---------------------------------------

home_rows = games.select(
    "game_id",
    F.col("home_team_id").alias("team_id"),
    F.col("visitor_team_id").alias("opponent_team_id"),
    "season",
    "season_type",
    "game_date",

    F.lit(True).alias("is_home"),

    F.col("home_team_score").alias("points_for"),
    F.col("visitor_team_score").alias("points_against"),

    (F.col("home_team_score") > F.col("visitor_team_score"))
        .cast("int").alias("win"),

    (F.col("home_team_score") < F.col("visitor_team_score"))
        .cast("int").alias("loss"),

    (F.col("home_team_score") - F.col("visitor_team_score"))
        .alias("point_diff"),

    "source_ingested_at"
)


# ---------------------------------------
# 4. Create visiting-team perspective
# ---------------------------------------

away_rows = games.select(
    "game_id",
    F.col("visitor_team_id").alias("team_id"),
    F.col("home_team_id").alias("opponent_team_id"),
    "season",
    "season_type",
    "game_date",

    F.lit(False).alias("is_home"),

    F.col("visitor_team_score").alias("points_for"),
    F.col("home_team_score").alias("points_against"),

    (F.col("visitor_team_score") > F.col("home_team_score"))
        .cast("int").alias("win"),

    (F.col("visitor_team_score") < F.col("home_team_score"))
        .cast("int").alias("loss"),

    (F.col("visitor_team_score") - F.col("home_team_score"))
        .alias("point_diff"),

    "source_ingested_at"
)


# ---------------------------------------
# 5. Combine both perspectives
# ---------------------------------------

team_game = home_rows.unionByName(away_rows)


# ---------------------------------------
# 6. Gold quality gate
# ---------------------------------------

metrics = team_game.agg(
    F.count("*").alias("total_rows"),

    F.countDistinct("game_id").alias("unique_games"),

    F.countDistinct("game_id", "team_id")
        .alias("unique_team_game_keys"),

    F.sum("win").alias("total_wins"),
    F.sum("loss").alias("total_losses"),

    F.sum(F.col("is_home").cast("int"))
        .alias("home_rows"),

    F.sum("points_for").alias("points_for_total"),
    F.sum("points_against").alias("points_against_total"),
    F.sum("point_diff").alias("net_point_diff")
).first().asDict()

print("Gold quality metrics:", metrics)

checks = {
    "exactly_two_rows_per_game":
        metrics["total_rows"] == 2460
        and metrics["unique_games"] == 1230,

    "unique_team_game_keys":
        metrics["unique_team_game_keys"] == 2460,

    "one_winner_and_loser_per_game":
        metrics["total_wins"] == 1230
        and metrics["total_losses"] == 1230,

    "one_home_team_per_game":
        metrics["home_rows"] == 1230,

    "balanced_scoring":
        metrics["points_for_total"]
        == metrics["points_against_total"],

    "zero_net_point_difference":
        metrics["net_point_diff"] == 0
}

for name, passed in checks.items():
    print(f"{name}: {'PASS' if passed else 'FAIL'}")

if not all(checks.values()):
    raise ValueError("Gold quality gate failed")

print("Gold quality gate PASSED")


# ---------------------------------------
# 7. MERGE into permanent Gold table
# ---------------------------------------

team_game.createOrReplaceTempView("gold_team_game_updates")

spark.sql("""
MERGE INTO nba_analytics.gold.team_game AS target

USING gold_team_game_updates AS source

ON target.game_id = source.game_id
AND target.team_id = source.team_id

WHEN MATCHED AND
    source.source_ingested_at > target.source_ingested_at
THEN UPDATE SET *

WHEN NOT MATCHED
THEN INSERT *
""")

print("Gold team-game MERGE completed!")

display(team_game.orderBy("game_date", "game_id", "team_id").limit(10))

