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
EXPECTED_GAMES = 1230
EXPECTED_TEAM_GAMES = EXPECTED_GAMES * 2

print("--------------------------------")
print("NBA Gold Team-Game Transformation")
print("--------------------------------")
print(f"Season: {SEASON}-{str(SEASON + 1)[-2:]}")
print(f"Season type: {SEASON_TYPE}")


# ---------------------------------------
# 2. Read Silver Games and Teams
# ---------------------------------------

games = (
    spark.table("nba_analytics.silver.games")
    .filter(
        (F.col("season") == SEASON)
        & (F.col("season_type") == SEASON_TYPE)
    )
)

teams = (
    spark.table("nba_analytics.silver.teams")
    .select("team_id")
)

silver_games = games.count()
unique_games = games.select("game_id").distinct().count()

print("Silver games:", silver_games)
print("Unique game IDs:", unique_games)

if silver_games != EXPECTED_GAMES:
    raise ValueError(
        f"Expected {EXPECTED_GAMES} Silver games "
        f"for season {SEASON}, found {silver_games}"
    )

if unique_games != silver_games:
    raise ValueError("Duplicate Silver game IDs detected")

if teams.count() != 30:
    raise ValueError("Expected 30 Silver teams")

if teams.distinct().count() != 30:
    raise ValueError("Duplicate Silver team IDs detected")


# ---------------------------------------
# 3. Validate source game records
# ---------------------------------------

is_final = F.coalesce(
    F.upper(F.trim(F.col("status"))).startswith("FINAL"),
    F.lit(False)
)

invalid_games = games.filter(
    ~is_final
    | F.col("game_id").isNull()
    | F.col("game_date").isNull()
    | F.col("home_team_id").isNull()
    | F.col("visitor_team_id").isNull()
    | F.col("source_ingested_at").isNull()
    | (F.col("home_team_id") == F.col("visitor_team_id"))
    | F.col("home_team_score").isNull()
    | F.col("visitor_team_score").isNull()
    | (F.col("home_team_score") < 0)
    | (F.col("visitor_team_score") < 0)
    | (
        F.col("home_team_score")
        == F.col("visitor_team_score")
    )
    | (~F.year("game_date").isin(SEASON, SEASON + 1))
).count()

if invalid_games > 0:
    raise ValueError(
        f"Found {invalid_games} invalid Silver games"
    )


# ---------------------------------------
# 4. Check Games-to-Teams relationships
# ---------------------------------------

game_team_ids = (
    games
    .select(F.col("home_team_id").alias("team_id"))
    .unionByName(
        games.select(
            F.col("visitor_team_id").alias("team_id")
        )
    )
    .distinct()
)

missing_teams = (
    game_team_ids
    .join(teams, on="team_id", how="left_anti")
)

missing_count = missing_teams.count()

print("Unmatched team IDs:", missing_count)

if missing_count > 0:
    display(missing_teams)
    raise ValueError(
        "Games reference teams missing from Silver Teams"
    )

print("Source data validation: PASS")


# ---------------------------------------
# 5. Create home-team perspective
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

    (
        F.col("home_team_score")
        > F.col("visitor_team_score")
    ).cast("int").alias("win"),

    (
        F.col("home_team_score")
        < F.col("visitor_team_score")
    ).cast("int").alias("loss"),

    (
        F.col("home_team_score")
        - F.col("visitor_team_score")
    ).alias("point_diff"),

    "source_ingested_at"
)


# ---------------------------------------
# 6. Create visiting-team perspective
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

    (
        F.col("visitor_team_score")
        > F.col("home_team_score")
    ).cast("int").alias("win"),

    (
        F.col("visitor_team_score")
        < F.col("home_team_score")
    ).cast("int").alias("loss"),

    (
        F.col("visitor_team_score")
        - F.col("home_team_score")
    ).alias("point_diff"),

    "source_ingested_at"
)


# ---------------------------------------
# 7. Combine both perspectives
# ---------------------------------------

team_game = home_rows.unionByName(away_rows)


# ---------------------------------------
# 8. Run Gold quality checks
# ---------------------------------------

metrics = team_game.agg(
    F.count("*").alias("total_rows"),

    F.countDistinct("game_id").alias("unique_games"),

    F.countDistinct("game_id", "team_id")
        .alias("unique_team_game_keys"),

    F.countDistinct("team_id").alias("unique_teams"),

    F.sum("win").alias("total_wins"),
    F.sum("loss").alias("total_losses"),

    F.sum(F.col("is_home").cast("int"))
        .alias("home_rows"),

    F.sum("points_for").alias("points_for_total"),

    F.sum("points_against")
        .alias("points_against_total"),

    F.sum("point_diff").alias("net_point_diff")
).first().asDict()


# Additional check: validate EACH game,
# not just the season-wide totals.

per_game = team_game.groupBy("game_id").agg(
    F.count("*").alias("row_count"),

    F.countDistinct("team_id")
        .alias("team_count"),

    F.sum(F.col("is_home").cast("int"))
        .alias("home_count"),

    F.sum("win").alias("wins"),
    F.sum("loss").alias("losses"),

    F.sum("point_diff").alias("net_point_diff")
)

invalid_game_pairs = per_game.filter(
    (F.col("row_count") != 2)
    | (F.col("team_count") != 2)
    | (F.col("home_count") != 1)
    | (F.col("wins") != 1)
    | (F.col("losses") != 1)
    | (F.col("net_point_diff") != 0)
).count()


checks = {
    "expected_team_game_rows":
        metrics["total_rows"] == EXPECTED_TEAM_GAMES,

    "expected_unique_games":
        metrics["unique_games"] == EXPECTED_GAMES,

    "unique_team_game_keys":
        metrics["unique_team_game_keys"]
        == EXPECTED_TEAM_GAMES,

    "30_participating_teams":
        metrics["unique_teams"] == 30,

    "one_win_and_loss_per_game":
        metrics["total_wins"] == EXPECTED_GAMES
        and metrics["total_losses"] == EXPECTED_GAMES,

    "one_home_team_per_game":
        metrics["home_rows"] == EXPECTED_GAMES,

    "balanced_points":
        metrics["points_for_total"]
        == metrics["points_against_total"],

    "zero_net_point_difference":
        metrics["net_point_diff"] == 0,

    "valid_individual_game_pairs":
        invalid_game_pairs == 0
}


print("\n----- GOLD QUALITY REPORT -----")
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
        f"Gold quality gate FAILED: {failures}"
    )

print("\nGold quality gate PASSED")


# ---------------------------------------
# 9. Create temporary SQL view
# ---------------------------------------

team_game.createOrReplaceTempView(
    "gold_team_game_updates"
)


# ---------------------------------------
# 10. MERGE into Gold fact table
# ---------------------------------------

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

print("Gold Team-Game MERGE completed!")


# ---------------------------------------
# 11. Verify the persisted season
# ---------------------------------------

persisted = (
    spark.table("nba_analytics.gold.team_game")
    .filter(
        (F.col("season") == SEASON)
        & (F.col("season_type") == SEASON_TYPE)
    )
)

persisted_rows = persisted.count()

persisted_keys = (
    persisted
    .select("game_id", "team_id")
    .distinct()
    .count()
)

print("Persisted rows:", persisted_rows)
print("Persisted unique keys:", persisted_keys)

if (
    persisted_rows != EXPECTED_TEAM_GAMES
    or persisted_keys != EXPECTED_TEAM_GAMES
):
    raise ValueError(
        "Post-MERGE Gold validation failed"
    )

print("Post-MERGE Gold validation PASSED")


# ---------------------------------------
# 12. Display sample records
# ---------------------------------------

display(
    persisted
    .orderBy("game_date", "game_id", "team_id")
    .limit(10)
)

print(
    f"\nSeason {SEASON}-{str(SEASON + 1)[-2:]} "
    "Gold transformation completed!"
)
