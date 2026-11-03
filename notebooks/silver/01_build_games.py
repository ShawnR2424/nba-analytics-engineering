
# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ---------------------------------------
# 1. Read the Bronze table
# ---------------------------------------

bronze_df = (
    spark.table("nba_analytics.bronze.games_raw")
    .filter(
        (F.col("season") == 2024) &
        (F.col("season_type") == "regular") &
        (F.col("per_page") == 50)
    )
)


# ---------------------------------------
# 2. Define the expected JSON structure
# ---------------------------------------

GAME_SCHEMA = """
STRUCT<
    data: ARRAY<STRUCT<
        id: BIGINT,
        date: STRING,
        season: INT,
        status: STRING,
        home_team_score: INT,
        visitor_team_score: INT,
        home_team: STRUCT<id: INT>,
        visitor_team: STRUCT<id: INT>
    >>
>
"""


# ---------------------------------------
# 3. Parse the raw JSON
# ---------------------------------------

parsed_df = bronze_df.select(
    "batch_id",
    "ingested_at",
    "season_type",
    F.sha2("payload", 256).alias("page_hash"),
    F.from_json("payload", GAME_SCHEMA).alias("body")
)

if parsed_df.filter(F.col("body").isNull()).count() > 0:
    raise ValueError("One or more Bronze JSON payloads could not be parsed")


# ---------------------------------------
# 4. Explode the game arrays
# ---------------------------------------

exploded_df = parsed_df.select(
    "batch_id",
    "ingested_at",
    "season_type",
    "page_hash",
    F.explode("body.data").alias("game")
)


# ---------------------------------------
# 5. Select and type individual fields
# ---------------------------------------

games_df = exploded_df.select(
    F.col("game.id").alias("game_id"),
    F.col("game.season").alias("season"),
    F.col("season_type"),
    F.to_date("game.date").alias("game_date"),
    F.col("game.status").alias("status"),

    F.col("game.home_team.id").alias("home_team_id"),
    F.col("game.visitor_team.id").alias("visitor_team_id"),

    F.col("game.home_team_score").alias("home_team_score"),
    F.col("game.visitor_team_score").alias("visitor_team_score"),

    F.col("ingested_at").alias("source_ingested_at"),
    F.col("page_hash"),
    F.col("batch_id")
)


# ---------------------------------------
# 6. Keep the latest record per game
# ---------------------------------------

game_window = Window.partitionBy("game_id").orderBy(
    F.col("source_ingested_at").desc(),
    F.col("page_hash").desc(),
    F.col("batch_id").desc()
)

silver_preview = (
    games_df
    .withColumn("_row_number", F.row_number().over(game_window))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number", "page_hash", "batch_id")
)


# ---------------------------------------
# 7. Validate the transformation
# ---------------------------------------

bronze_pages = bronze_df.count()
raw_games = games_df.count()
unique_games = silver_preview.count()

invalid_games = silver_preview.filter(
    F.col("game_id").isNull() |
    F.col("game_date").isNull() |
    F.col("home_team_id").isNull() |
    F.col("visitor_team_id").isNull() |
    (F.col("home_team_id") == F.col("visitor_team_id"))
).count()

print("Bronze pages:", bronze_pages)
print("Raw games:", raw_games)
print("Unique Silver games:", unique_games)
print("Invalid game records:", invalid_games)

if unique_games != 1230 or invalid_games != 0:
    raise ValueError("Silver validation failed")

display(silver_preview.orderBy("game_date").limit(10))

# ---------------------------------------
# 8. Silver quality gate + Delta MERGE
# ---------------------------------------

from pyspark.sql import functions as F


def validate_silver_games(df, expected_games=1230):
    """
    Validate the completed 2024-25 regular season.
    Raise ValueError if a critical check fails.
    """

    final_game = F.upper(
        F.trim(F.col("status"))
    ).startswith("FINAL")

    missing_required = (
        F.col("game_id").isNull()
        | F.col("season").isNull()
        | F.col("season_type").isNull()
        | F.col("game_date").isNull()
        | F.col("status").isNull()
        | F.col("home_team_id").isNull()
        | F.col("visitor_team_id").isNull()
        | F.col("source_ingested_at").isNull()
    )

    invalid_final_score = (
        F.col("home_team_score").isNull()
        | F.col("visitor_team_score").isNull()
        | (F.col("home_team_score") < 0)
        | (F.col("visitor_team_score") < 0)
        | (
            F.col("home_team_score")
            == F.col("visitor_team_score")
        )
    )

    wrong_season = (
        (F.col("season") != 2024)
        | (F.col("season_type") != "regular")
        | (~F.year("game_date").isin(2024, 2025))
    )

    # Calculate all metrics in one aggregation
    metrics = df.agg(
        F.count("*").alias("total_games"),
        F.countDistinct("game_id").alias("unique_games"),

        F.count(
            F.when(missing_required, 1)
        ).alias("missing_required_fields"),

        F.count(
            F.when(
                F.col("home_team_id")
                == F.col("visitor_team_id"), 1
            )
        ).alias("same_team_errors"),

        F.count(
            F.when(final_game & invalid_final_score, 1)
        ).alias("invalid_final_scores"),

        F.count(
            F.when(wrong_season, 1)
        ).alias("invalid_season_records")

    ).first().asDict()

    # Define the pass/fail conditions
    checks = {
        "expected_game_count":
            metrics["total_games"] == expected_games,

        "unique_game_ids":
            metrics["unique_games"] == metrics["total_games"],

        "required_fields":
            metrics["missing_required_fields"] == 0,

        "different_teams":
            metrics["same_team_errors"] == 0,

        "valid_final_scores":
            metrics["invalid_final_scores"] == 0,

        "correct_season":
            metrics["invalid_season_records"] == 0
    }

    print("\n----- SILVER QUALITY REPORT -----")

    for check, passed in checks.items():
        print(f"{check}: {'PASS' if passed else 'FAIL'}")

    print("\nMetrics:", metrics)

    failures = [
        name for name, passed in checks.items()
        if not passed
    ]

    if failures:
        raise ValueError(
            f"Silver quality gate FAILED: {failures}"
        )

    print("\nSilver quality gate PASSED")
    return metrics


# ---------------------------------------
# 9. Validate BEFORE writing
# ---------------------------------------

validate_silver_games(silver_preview)


# ---------------------------------------
# 10. Persist validated data
# ---------------------------------------

silver_preview.createOrReplaceTempView(
    "silver_games_updates"
)

spark.sql("""
MERGE INTO nba_analytics.silver.games AS target

USING silver_games_updates AS source

ON target.game_id = source.game_id

WHEN MATCHED AND
    source.source_ingested_at > target.source_ingested_at
THEN UPDATE SET *

WHEN NOT MATCHED
THEN INSERT *
""")

print("Silver MERGE completed successfully!")
