# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql.window import Window


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

print("--------------------------------")
print("NBA Silver Games Transformation")
print("--------------------------------")
print(f"Season: {SEASON}-{str(SEASON + 1)[-2:]}")
print(f"Season type: {SEASON_TYPE}")
print(f"Expected games: {EXPECTED_GAMES}")


# ---------------------------------------
# 2. Read the Bronze table
# ---------------------------------------

bronze_df = (
    spark.table("nba_analytics.bronze.games_raw")
    .filter(
        (F.col("season") == SEASON)
        & (F.col("season_type") == SEASON_TYPE)
    )
)

bronze_pages = bronze_df.count()

print(f"\nBronze pages found: {bronze_pages}")

if bronze_pages == 0:
    raise ValueError(
        f"No Bronze records found for season {SEASON}"
    )


# ---------------------------------------
# 3. Define the expected JSON structure
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
# 4. Parse Bronze JSON
# ---------------------------------------

parsed_df = bronze_df.select(
    "batch_id",
    "ingested_at",
    "records_returned",
    "season_type",
    F.sha2("payload", 256).alias("page_hash"),
    F.from_json("payload", GAME_SCHEMA).alias("body")
)

# Verify that each API page was parsed
# and contains its reported number of games.

invalid_pages = parsed_df.filter(
    F.col("body.data").isNull()
    | F.col("records_returned").isNull()
    | (
        F.size("body.data")
        != F.col("records_returned")
    )
).count()

if invalid_pages > 0:
    raise ValueError(
        f"Found {invalid_pages} invalid Bronze pages"
    )

print("Bronze JSON validation: PASS")


# ---------------------------------------
# 5. Explode the game arrays
# ---------------------------------------

exploded_df = parsed_df.select(
    "batch_id",
    "ingested_at",
    "season_type",
    "page_hash",
    F.explode("body.data").alias("game")
)


# ---------------------------------------
# 6. Flatten and type game fields
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
    "page_hash",
    "batch_id"
)

raw_games = games_df.count()

print(f"Raw game records: {raw_games}")

# Validate IDs before deduplication.
# Otherwise, multiple null IDs could collapse
# into a single row and hide source errors.

null_game_ids = games_df.filter(
    F.col("game_id").isNull()
).count()

if null_game_ids > 0:
    raise ValueError(
        f"Found {null_game_ids} null game IDs in Bronze"
    )


# ---------------------------------------
# 7. Deduplicate by latest ingestion
# ---------------------------------------

game_window = (
    Window
    .partitionBy("game_id")
    .orderBy(
        F.col("source_ingested_at").desc(),
        F.col("page_hash").desc(),
        F.col("batch_id").desc()
    )
)

silver_preview = (
    games_df
    .withColumn(
        "_row_number",
        F.row_number().over(game_window)
    )
    .filter(F.col("_row_number") == 1)
    .drop(
        "_row_number",
        "page_hash",
        "batch_id"
    )
)

unique_games = silver_preview.count()

print(f"Unique Silver games: {unique_games}")


# ---------------------------------------
# 8. Define automated quality gate
# ---------------------------------------

def validate_silver_games(df, expected_games):
    """
    Validate a completed NBA regular season.

    Raises ValueError when a critical
    data-quality check fails.
    """

    final_game = F.coalesce(
        F.upper(
            F.trim(F.col("status"))
        ).startswith("FINAL"),
        F.lit(False)
    )

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
        (F.col("season") != SEASON)
        | (F.col("season_type") != SEASON_TYPE)
        | (
            ~F.year("game_date").isin(
                SEASON,
                SEASON + 1
            )
        )
    )

    metrics = df.agg(
        F.count("*").alias("total_games"),

        F.countDistinct("game_id")
            .alias("unique_games"),

        F.count(
            F.when(missing_required, 1)
        ).alias("missing_required_fields"),

        F.count(
            F.when(
                F.col("home_team_id")
                == F.col("visitor_team_id"),
                1
            )
        ).alias("same_team_errors"),

        F.count(
            F.when(
                final_game & invalid_final_score,
                1
            )
        ).alias("invalid_final_scores"),

        F.count(
            F.when(wrong_season, 1)
        ).alias("invalid_season_records")

    ).first().asDict()

    checks = {
        "expected_game_count":
            metrics["total_games"] == expected_games,

        "unique_game_ids":
            metrics["unique_games"]
            == metrics["total_games"],

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
        result = "PASS" if passed else "FAIL"
        print(f"{check}: {result}")

    print("\nMetrics:", metrics)

    failures = [
        name
        for name, passed in checks.items()
        if not passed
    ]

    if failures:
        raise ValueError(
            f"Silver quality gate FAILED: {failures}"
        )

    print("\nSilver quality gate PASSED")

    return metrics


# ---------------------------------------
# 9. Validate BEFORE writing to Silver
# ---------------------------------------

validate_silver_games(
    silver_preview,
    expected_games=EXPECTED_GAMES
)


# ---------------------------------------
# 10. Create temporary SQL view
# ---------------------------------------

silver_preview.createOrReplaceTempView(
    "silver_games_updates"
)


# ---------------------------------------
# 11. MERGE validated records into Silver
# ---------------------------------------

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

print("\nSilver MERGE completed successfully!")


# ---------------------------------------
# 12. Display sample results
# ---------------------------------------

display(
    silver_preview
    .orderBy("game_date", "game_id")
    .limit(10)
)

print(
    f"\nSeason {SEASON}-{str(SEASON + 1)[-2:]} "
    "transformation completed!"
)

