
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
# 8. Persist validated games into Silver
# ---------------------------------------

# Make the DataFrame accessible to Spark SQL
silver_preview.createOrReplaceTempView(
    "silver_games_updates"
)

# Insert new games; update existing games
# only when we have a newer ingested snapshot
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

print("Silver games MERGE completed successfully!")

