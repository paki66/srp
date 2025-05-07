from pyspark.sql.functions import col, trim, initcap, lit, rand, when, row_number, current_timestamp
from pyspark.sql.window import Window
from etl.spark_session import get_spark_session


def transform_game_dim(game_df, season_df, csv_plays_df=None):
    gm = game_df.alias('gm')
    s = season_df.alias('s')

    merged_df = (
        gm.join(s, col("s.id") == col("gm.season_fk"), how='left')
        .select(
            col("gm.id").alias("game_id"),
            trim(col("s.year")).alias("year"),
            trim(col("gm.home_team")).alias("home_team"),
            trim(col("gm.away_team")).alias("away_team")
        )
        .withColumn("game_id", initcap(trim(col("game_id"))))
        .withColumn("year", initcap(trim(col("year"))))
        .withColumn("home_team", initcap(trim(col("home_team"))))
        .withColumn("away_team", initcap(trim(col("away_team"))))
    )

    if csv_plays_df:
        csv_game_df = (
            csv_plays_df
            .selectExpr("GameID as game_id", "HomeTeam as home_team", "AwayTeam as away_team", "Season as year")
            .withColumn("game_id", initcap(trim(col("game_id"))))
            .withColumn("year", initcap(trim(col("year"))))
            .withColumn("home_team", initcap(trim(col("home_team"))))
            .withColumn("away_team", initcap(trim(col("away_team"))))
            .dropDuplicates("game_id")
        )

        merged_df = (merged_df.select("game_id", "year", "home_team", "away_team")
                     .unionByName(csv_game_df)
                     .dropDuplicates("game_id"))

    window = Window.orderBy("game_id")
    merged_df = merged_df.withColumn("game_tk", row_number().over(window))

    final_df = merged_df.select("game_tk", "game_id", "year", "home_team", "away_team")

    return final_df