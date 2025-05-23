from pyspark.sql.functions import col, trim, upper, row_number
from pyspark.sql.window import Window
from etl.spark_session import get_spark_session

def transform_defense_dim(psql_defense_df, csv_defense_df=None):
    spark = get_spark_session()

    defense_lookup = (
        psql_defense_df.select(
            col("id").cast("long").alias("defense_id"),
            upper(trim(col("team_name"))).alias("defense_team_name"),
            col("score").alias("defense_score")
        )
    )

    if csv_defense_df:
        csv_df = (
            csv_defense_df
            .selectExpr("defense_team as defense_team_name", "defense_team_score as defense_score")
            .withColumn("defense_team_name", upper(trim(col("defense_team_name"))))
            .withColumn("defense_team_score", col("defense_score"))
            .dropDuplicates(["defense_team_name", "defense_score"])
        )

        defense_df = (
            csv_df.alias("csv")
            .join(
                defense_lookup.alias("db"),
                on=["defense_team_name", "defense_score"],
                how="left"
            )
            .select(
                col("db.defense_id"),
                col("csv.defense_team_name").alias("team"),
                col("csv.defense_score").alias("score")
            )
        )

    else:
        defense_df = spark.createDataFrame([], "defense_id long, team string, score float")

    window = Window.orderBy("team")
    final_df = (
        defense_df
        .dropDuplicates(["team", "score"])
        .withColumn("defense_tk", row_number().over(window))
        .select("defense_tk", "team", "score")
    )

    return final_df
