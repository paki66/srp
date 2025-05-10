from pyspark.sql.functions import col, trim, upper, row_number
from pyspark.sql.window import Window
from etl.spark_session import get_spark_session

def transform_offense_dim(psql_offense_df, csv_offense_df=None):
    spark = get_spark_session()

    offense_lookup = (
        psql_offense_df.select(
            col("id").cast("long").alias("offense_id"),
            upper(trim(col("team_name"))).alias("offense_team_name"),
            col("score").alias("offense_score")
        )
    )

    if csv_offense_df:
        csv_df = (
            csv_offense_df
            .selectExpr("offense_team as offense_team_name", "offense_team_score as offense_score")
            .withColumn("offense_team_name", upper(trim(col("offense_team_name"))))
            .withColumn("offense_team_score", col("offense_score"))
            .dropDuplicates(["offense_team_name", "offense_score"])
        )

        offense_df = (
            csv_df.alias("csv")
            .join(
                offense_lookup.alias("db"),
                on=["offense_team_name", "offense_score"],
                how="left"
            )
            .select(
                col("db.offense_id"),
                col("csv.offense_team_name").alias("team"),
                col("csv.offense_score").alias("score")
            )
        )

    else:
        offense_df = spark.createDataFrame([], "offense_id long, team string, score float")

    window = Window.orderBy("team")
    final_df = (
        offense_df
        .dropDuplicates(["team", "score"])
        .withColumn("offense_tk", row_number().over(window))
        .select("offense_tk", "team", "score")
    )

    return final_df
