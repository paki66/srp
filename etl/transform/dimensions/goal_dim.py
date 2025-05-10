from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


def transform_goal_dim(psql_plays_df, csv_plays_df=None):
    psql_df = (
        psql_plays_df
        .select(
            col("yards_to_go"),
            col("down").cast("int")
        )
        .dropna()
        .dropDuplicates()
    )

    if csv_plays_df:
        csv_df = (
            csv_plays_df
            .select(
                "yards_to_go",
                "down"
            )
        )

        combined_df = psql_df.unionByName(csv_df).dropDuplicates(["yards_to_go", "down"])

    else:
        combined_df = psql_df

    window = Window.orderBy("yards_to_go", "down")
    combined_df = combined_df.withColumn("goal_tk", row_number().over(window))

    return combined_df.orderBy("yards_to_go", "down")