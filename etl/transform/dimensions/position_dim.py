from pyspark.sql.functions import col, trim
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def transform_position_dim(psql_plays_df, csv_plays_df=None):
    psql_df = (
        psql_plays_df
        .select(
            col("side_of_field"),
            col("yard_line").cast("int")
        )
        .dropna()
        .dropDuplicates()
    )

    if csv_plays_df:
        csv_df = (
            csv_plays_df
            .select(
                "side_of_field",
                "yard_line"
            )
        )

        combined_df = psql_df.unionByName(csv_df).dropDuplicates(["side_of_field", "yard_line"])

    else:
        combined_df = psql_df

    window = Window.orderBy("side_of_field", "yard_line")
    combined_df = combined_df.withColumn("position_tk", row_number().over(window))

    return combined_df.orderBy("side_of_field", "yard_line")