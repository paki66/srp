from pyspark.sql import Window
from pyspark.sql.functions import row_number
from etl.spark_session import get_spark_session


def transform_time_dim():
    spark = get_spark_session()

    nums1 = spark.range(1, 5).toDF("quarter")
    nums2 = spark.range(0, 16).toDF("time_under")

    window = Window.orderBy("quarter", "time_under")
    complete_df = (
        nums1.crossJoin(nums2)
        .withColumn("time_tk"), row_number().over(window)
        .orderBy("quarter", "time_tk")
    )

    return complete_df
