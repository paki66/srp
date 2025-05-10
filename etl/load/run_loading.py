from pyspark.sql import DataFrame


def write_spark_df_to_psql(spark_df: DataFrame, table_name: str, mode: str = "append"):
    jdbc_url = "jdbc:postgresql://localhost/srp_db?useSSL=false"
    connection_properties = {
        "user": "paolo_srp",
        "password": "paolo_srp",
        "driver": "org.postgresql.Driver"
    }

    print(f"Writing to table `{table_name}` with mode `{mode}`...")
    spark_df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode=mode,
        properties=connection_properties
    )
    print(f"✅ Done writing to `{table_name}`.")