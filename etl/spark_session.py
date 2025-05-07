# spark_session.py
from pyspark.sql import SparkSession

# Lazy-load pattern: Spark is only created when this function is called
def get_spark_session(app_name="ETL_App"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "Connectors/postgresql-42.7.5.jar") \
        .getOrCreate()