# extract/extract_mysql.py
from etl.spark_session import get_spark_session

def extract_table(table_name):
    spark = get_spark_session("ETL_App")

    jdbc_url = "jdbc:postgresql://localhost/srp_db?useSSL=false"
    connection_properties = {
        "user": "paolo_srp",
        "password": "paolo_srp",
        "driver": "org.postgresql.Driver"
    }

    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
    return df

def extract_all_tables():
    return {
        "season": extract_table("season"),
        "game": extract_table("game"),
        "quarter": extract_table("quarter"),
        "offense": extract_table("offense"),
        "defense": extract_table("defense"),
        "play_type": extract_table("play_type"),
        "plays": extract_table("plays"),
    }