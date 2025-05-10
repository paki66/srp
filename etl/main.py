# main.py
from extract.extract_psql import extract_all_tables
from extract.extract_csv import extract_from_csv
from transform.pipeline import run_transformations
from spark_session import get_spark_session
from load.run_loading import write_spark_df_to_psql
import os

# Unset SPARK_HOME if it exists to prevent Spark session conflicts
os.environ.pop("SPARK_HOME", None)

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    spark.catalog.clearCache()

    # Load data
    print("🚀 Starting data extraction")
    psql_df = extract_all_tables()
    csv_df = {"csv_plays":extract_from_csv("../NFL_dataset20_processed.csv")}
    merged_df = {**psql_df, **csv_df}
    print("✅ Data extraction completed")

    # Transform data
    print("🚀 Starting data transformation")
    load_ready_dict = run_transformations(merged_df)
    print("✅ Data transformation completed")

    # Load data
    print("🚀 Starting data loading")
    for table_name, df in load_ready_dict.items():
        write_spark_df_to_psql(df, table_name)
    print("👏 Data loading completed")

if __name__ == "__main__":
    main()