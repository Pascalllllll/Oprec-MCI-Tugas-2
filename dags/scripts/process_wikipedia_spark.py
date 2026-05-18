from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from clickhouse_driver import Client
import os
import glob

def run_spark_analytics():
    spark = SparkSession.builder \
        .appName("Orders_Analytics") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    print("Membaca data dari Data Lake (Orders)...")
    df_raw = spark.read.parquet("file:///opt/airflow/data_lake/orders/")

    print("Melakukan Agregasi Data...")
    summary_df = df_raw.groupBy("order_hour_of_day") \
        .agg(
            F.count("order_id").alias("total_orders")
        ) \
        .orderBy("order_hour_of_day")

    final_results = summary_df.toPandas()
    spark.stop()

    print("Memuat ke ClickHouse Warehouse...")
    client = Client(host='clickhouse-server', user='admin', password='rahasia')

    client.execute('CREATE DATABASE IF NOT EXISTS analytics')
    
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.orders_by_hour (
            order_hour_of_day Int32,
            total_orders Int32
        ) ENGINE = MergeTree()
        ORDER BY order_hour_of_day
    ''')
    
    client.execute('TRUNCATE TABLE analytics.orders_by_hour')
    data_tuples = [tuple(x) for x in final_results.to_numpy()]
    if data_tuples:
        client.execute('INSERT INTO analytics.orders_by_hour VALUES', data_tuples)
    
    print("Membersihkan file Parquet lama...")
    files = glob.glob('/opt/airflow/data_lake/orders/*.parquet')
    for f in files:
        try:
            os.remove(f)
        except OSError:
            pass
            
    print("✅ Pipeline Orders Selesai!")

if __name__ == "__main__":
    run_spark_analytics()