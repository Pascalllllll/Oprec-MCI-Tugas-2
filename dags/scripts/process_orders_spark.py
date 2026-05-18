"""
Task 2: Baca semua parquet di data lake, agregat semua produk yang muncul, lalu muat ke ClickHouse.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from clickhouse_driver import Client
import os
import glob

def run_spark_analytics():
    spark = SparkSession.builder \
        .appName("Orders_Products_Analytics") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    print("Membaca seluruh aliran data dari data lake...")
    df_raw = spark.read.parquet("file:///opt/airflow/data_lake/orders/")

    print("Mengagregat seluruh produk yang muncul di snapshot...")
    all_products = df_raw.groupBy("product_name", "department") \
        .agg(
            F.count("*").alias("total_orders"),
            F.sum("reordered").alias("reorder_count"),
            F.countDistinct("user_id").alias("unique_users")
        ) \
        .orderBy(F.desc("total_orders"))

    final_results = all_products.toPandas()
    spark.stop()

    print(f"Memuat {len(final_results)} produk ke ClickHouse...")
    client = Client(
        host="clickhouse-server",
        user="admin",
        password="rahasia"
    )

    client.execute("CREATE DATABASE IF NOT EXISTS analytics")
    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.orders_top_products (
            product_name   String,
            department     String,
            total_orders   Int32,
            reorder_count  Int32,
            unique_users   Int32
        ) ENGINE = MergeTree()
        ORDER BY total_orders
    """)

    client.execute("TRUNCATE TABLE analytics.orders_top_products")
    data_tuples = [tuple(x) for x in final_results.to_numpy()]
    if data_tuples:
        client.execute("INSERT INTO analytics.orders_top_products VALUES", data_tuples)

    print("Membersihkan parquet lama dari data lake...")
    files = glob.glob("/opt/airflow/data_lake/orders/*.parquet")
    for f in files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Gagal hapus {f}: {e.strerror}")

    print(f"✅ Pipeline selesai. {len(data_tuples)} produk tersimpan di ClickHouse.")

if __name__ == "__main__":
    run_spark_analytics()