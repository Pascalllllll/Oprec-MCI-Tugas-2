"""
DAG Airflow untuk pipeline orders real-time.

Alurnya:
  http://96.9.212.102:8000/orders
      → fetch_orders.py (simpan parquet ke data_lake/orders/)
      → process_orders_spark.py (agregat top 30 produk → ClickHouse)
      → divisualisasikan di Metabase.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "mmds_engineer",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "orders_realtime_pipeline",
    default_args=default_args,
    schedule_interval="*/10 * * * *", # Setiap 10 menit
    catchup=False,
    max_active_runs=1,
    description="Micro-batching Orders API → Spark → ClickHouse",
) as dag:

    fetch_step = BashOperator(
        task_id="fetch_orders",
        bash_command="python /opt/airflow/dags/scripts/fetch_orders.py",
    )

    process_step = BashOperator(
        task_id="process_top_products_spark",
        bash_command="python /opt/airflow/dags/scripts/process_orders_spark.py",
    )

    fetch_step >> process_step