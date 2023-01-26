from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("prueba", start_date=datetime(2021, 1 ,1), 
    schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    
    prueba_spark = SparkSubmitOperator(
            task_id="prueba",
            application="${SPARK_HOME}/examples/src/main/python/pi.py",
            conn_id="spark_conn",
            verbose=False
        )

    prueba_python = PythonOperator(
        task_id="downloading_rates",
        python_callable=print('prueba')
    )

    prueba_spark >> prueba_python