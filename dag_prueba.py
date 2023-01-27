from airflow import DAG
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

def dummy():
    print("yes")

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("prueba_Bash", start_date=datetime(2021, 1 ,1), 
    schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    
    # prueba_spark = SparkSubmitOperator(
    #         task_id="prueba_spark",
    #         application="/home/airflow/.local/lib/python3.7/site-packages/pyspark/examples/src/main/python/pi.py",
    #         conn_id="spark_conn",
    #         verbose=False
    #     )

    prueba_python = PythonOperator(
        task_id="prueba_python",
        python_callable=dummy
    )

    prueba_bash = BashOperator(
        task_id="prueba_bash",
        bash_command="""
            curl -X POST -H "Content-Type: application/json" -d 
            '{ "appResource": "/home/airflow/.local/lib/python3.7/site-packages/pyspark/examples/src/main/python/pi.py", 
                "sparkProperties": 
                    {"spark.driver.memory": "1g",
                    "spark.master": "spark://spark-master-svc.default.svc:7077",
                    "spark.app.name": "Prueba bash operator con API",
                    "spark.submit.deployMode": "cluster",
                    "spark.ui.showConsoleProgress": "true",
                    "spark.eventLog.enabled": "false",
                    "spark.logConf": "false",
                    "spark.executor.cores":"1",
                    "spark.cores.max":"1",
                    "spark.driver.bindAddress": "0.0.0.0",
                    }, 
                "clientSparkVersion": "3.3.0", 
                "mainClass": "org.apache.spark.deploy.SparkSubmit",
                "environmentVariables": { "SPARK_YARN_MODE": true}, 
                "action": "CreateSubmissionRequest",
                "appArgs": ["/home/airflow/.local/lib/python3.7/site-packages/pyspark/examples/src/main/python/pi.py"]
            }' 
            http://spark.api/v1/submissions/create
        """
    )

    prueba_bash >> prueba_python