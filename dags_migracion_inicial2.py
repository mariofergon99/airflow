from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
import boto3
import yaml

bucket = 'config'
key = 'config_dagsMigracion.yaml'
s3_client = boto3.client('s3', 
    aws_access_key_id = 'inditedge',            #<------ Ponerlo como secreto
    aws_secret_access_key = 'inditedge', 
    endpoint_url = 'http://minio.default.svc:9000', 
    region_name = 'eu-south-2')

obj = s3_client.get_object(Bucket=bucket, Key=key)
cf = yaml.safe_load(obj['Body'].read())

## TODO: ##
#   - Default args, ver cuales mas pueden ser interesantes
#   - Squedule args 
#   - DAG args
#   - Path del config file
#   - Parametrizar funcion de lectura mysql y escritura en kafka
#   - Configuracion de la conexion con mysql
#   - Query/s deseadas
#   - Configuracion producer
#   - Spark Operator args para Hudi Streaming

default_args = {                                                                   ## <-----
    "owner": cf['airflow']['owner'],
    "email_on_failure": cf['airflow']['email_on_failure'],
    "email_on_retry": cf['airflow']['email_on_retry'],
    "email": cf['airflow']['email'],
    "retries": cf['airflow']['retries'],
    "retry_delay": timedelta(minutes=cf['airflow']['retry_delay'])
}

def mySQL_to_Kafka(cf):   
    from kafka import KafkaProducer
    from json import dumps
    import mysql.connector
    import pandas as pd

    ## Lectura de MySQL ## 
    # - Conexion a la base de datos
    mysql_db_conn = mysql.connector.connect(                                      ## <-----
        user = cf['mySQL']['user'], 
        database = cf['mySQL']['database'],
        password = cf['mySQL']['password'],
        host = cf['mySQL']['host'],
        port = cf['mySQL']['port'])
    
    cursor = mysql_db_conn.cursor()

    # - Realización de la consulta
    # - Paginar los datos
    # num_entries_pd = pd.read_sql('SELECT ID FROM monitoring_system_events ORDER BY ID DESC LIMIT 1', con=mysql_db_conn)
    # num_entries = num_entries_pd['ID'][0]
    # for :
    query = str('select * from monitoring_system_events')                         ## <-----  
    data_mysql_pd = pd.read_sql(query, con=mysql_db_conn)
    mysql_db_conn.close()

    # - Procesado de los datos
    data_mysql_json = data_mysql_pd.to_dict(orient="records")
    
    ## Escritura a Kafka ##
    # - Enviar datos con producer -> 1 a 1 o todo de golpe?
    # - Configurar producer
    producer = KafkaProducer(
        bootstrap_servers=[cf['kafka']['bootstrap-servers']],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    for event_data in data_mysql_json:
        producer.send(cf['kafka']['write-topic'], value = event_data)

    # mysql_db_conn.close()

with DAG(dag_id='Migracion_inicial', start_date=datetime(2021, 1 ,1), schedule_interval=cf['airflow']['schedule_interval'], default_args=default_args, description='Migracion inicial de datos históricos', tags=['mysql', 'kafka', 'spark'], catchup=False) as dag:
    
    mySQL_to_Kafka_DAG = PythonOperator(
        task_id="mySQL_to_Kafka",
        python_callable=mySQL_to_Kafka,
        op_args=[cf]
    )

    Kafka_to_Iceberg_DAG = SparkSubmitOperator(
        task_id="Kafka_to_Iceberg",
        application="/opt/airflow/dags/code/kafkaToIceberg.py",    # <----
        conn_id="spark_conn",
        verbose=False,
        packages= 'org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.1.0, org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0', #
        executor_cores= 1,
        driver_memory= '10g'
    )

    mySQL_to_Kafka_DAG >> Kafka_to_Iceberg_DAG