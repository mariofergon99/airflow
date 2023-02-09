from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
import boto3
import yaml

bucket = 'config'
key = 'config_dagsSimulacion.yaml'
s3_client = boto3.client('s3', 
                aws_access_key_id = 'inditedge',            #<------ Ponerlo como secreto
                aws_secret_access_key = 'inditedge', 
                endpoint_url = 'minio.default.svc:9000', 
                region_name = 'eu-south-2')

obj = s3_client.get_object(Bucket=bucket, Key=key)
cf = yaml.safe_load(obj['Body'].read())

def mySQL_writer(cf):
    from datetime import datetime
    from random import randint, choice
    import mysql.connector
    import string
    
    ## Escritura en MySQL ## 
    # - Conexion a la base de datos
    mysql_db_conn = mysql.connector.connect(                                      ## <-----
        user = cf['mySQL']['user'], 
        database = cf['mySQL']['database'],
        password = cf['mySQL']['password'],
        host = cf['mySQL']['host'],
        port = cf['mySQL']['port'])

    cursor = mysql_db_conn.cursor()

    # - Crear la tabla si no existe
    cursor.execute("CREATE TABLE IF NOT EXISTS monitoring_system_events (ID BIGINT(20), VERSION_OPTLOCK INT(11), DATE_INS TIMESTAMP, DATE_MODIF DATETIME, ACK_DATE DATETIME, USER_ACK VARCHAR(255), ALARM_DATE DATETIME, COMMON_NETWORK_COMPONENT_ID BIGINT(20), NETWORK_ELEMENT VARCHAR(150), NETWORK_ELEMENT_NOTE VARCHAR(255), STATION VARCHAR(255), DURATION BIGINT(20), ORIGINAL_EVENT_ID BIGINT(20), ALARM_TYPE_ID BIGINT(20), UNKNOWN_EQUIPMENT_TRAP_TEMPLATE_ID BIGINT(20), EVENT_TYPE_ID BIGINT(20), SEVERITY_ID BIGINT(20), SIMBOL_ID BIGINT(20), TRAP_ID BIGINT(20), ALARM_SOURCES VARCHAR(255), TICKET VARCHAR(200), DESCRIPTION VARCHAR(500), SYSLOG_FACILITY_CODE_ID BIGINT(20), PROTOCOL_GROUP BIGINT(20), PRIMARY KEY(ID))")
    for i in range(cf['airflow']['max_rows']):
        # - Realizar la escritura
        sql = "INSERT INTO monitoring_system_events (ID, VERSION_OPTLOCK, DATE_INS, DATE_MODIF, ACK_DATE, USER_ACK, ALARM_DATE, COMMON_NETWORK_COMPONENT_ID, NETWORK_ELEMENT, NETWORK_ELEMENT_NOTE, STATION, DURATION, ORIGINAL_EVENT_ID, ALARM_TYPE_ID, UNKNOWN_EQUIPMENT_TRAP_TEMPLATE_ID, EVENT_TYPE_ID, SEVERITY_ID, SIMBOL_ID, TRAP_ID, ALARM_SOURCES, TICKET, DESCRIPTION, SYSLOG_FACILITY_CODE_ID, PROTOCOL_GROUP) VALUES (%d, %d, %s, %s, %s, %s, %s, %d, %s, %s, %s, %d, %s, %d, %s, %d, %d, %d, %s, %s, %s, %s, %s, %s)"
        ID= i
        VERSION_OPTLOCK= 0
        DATE_INS= datetime.now()
        DATE_MODIF= None
        ACK_DATE= None
        USER_ACK= None
        ALARM_DATE= datetime.now()
        COMMON_NETWORK_COMPONENT_ID= randint(1, 200)
        NETWORK_ELEMENT= choice(string.ascii_letters)
        NETWORK_ELEMENT_NOTE= None
        STATION= choice(string.ascii_letters)
        DURATION= randint(0,500000)
        ORIGINAL_EVENT_ID= None
        ALARM_TYPE_ID= randint(0,5000)
        UNKNOWN_EQUIPMENT_TRAP_TEMPLATE_ID= None
        EVENT_TYPE_ID= randint(0,7)
        SEVERITY_ID= randint(0,7)
        SIMBOL_ID= randint(0,7)
        TRAP_ID= None
        ALARM_SOURCES= choice(string.ascii_letters)
        TICKET= None
        DESCRIPTION= None
        SYSLOG_FACILITY_CODE_ID= None
        PROTOCOL_GROUP= None
        val = (ID, VERSION_OPTLOCK, DATE_INS, DATE_MODIF, ACK_DATE, USER_ACK, ALARM_DATE, COMMON_NETWORK_COMPONENT_ID, NETWORK_ELEMENT, NETWORK_ELEMENT_NOTE, STATION, DURATION, ORIGINAL_EVENT_ID, ALARM_TYPE_ID, UNKNOWN_EQUIPMENT_TRAP_TEMPLATE_ID, EVENT_TYPE_ID, SEVERITY_ID, SIMBOL_ID, TRAP_ID, ALARM_SOURCES, TICKET, DESCRIPTION, SYSLOG_FACILITY_CODE_ID, PROTOCOL_GROUP)
        cursor.execute(sql, val)

        mysql_db_conn.commit()
    
default_args = {                                                                   ## <-----
    "owner": cf['airflow']['owner'],
    "email_on_failure": cf['airflow']['email_on_failure'],
    "email_on_retry": cf['airflow']['email_on_retry'],
    "email": cf['airflow']['email'],
    "retries": cf['airflow']['retries'],
    "retry_delay": timedelta(minutes=cf['airflow']['retry_delay'])
}

with DAG(dag_id='Simulador_mysql', start_date=datetime(2021, 1 ,1), schedule_interval=cf['airflow']['schedule_interval'], default_args=default_args, description='Simulación de datos mysql', tags=['mysql', 'kafka', 'spark'], catchup=False) as dag:
    mySQL_to_Kafka_DAG = PythonOperator(
        task_id="mySQL to Kafka",
        python_callable=mySQL_writer,
        op_args=[cf]
    )