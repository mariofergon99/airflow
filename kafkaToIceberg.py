if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, TimestampType
    from pyspark.sql.functions import col, from_json

    spark = SparkSession\
        .builder\
        .enableHiveSupport() \
        .getOrCreate()  \
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
        .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog') \
        .config('spark.sql.catalog.spark_catalog.type', 'hive') \
        .config('spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog') \
        .config('spark.sql.catalog.local.type', 'hadoop') \
        .config('spark.sql.catalog.local.warehouse', '$PWD/warehouse')

    import yaml 
    cf_path = 's3a://bucket-config/config_kafkaToIceberg.yaml'

    with open(cf_path, 'r') as file:  ## <----- 
        cf = yaml.safe_load(file)

    schema = StructType() \
            .add('Timestamp', TimestampType())     # <-----

    # Read Kafka data  
    data = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', cf['kafka']['bootstrap-servers']) \
        .option('failOnDataLoss', 'false') \
        .option('subscribe', cf['kafka']['read-topics']) \
        .option('startingOffsets', 'latest') \
        .option('enable.auto.commit', 'true') \
        .load() 

    # Data processing?

    # Create database

    # Create table
    spark.sql('CREATE TABLE IF NOT EXISTS local.db.table (id bigint, data string) USING iceberg')

    # Write data to Iceberg
    data.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime=1) \
        .option("path", cf['iceberg']['tableIdentifier']) \
        .option("checkpointLocation", cf['iceberg']['checkpointPath']) \
        .start()