# import datetime
# import uuid
# import logging
# import os

# from pyspark.sql.functions import *
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
# from pyspark.sql import SparkSession
# from configs import kafka_config, topic_name, alert_topic

# # Configure logging
# logging.basicConfig(
#     filename="streaming_batches.log",
#     filemode="a",  # Append mode
#     format="%(asctime)s - %(message)s",
#     level=logging.INFO
# )

# logging.info("Starting the Kafka streaming application.")

# os.environ[
#     'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# spark = (SparkSession.builder
#          .appName("KafkaStreaming")
#          .master("local[*]")
#          .config("spark.driver.memory", "4g")
#          .config("spark.executor.memory", "4g")
#          .config("spark.sql.debug.maxToStringFields", "200")
#          .config("spark.sql.columnNameLengthThreshold", "200")
#          .getOrCreate())

# alerts_df = spark.read.csv("./data/alerts_conditions.csv", header=True)

# spark.sparkContext.setLogLevel("WARN")

# window_duration = "1 minute"
# sliding_interval = "30 seconds"

# kafka_sasl_jaas_config = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";'

# df = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", ','.join(kafka_config['bootstrap_servers'])) \
#     .option("kafka.security.protocol", kafka_config['security_protocol']) \
#     .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
#     .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config) \
#     .option("subscribe", topic_name) \
#     .option("startingOffsets", "earliest") \
#     .option("maxOffsetsPerTrigger", "300") \
#     .load()

# json_schema = StructType([
#     StructField("sensor_id", IntegerType(), True),
#     StructField("timestamp", StringType(), True),
#     StructField("temperature", IntegerType(), True),
#     StructField("humidity", IntegerType(), True)
# ])

# avg_stats = df.selectExpr("CAST(key AS STRING) AS key_deserialized", "CAST(value AS STRING) AS value_deserialized") \
#     .withColumn("value_json", from_json(col("value_deserialized"), json_schema)) \
#     .withColumn("timestamp", to_timestamp(col("value_json.timestamp"), "yyyy-MM-dd HH:mm:ss")) \
#     .withWatermark("timestamp", "10 seconds") \
#     .groupBy(window(col("timestamp"), window_duration, sliding_interval)) \
#     .agg(
#         avg("value_json.temperature").alias("t_avg"),
#         avg("value_json.humidity").alias("h_avg")
#     )

# all_alerts = avg_stats.crossJoin(alerts_df)

# valid_alerts = all_alerts \
#     .where("t_avg > temperature_min AND t_avg < temperature_max") \
#     .union(
#         all_alerts
#         .where("h_avg > humidity_min AND h_avg < humidity_max")
#     ) \
#     .withColumn("timestamp", lit(datetime.datetime.now())) \
#     .drop("id", "humidity_min", "humidity_max", "temperature_min", "temperature_max")

# uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

# kafka_df = valid_alerts \
#     .withColumn("key", uuid_udf()) \
#     .select(
#         col("key"),
#         to_json(struct(
#             col("window"),
#             col("t_avg"),
#             col("h_avg"),
#             col("code"),
#             col("message"),
#             col("timestamp")
#         )).alias("value")
#     )

# def log_and_write_to_kafka(batch_df, batch_id):
#     """
#     Function to process each micro-batch:
#     1. Logs the data in the batch to a file.
#     2. Sends the data to Kafka.
#     """
#     # Log the batch ID and preview of data
#     logging.info(f"Processing Batch ID: {batch_id}")
    
#     if batch_df.isEmpty():
#         logging.info("Batch Data: Empty DataFrame")
#     else:
#         batch_df.show(truncate=False)  # Display on console (optional)
#         logging.info(f"Batch Data:\n{batch_df.toPandas().to_string(index=False)}")  # Write to log file

#     # Write to Kafka if the DataFrame is not empty
#     if not batch_df.isEmpty():
#         batch_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
#             .write \
#             .format("kafka") \
#             .option("kafka.bootstrap.servers", ','.join(kafka_config['bootstrap_servers'])) \
#             .option("topic", alert_topic) \
#             .option("kafka.security.protocol", kafka_config['security_protocol']) \
#             .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
#             .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config) \
#             .save()

# # WriteStream with foreachBatch
# query = kafka_df.writeStream \
#     .trigger(processingTime='30 seconds') \
#     .outputMode("update") \
#     .foreachBatch(log_and_write_to_kafka) \
#     .option("checkpointLocation", "/tmp/unique_checkpoints") \
#     .start()

# query.awaitTermination()


# spark_streaming.py

import datetime
import uuid
import logging
import os

from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType
from pyspark.sql import SparkSession
from configs import kafka_config, topic_name, alert_topic

# Конфігурація логування
logging.basicConfig(
    filename="streaming_batches.log",
    filemode="a",  # Append mode
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

logging.info("Starting the Kafka streaming application.")

# Встановлюємо необхідні пакети для Spark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .config("spark.driver.memory", "4g")
         .config("spark.executor.memory", "4g")
         .config("spark.sql.debug.maxToStringFields", "200")
         .config("spark.sql.columnNameLengthThreshold", "200")
         .getOrCreate())

# Зчитування умов алертів з CSV
alerts_df = spark.read.csv("./data/alerts_conditions.csv", header=True)

spark.sparkContext.setLogLevel("WARN")

# Параметри вікна
window_duration = "1 minute"
sliding_interval = "30 seconds"

# Конфігурація SASL для Kafka
kafka_sasl_jaas_config = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";'

# Читання потоку даних з Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", ','.join(kafka_config['bootstrap_servers'])) \
    .option("kafka.security.protocol", kafka_config['security_protocol']) \
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
    .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "300") \
    .load()

# Схема JSON для розбору повідомлень
json_schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", IntegerType(), True),
    StructField("humidity", IntegerType(), True)
])

# Перетворення даних з Kafka
sensor_df = df.selectExpr("CAST(key AS STRING) AS key_deserialized", "CAST(value AS STRING) AS value_deserialized") \
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema)) \
    .select("key_deserialized", "value_json.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .withWatermark("timestamp", "10 seconds")

# Агрегація даних за вікном
avg_stats = sensor_df.groupBy(window(col("timestamp"), window_duration, sliding_interval)) \
    .agg(
        avg("temperature").alias("t_avg"),
        avg("humidity").alias("h_avg")
    )

# Перехресне приєднання з умовами алертів
all_alerts = avg_stats.crossJoin(alerts_df)

# Визначення алертів (значення поза межами допустимого діапазону)
valid_alerts = all_alerts \
    .filter(
        ((col("t_avg") < col("temperature_min")) | (col("t_avg") > col("temperature_max"))) |
        ((col("h_avg") < col("humidity_min")) | (col("h_avg") > col("humidity_max")))
    ) \
    .withColumn("alert_timestamp", current_timestamp()) \
    .select(
        col("window"),
        col("t_avg"),
        col("h_avg"),
        col("code"),
        col("message"),
        col("alert_timestamp")
    )

# Додавання UUID як ключа для алертів
uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

kafka_df = valid_alerts \
    .withColumn("key", uuid_udf()) \
    .select(
        col("key"),
        to_json(struct(
            col("window"),
            col("t_avg"),
            col("h_avg"),
            col("code"),
            col("message"),
            col("alert_timestamp")
        )).alias("value")
    )

def log_and_write_to_kafka(batch_df, batch_id):
    """
    Функція для обробки кожного мікробачення:
    1. Логування даних у файл.
    2. Відправка даних до Kafka.
    """
    logging.info(f"Processing Batch ID: {batch_id}")
    
    if batch_df.rdd.isEmpty():
        logging.info("Batch Data: Empty DataFrame")
    else:
        # Логування даних
        batch_pd = batch_df.toPandas()
        logging.info(f"Batch Data:\n{batch_pd.to_string(index=False)}")
        
        # Відправка даних до Kafka
        batch_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", ','.join(kafka_config['bootstrap_servers'])) \
            .option("topic", alert_topic) \
            .option("kafka.security.protocol", kafka_config['security_protocol']) \
            .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
            .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config) \
            .save()

# Запуск стрімінгу з обробкою мікробачень
query = kafka_df.writeStream \
    .trigger(processingTime='30 seconds') \
    .outputMode("update") \
    .foreachBatch(log_and_write_to_kafka) \
    .option("checkpointLocation", "/tmp/unique_checkpoints_2") \
    .start()

query.awaitTermination()
