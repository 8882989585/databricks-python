# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

kafka_props = {
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule  required username="ZUCAGON33TP3ZF45"password="teEbFxCUrOW5Nzt7Xch7KUslBIalCVjlpN18xxDpzw4jPhJ+v4tOXcRwo/d7ZmmD";',
    "kafka.bootstrap.servers": "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092",
    "kafka.ssl.endpoint.identification.algorithm": "https",
    "kafka.security.protocol": "SASL_SSL",
    "subscribe": "topic_1",
}


def write_date(df: DataFrame, i: int):
    df.write.mode("append").format("csv").save("s3://acs-ap-south-1-bucket/databricks-ext/person/")


df = spark.readStream.format("kafka").options(**kafka_props).load()

df = df.select(
    col("value").cast("string"),
    col("partition").cast("string"),
    col("offset").cast("string"),
).select(
        from_json(
            col("value"),
            StructType(
                [StructField("name", StringType()), StructField("token", IntegerType())]
            )
        ).alias("value")
    ).select(col("value.*"), current_timestamp().alias("event_ts_m"))

df.writeStream.format("parquet").options(
    **{
        "checkpointLocation": "s3://acs-ap-south-1-bucket/databricks-ext/checkpoint/person/"
    }
).trigger(processingTime="1 minutes").foreachBatch(
    write_date
).start()

# COMMAND ----------

kafka_props_2 = {
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule  required username="ZUCAGON33TP3ZF45"password="teEbFxCUrOW5Nzt7Xch7KUslBIalCVjlpN18xxDpzw4jPhJ+v4tOXcRwo/d7ZmmD";',
    "kafka.bootstrap.servers": "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092",
    "kafka.ssl.endpoint.identification.algorithm": "https",
    "kafka.security.protocol": "SASL_SSL",
    "subscribe": "topic_2",
}

def write_data_2(df: DataFrame, i: int):
    df.write.mode("append").format("csv").save("s3://acs-ap-south-1-bucket/databricks-ext/token/")

df2 = spark.readStream.format("kafka").options(**kafka_props).load()

df2 = df2.select(
    col("value").cast("string"),
    col("partition").cast("string"),
    col("offset").cast("string"),
).select(
        from_json(
            col("value"),
            StructType(
                [StructField("token", IntegerType())]
            )
        ).alias("value")
    ).select(col("value.*"), current_timestamp().alias("event_ts")).select(col("token").alias("token2"), col("event_ts"))

df2.writeStream.format("parquet").options(
    **{
        "checkpointLocation": "s3://acs-ap-south-1-bucket/databricks-ext/checkpoint/token/"
    }
).trigger(processingTime="1 minutes").foreachBatch(
    write_data_2
).start()

# COMMAND ----------

def write_data_3(df: DataFrame, i: int):
    df.write.option("header", True).mode("append").format("csv").save(
        "s3://acs-ap-south-1-bucket/databricks-ext/lottery/"
    )


df1_wm = df.withWatermark("event_ts_m", "10 minutes")
df2_wm = df2.withWatermark("event_ts", "10 minutes")

df3 = (
    df1_wm.join(
        df2_wm,
        (col("token") == col("token2"))
        & (col("event_ts") <= col("event_ts_m") + expr("INTERVAL 5 MINUTES"))
        & (col("event_ts") >= col("event_ts_m") - expr("INTERVAL 5 MINUTES")),
        how="left",
    )
    .groupBy(window(col("event_ts_m"), "2 minutes"), col("name"), col("token"))
    .count()
    .select(col("name"), col("token"), col("count"))
)

df3.writeStream.format("parquet").options(
    **{
        "checkpointLocation": "s3://acs-ap-south-1-bucket/databricks-ext/checkpoint/lottery/"
    }
).trigger(processingTime="2 minutes").foreachBatch(write_data_3).start()

# COMMAND ----------

spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("cloudFiles.schemaLocation", "s3://acs-ap-south-1-bucket/databricks-ext/checkpoint/person_lottery/").load("s3://acs-ap-south-1-bucket/databricks-ext/lottery/*.csv").writeStream.option("checkpointLocation", "s3://acs-ap-south-1-bucket/databricks-ext/checkpoint/person_lottery/").table("people_lottery")
