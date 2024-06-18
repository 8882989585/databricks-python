# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

kafka_props = {
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule  required username="ZUCAGON33TP3ZF45"password="teEbFxCUrOW5Nzt7Xch7KUslBIalCVjlpN18xxDpzw4jPhJ+v4tOXcRwo/d7ZmmD";',
    "kafka.bootstrap.servers": "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092",
    "kafka.ssl.endpoint.identification.algorithm": "https",
    "kafka.security.protocol": "SASL_SSL",
    "subscribe": "topic_1",
}


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

dlt.create_streaming_table("kafka_topic_4")  

@dlt.append_flow(target = "kafka_topic_4")
def kafka_topic_4():
  return df

kafka_props_2 = {
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule  required username="ZUCAGON33TP3ZF45"password="teEbFxCUrOW5Nzt7Xch7KUslBIalCVjlpN18xxDpzw4jPhJ+v4tOXcRwo/d7ZmmD";',
    "kafka.bootstrap.servers": "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092",
    "kafka.ssl.endpoint.identification.algorithm": "https",
    "kafka.security.protocol": "SASL_SSL",
    "subscribe": "topic_2",
}

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

dlt.create_streaming_table("kafka_topic_3")  

@dlt.append_flow(target = "kafka_topic_3")
def kafka_topic_3():
  return df2


dlt.create_streaming_table("dlt_result")

@dlt.append_flow(target="dlt_result")
def dlt_result():
    df2_wm = dlt.read_stream("kafka_topic_3").withWatermark("event_ts", "1 minutes")
    df1_wm = dlt.read_stream("kafka_topic_4").withWatermark("event_ts_m", "1 minutes")
    df3 = (
        df1_wm.join(
            df2_wm,
            (col("token") == col("token2"))
            & (col("event_ts") <= col("event_ts_m") + expr("INTERVAL 1 MINUTES"))
            & (col("event_ts") >= col("event_ts_m") - expr("INTERVAL 1 MINUTES")),
            how="left",
        )
        .groupBy(window(col("event_ts_m"), "1 minutes"), col("name"), col("token"))
        .count()
        .select(col("name"), col("token"), col("count"))
    )
    return df3  
