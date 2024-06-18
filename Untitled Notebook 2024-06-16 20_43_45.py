# Databricks notebook source
# MAGIC %sql
# MAGIC select * from kafka_topic_1 order by event_ts_m desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kafka_topic_2 order by event_ts desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dlt_result;
