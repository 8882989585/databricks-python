# Databricks notebook source
ss = spark.builder.getOrCreate()

df = ss.createDataFrame(ss.sparkContext.parallelize([("A", 1), ("B", 2), ("C", 3),("D", 4),("E", 5),("F", 6),("G", 7),("H", 8)])).toDF(*["Name", "Class"])

df.write.format("delta").save("s3://acs-ap-south-1-bucket/databricks-ext/result/")


# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("delta_demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail delta_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC update delta_demo set Class = 10 where Class = 8

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta_demo

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, 's3://acs-ap-south-1-bucket/databricks-ext/result/')
deltaTable.update(
  condition = col('Class') == 8,
  set = { 'Class': lit(10) }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY 's3://acs-ap-south-1-bucket/databricks-ext/result/'

# COMMAND ----------

df = spark.read.option("versionAsOf", 1).load('s3://acs-ap-south-1-bucket/databricks-ext/result/')
df.show(truncate=False)

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, 's3://acs-ap-south-1-bucket/databricks-ext/result/')
deltaTable.restoreToVersion(1)

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, 's3://acs-ap-south-1-bucket/databricks-ext/result/')
deltaTable.optimize().executeZOrderBy("Class")

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forName(spark, 'delta_demo')
deltaTable.vacuum()

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema associate_demo

# COMMAND ----------

df = ss.createDataFrame(ss.sparkContext.parallelize([("A", 1), ("B", 2), ("C", 3),("D", 4),("E", 5),("F", 6),("G", 7),("H", 8)])).toDF(*["Name", "Class"])

df.write.format("delta").option("schema","associate_demo").option("location","s3://acs-ap-south-1-bucket/databricks-ext/result-ext/").mode("append").saveAsTable("delta_demo_ext")
# location is ignored with save as table

# COMMAND ----------

# MAGIC %sql
# MAGIC use associate_demo;
# MAGIC select * from delta_demo_ext;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table default.delta_demo_2 location 's3://acs-ap-south-1-bucket/databricks-ext/result-ext/' as select * from default.delta_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC create view default.delta_demo_view as select * from default.delta_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.delta_demo_view;

# COMMAND ----------

df = ss.read.format("csv").load("s3://acs-ap-south-1-bucket/databricks-ext/other-data/*.csv")
df.show(truncate=False)

# COMMAND ----------

df = ss.read.format("json").load("s3://acs-ap-south-1-bucket/databricks-ext/other-data/*.json")
df.show(truncate=False)

# COMMAND ----------

df.write.format("delta").save("s3://acs-ap-south-1-bucket/databricks-ext/result-json/'")
