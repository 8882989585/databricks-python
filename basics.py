# Databricks notebook source
print("hello world !!!")

# COMMAND ----------

# MAGIC %run ./basics-utils

# COMMAND ----------

print(full_name("Abhishek", "lamba"))

# COMMAND ----------

files = dbutils.fs.ls("/databricks-datasets")
display(files)
