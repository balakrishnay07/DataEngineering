# Databricks notebook source
dbutils.widgets.text("Folder_name","","")
dbutils.widgets.text("File_name","","")

# COMMAND ----------

Folder_location = dbutils.widgets.get("Folder_name")
File_location = dbutils.widgets.get("File_name")

print("Folder Variable is ", Folder_location)
print("File variable is ", File_location)

# COMMAND ----------

df = spark.read.format("csv").option("inferSchema", True).option("sep",";").option("header",True).load(Folder_location+File_location)

display(df)

# COMMAND ----------


