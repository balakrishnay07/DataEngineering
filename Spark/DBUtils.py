# Databricks notebook source
dbutils.fs.help()

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

dbutils.fs.head("/FileStore/tables/CountryPops.csv")

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/tables/pyspark")

# COMMAND ----------

dbutils.fs.cp("/FileStore/tables/CountryPops1.csv","/FileStore/tables/pyspark")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/pyspark")

# COMMAND ----------

dbutils.fs.head("/FileStore/tables/pyspark/CountryPops1.csv")

# COMMAND ----------

dbutils.fs.mv("/FileStore/tables/CountryPops2.csv", "/FileStore/tables/pyspark/")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/pyspark/")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

dbutils.fs.put("/FileStore/tables/pyspark/test.txt", "Hello World")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/pyspark/")

# COMMAND ----------

dbutils.fs.head("/FileStore/tables/pyspark/test.txt")

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/pyspark/test.txt")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/pyspark")

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/pyspark/", True)

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC dbutils notebook commands

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/p950bly@fspa.myntet.se/DataEngineering/Spark/PySpark- reading csv", 60)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Widgets

# COMMAND ----------

# MAGIC %run /Workspace/Users/p950bly@fspa.myntet.se/DataEngineering/Spark/Training_Widget_for_DBUtils $Folder_name="/FileStore/tables/" $File_name="CountryPops1.csv"

# COMMAND ----------

dbutils.widgets.dropdown("Drop_down","1",[str(x) for x in range(1,10)])

# COMMAND ----------

dbutils.widgets.combobox("combo_box","1", [str(x) for x in range(1,10)])

# COMMAND ----------

dbutils.widgets.multiselect("product","Camera",("Camera","gps","smartphone"))

# COMMAND ----------

dbutils.widgets.remove("combo_box")

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------


