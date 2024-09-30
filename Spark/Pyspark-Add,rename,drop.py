# Databricks notebook source
employee_data = [(10,'Raj Kumar','1999','100','M',2000),
                 (20,'Rajani','2002','200','F',8000),
                 (30,'Ragav ','2010','100',None,6000),
                 (40,'Raja Singh','2004','100','F',7000),
                 (50,'Rama Krish','1999','100','M',2000),
                 (60,'Rasul','2014','500','M',5000),
                 (70,'Kumar Chand','2004','600','M',5000)                 
                 ]

employee_schema = ['EmployeeID','Name','Doj','dept_id','Gender','Salary']

empDF = spark.createDataFrame(data=employee_data, schema= employee_schema)

display(empDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Add new column using constant literal

# COMMAND ----------

from pyspark.sql.functions import lit

empDF_AddColumn = empDF.withColumn("Location",lit("Bangalore")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Add new column by calculation 

# COMMAND ----------

from pyspark.sql.functions import concat

empDF_AddColumn = empDF.withColumn("Bonus",empDF.Salary*0.1)

display(empDF_AddColumn)

# COMMAND ----------

empDF_AddColumn.printSchema()

# COMMAND ----------

print(empDF_AddColumn.dtypes)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Rename a column

# COMMAND ----------

empDF_RenameColumn = empDF_AddColumn.withColumnRenamed("Bonus","Hike")

# COMMAND ----------

display(empDF_RenameColumn)

# COMMAND ----------

empDF_RenameColumn = empDF_AddColumn.withColumnRenamed("Doj","Date_Of_Join")

# COMMAND ----------

display(empDF_RenameColumn)

# COMMAND ----------

# MAGIC %md
# MAGIC Drop a column

# COMMAND ----------

empDF_DropColumn = empDF_AddColumn.drop("Bonus").show()

# COMMAND ----------

empDF_DropColumn = empDF_RenameColumn.drop("Bonus")

# COMMAND ----------

empDF_DropColumn.printSchema()

# COMMAND ----------


