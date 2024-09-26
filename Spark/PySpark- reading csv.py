# Databricks notebook source
df = spark.read.format("csv").option("inferschema", True).option("header", True).option("delimiter",";").load("/FileStore/tables/CountryPops.csv")

display(df)

# COMMAND ----------

print(df.count())

# COMMAND ----------

df1 = spark.read.format("csv").options(inferschema="true", sep=";", header="True").load("/FileStore/tables/CountryPops.csv")

display(df1)

print(df1.count())

# COMMAND ----------

df2 = spark.read.format("csv").option("inferschema",True).option("sep", ";").option("header", True).load(["/FileStore/tables/CountryPops.csv","/FileStore/tables/CountryPops1.csv"])

display(df2)

print(df2.count())

# COMMAND ----------

df3 = spark.read.format("csv").options(inferschema="True", delimiter=";", header="True").load("/FileStore/tables")

display(df3)

print(df3.count())

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df5 = spark.read.format("csv").options(inferschema="True",sep=";",header="True").load("/FileStore/tables/CountryPops1.csv")

display(df5)

print(df5.count())

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType, StringType

schema_defined = StructType([StructField("Rank",IntegerType(),True),
                             StructField("Country (or dependent territory)",StringType(),True),
                             StructField("population", IntegerType(), True),
                             StructField("Date", StringType(), True),
                             StructField("% of world population", StringType(), True),
                             StructField("Source", StringType(), True)                             
                             ])

# COMMAND ----------

df6 = spark.read.format("csv").schema(schema_defined).options(sep=";", header=True).load("/FileStore/tables/CountryPops3.csv")

display(df6)

print(df6.count())

# COMMAND ----------

df6.printSchema()

# COMMAND ----------


