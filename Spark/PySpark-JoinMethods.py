# Databricks notebook source
# MAGIC %md
# MAGIC Create DataFrame

# COMMAND ----------

employee_data = [(10,'Raj Kumar','1999','100','M',2000),
                 (20,'Rajani','2002','200','F',8000),
                 (30,'Ragav ','2010','100',None,6000),
                 (40,'Raja Singh','2004','100','F',7000),
                 (50,'Rama Krish','1999','400','M',2000),
                 (60,'Rasul','2014','500','M',5000),
                 (70,'Kumar Chand','2004','600','M',5000)                 
                 ]

employee_schema = ['EmployeeID','Name','Doj','dept_id','Gender','Salary']

empDF = spark.createDataFrame(employee_data, employee_schema)

# COMMAND ----------

display(empDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Create Department DataFrame

# COMMAND ----------

department_data = [('HR',100),
                   ('Supply',200),
                   ('Sales',300),
                   ('Stock',400)
                   ]

department_schema = ['dept_name', 'dept_id']

deptDF = spark.createDataFrame(department_data, department_schema)

# COMMAND ----------

display(deptDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Inner join

# COMMAND ----------

df_join = empDF.join(deptDF, empDF.dept_id == deptDF.dept_id, 'inner') 

display(df_join)

# COMMAND ----------

# MAGIC %md
# MAGIC Full outer join

# COMMAND ----------

df_join = empDF.join(deptDF, empDF.dept_id == deptDF.dept_id, 'fullouter')

display(df_join)

# COMMAND ----------

df_join = empDF.join(deptDF, empDF.dept_id == deptDF.dept_id, 'left')

display(df_join)

# COMMAND ----------

df_join = empDF.join(deptDF, empDF.dept_id == deptDF.dept_id, 'right')

display(df_join)

# COMMAND ----------

# MAGIC %md
# MAGIC Left Semi Join

# COMMAND ----------

df_join = empDF.join(deptDF, empDF.dept_id == deptDF.dept_id, 'leftsemi') 

display(df_join)

# COMMAND ----------

# MAGIC %md
# MAGIC Left Anti Join

# COMMAND ----------

df_join = empDF.join(deptDF, empDF.dept_id == deptDF.dept_id, 'leftanti') 

display(df_join)

# COMMAND ----------


