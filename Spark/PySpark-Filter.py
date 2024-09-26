# Databricks notebook source
# MAGIC %md
# MAGIC ## Create Dataframe

# COMMAND ----------

employee_data = [(10,'Raj Kumar','1999','100','M',2000),
                 (20,'Rajani','2002','200','F',8000),
                 (30,'Ragav ','2010','100',None,6000),
                 (40,'Raja Singh','2004','100','F',7000),
                 (50,'Rama Krish','1999','100','M',2000),
                 (60,'Rasul','2014','500','M',5000),
                 (70,'Kumar Chand','2004','600','M',5000)                 
                 ]

employee_schema = ['EmployeeID','Name','Doj','dept_id','Gender','Salary']

employeeDF = spark.createDataFrame(data = employee_data, schema = employee_schema)

display(employeeDF)

# COMMAND ----------

display(employeeDF)

display(employeeDF.filter(employeeDF.Salary ==5000))

# COMMAND ----------

display(employeeDF)

display(employeeDF.filter(employeeDF.Salary < 5000))

# COMMAND ----------

display(employeeDF)

display(employeeDF.filter((employeeDF.Gender == 'F') & (employeeDF.Doj == '2004')))

# COMMAND ----------

display(employeeDF)

display(employeeDF.filter(employeeDF.Name.contains('Kumar'))) #startswith #endswith

# COMMAND ----------

display(employeeDF)

display(employeeDF.filter(employeeDF.Gender.isNotNull())) #isNull #isNotNull

# COMMAND ----------

display(employeeDF)

display(employeeDF.filter(~employeeDF.Salary.isin(6000,8000))) #isin

# COMMAND ----------

display(employeeDF)

display(employeeDF.filter(employeeDF.Name.like('%Kumar%')))

# COMMAND ----------


