# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header","true")\
    .load("/FileStore/tables/emp_file.csv")

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.columns

# COMMAND ----------

# DBTITLE 1,String Method
df.select("id").show()

# COMMAND ----------

# DBTITLE 1,Column Method
df.select(col("id")).show()

# COMMAND ----------

# DBTITLE 1,sometimes string method is best, sometimes column or other methods it your choice
df.select("id + 5").show()

# COMMAND ----------

df.select(col("id")+5).show()

# COMMAND ----------

# DBTITLE 1,Multiple column select
df.select("name","age","salary").show()

# COMMAND ----------

df.select(col("name"),col("age"), col("salary")).show()

# COMMAND ----------

# DBTITLE 1,All 4 types in one Line
df.select("id",col("name"),df["salary"],df.address).show()

# COMMAND ----------

# DBTITLE 1,Error solve of String using expr
df.select("id + 5").show()

# COMMAND ----------

df.select(expr("id + 5")).show()

# COMMAND ----------

df.select(expr("id as emp_id"),expr("name as emp_name"),expr("concat(name,address)")).show()

# COMMAND ----------

df.select("*").show()

# COMMAND ----------

# MAGIC %md
# MAGIC SPARK SQL

# COMMAND ----------

df.createOrReplaceTempView("employee_tbl")

# COMMAND ----------

spark.sql("""
          
select * from employee_tbl     

""").show()

# COMMAND ----------


