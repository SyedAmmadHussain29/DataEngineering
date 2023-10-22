# Databricks notebook source
emp_df = spark.read.format("csv").option("header","true").option("MODE","permissive")    \
.load("/FileStore/tables/emp_file.csv")
emp_df.show()

# COMMAND ----------

emp_df1 = spark.read.format("csv").option("header","true").option("MODE","DROPMALFORMED")    \
.load("/FileStore/tables/emp_file.csv")

emp_df1.show()

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

emp_schema = StructType([
    StructField("id",IntegerType(), True),
    StructField("name",StringType(), True),
    StructField("age",IntegerType(), True),
    StructField("salary",IntegerType(), True),
    StructField("address",StringType(), True),
    StructField("nominee",StringType(), True),
    StructField("_corrupt_record",StringType(), True)
])

# COMMAND ----------

emp_df2 = spark.read.format("csv").option("header","true").option("MODE","PERMISSIVE")\
.schema(emp_schema)\
.load("/FileStore/tables/emp_file.csv")

emp_df2.show(truncate=False)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

emp_df2 = spark.read.format("csv").option("header","true")\
.schema(emp_schema)\
.option("badRecordsPath","/FileStore/tables/bad_records")\
.load("/FileStore/tables/emp_file.csv")

emp_df2.show(truncate=False)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/bad_records/20231013T120219

# COMMAND ----------

ls /FileStore/tables/bad_records/20231013T120219/bad_records

# COMMAND ----------

bad_df = spark.read.format("json").load("/FileStore/tables/bad_records/20231013T120219/bad_records/")
bad_df.show(truncate=False)

# COMMAND ----------


