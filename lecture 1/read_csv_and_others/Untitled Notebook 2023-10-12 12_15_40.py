# Databricks notebook source
spark

# COMMAND ----------

flight_df = spark.read.format("csv")\
    .load("/FileStore/tables/2015_summary.csv")

flight_df.show(5)

# COMMAND ----------

flight_df_1 = spark.read.format("csv")\
    .option("header","true")\
    .load("/FileStore/tables/2015_summary.csv")

flight_df_1.show(5)

# COMMAND ----------

flight_df_2 = spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .option("mode","FAILFAST")\
    .load("/FileStore/tables/2015_summary.csv")

flight_df_2.show(5)

# COMMAND ----------

flight_df_2.printSchema()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

mySchema = StructType([
    StructField("DEST_COUNTRY_NAME",StringType(),True),
    StructField("ORIGIN_COUNTRY_NAME",StringType(),True),
    StructField("count", IntegerType(),True)
])

# COMMAND ----------

flight_df_schema = spark.read.format("csv")\
    .option("header","false")\
    .option("skipRows",1) \
    .option("inferschema","false")\
    .schema(mySchema)\
    .load("/FileStore/tables/2015_summary.csv")

flight_df_schema.show(5)

# COMMAND ----------

FAILFAST
DROPMALFORMED
PERMISSIVE

# COMMAND ----------

flight_df_schema_1 = spark.read.format("csv")\
    .option("header","false")\
    .option("skipRows",1) \
    .option("inferschema","false")\
    .option("MODE","PERMISSIVE")\
    .schema(mySchema)\
    .load("/FileStore/tables/2015_summary.csv")

flight_df_schema_1.show(5)

# COMMAND ----------


