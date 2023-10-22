# Databricks notebook source
# DBTITLE 1,Create Dataframe
df=[(1,1),
(2,1),
(3,1),
(4,2),
(5,1),
(6,2),
(7,2)]

# COMMAND ----------

mySchema = ['id' ,'num']

# COMMAND ----------

spark.createDataFrame(data=df, schema=mySchema).show()
