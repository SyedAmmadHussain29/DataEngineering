# Databricks notebook source
File uploaded to /FileStore/tables/line_delimited.json
File uploaded to /FileStore/tables/corrupted.json
File uploaded to /FileStore/tables/Multi_line_incorrect.json
File uploaded to /FileStore/tables/Multi_line_correct.json
File uploaded to /FileStore/tables/single_file_json_with_extra_fields.json

# COMMAND ----------

spark.read.format("json").load("/FileStore/tables/line_delimited.json").show()

# COMMAND ----------

spark.read.format("json").option("header","true").option("mode","permissive")\
    .load("/FileStore/tables/single_file_json_with_extra_fields.json").show()

# COMMAND ----------

spark.read.format("json").option("multiline","true").load("/FileStore/tables/Multi_line_correct.json").show()

# COMMAND ----------

spark.read.format("json").option("multiline","true").load("/FileStore/tables/Multi_line_incorrect.json").show()

# COMMAND ----------

spark.read.format("json").load("/FileStore/tables/corrupted.json").show(truncate=False)

# COMMAND ----------


