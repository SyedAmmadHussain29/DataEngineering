# Databricks notebook source
df = spark.read.format("csv").option("header","true")\
    .option("inferschema","true").load("/FileStore/tables/employee_write_dataa.csv")
    
df.show()

# COMMAND ----------

now suppose we have done all the transformations, now we need to save our data into to other location

# COMMAND ----------

df.write.format("csv")\
    .option("header","true")\
        .option("mode","overwrite")\
            .option("path","/FileStore/tables/csv_write/").save()

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/csv_write/")

# COMMAND ----------

# DBTITLE 1,for more partition
df.repartition(3).write.format("csv")\
    .option("header","true")\
        .option("mode","overwrite")\
            .option("path","/FileStore/tables/csv_write_repartition/").save()

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/csv_write_repartition/")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.format("csv")\
    .option("header","true")\
        .option("mode","overwrite")\
            .option("path","/FileStore/tables/partition_by_address/")\
                .partitionBy("address").save()

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/partition_by_address/")

# COMMAND ----------

df.write.format("csv")\
    .option("header","true")\
        .option("mode","overwrite")\
            .option("path","/FileStore/tables/partition_by_address_and_gender/")\
                .partitionBy("address","gender").save()

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/partition_by_address_and_gender/")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/partition_by_address_and_gender/address=INDIA/")

# COMMAND ----------

# DBTITLE 1,Bucket
df.write.format("csv")\
    .option("header","true")\
        .option("mode","overwrite")\
            .option("path","/FileStore/tables/bucket_by_id/")\
                .bucketBy(3,"id")\
                    .saveAsTable("bucket_by_id_table")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/bucket_by_id/")

# COMMAND ----------


