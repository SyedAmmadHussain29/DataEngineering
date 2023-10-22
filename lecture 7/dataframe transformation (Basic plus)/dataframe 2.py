# Databricks notebook source
### imports ###
from pyspark.sql.functions import *
from pyspark.sql.types import *

### Deading Data ###
emp_df = spark.read.format("csv")\
    .option("header","true")\
    .option("mode","permissive")\
    .load("/FileStore/tables/emp_file.csv")
emp_df.show()

### Printing Schema ###
emp_df.printSchema() 

### Create Temporary view ###
emp_df.createOrReplaceTempView("emp_tbl")

# COMMAND ----------

# DBTITLE 1,Expr Method
### expr ###
emp_df.select(expr("id as emp_id"), ).show()

# COMMAND ----------

# DBTITLE 1,Alias with column
### in this example we have use alias with works with col 
emp_df.select(col("id").alias("emp_id"),"name","age").show()

# COMMAND ----------

# DBTITLE 1,Filter/where
### we can use where & filter both. It will give same answer
### Q: salary > 150K

### Filter ###
emp_df.filter(col("salary")>150000).show()
### Where ###
emp_df.where(col("salary")>150000).show()



# COMMAND ----------

# DBTITLE 1,Filter/ Where with conditional statement
### brakets are necessary to remove error
emp_df.where((col("salary")>150000) & (col("age")>18)).show()

# COMMAND ----------

# DBTITLE 1,Literal
### it is use to add same data is all the columns, by creating another column
### lit keyword is use 
### alias use to name the column 
emp_df.select("*", lit("khan").alias("last_name")).show()

# COMMAND ----------

# DBTITLE 1,Adding Column
### lit will also use in this
emp_df.withColumn("surname", lit("hussain")).show()

# COMMAND ----------

# DBTITLE 1,Rename Column
emp_df.withColumnRenamed("id","emp_id").show()

# COMMAND ----------

emp_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Type Casting
emp_df.withColumn("id",col("id").cast("string")).withColumn("salary",col("salary").cast("long")).printSchema()
### here you can see ID, salary  cast has been change

# COMMAND ----------

# DBTITLE 1,Removing Column
emp_df.drop("id").show()
### emp_df.drop(col("id")).show() 
### both methods are same

# COMMAND ----------

# MAGIC %md Same but using Spark SQL

# COMMAND ----------

# DBTITLE 1,where
### where
spark.sql("""
select * from emp_tbl where salary>150000 and age >18   
""").show()

# COMMAND ----------

spark.sql("""
select *, "Kumar" as last_name from emp_tbl   
""").show()

# COMMAND ----------

# DBTITLE 1,add column
spark.sql("""
select *, "Kumar" as last_name, concat(name,last_name ) as full_name from emp_tbl   
""").show()

# COMMAND ----------

# DBTITLE 1,rename column
spark.sql("""
select *, id as employee_id from emp_tbl   
""").show()


# COMMAND ----------

# DBTITLE 1,type casting
spark.sql("""
select *, id as employee_id, cast(id as string) from emp_tbl   
""").printSchema()


# COMMAND ----------


