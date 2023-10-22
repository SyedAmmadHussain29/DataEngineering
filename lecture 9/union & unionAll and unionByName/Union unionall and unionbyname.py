# Databricks notebook source
### Data
data=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18),
(13 ,'Nidhi',60000,  17),
(14 ,'Priya',80000,  18),
(15 ,'Mohit',45000,  18),
(16 ,'Rajesh',90000, 10),
(17 ,'Raman',55000, 16),
(18 ,'Sam',65000,   17)]

### schema ###
schema = ['id','Name','salary','mngr_id']
### Create Data Frame
manager_df = spark.createDataFrame(data=data, schema = schema)
manager_df.show()

# COMMAND ----------

### Another data ###
data1=[(19 ,'Sohan',50000, 18),
(20 ,'Sima',75000,  17)]
### creating schema1 ###
schema1=['id','Name','salary','mngr_id']
### Creating Data Frame ###
manager_df1 = spark.createDataFrame(data=data1, schema=schema1)
manager_df1.show()

# COMMAND ----------

manager_df.union(manager_df1).show()


# COMMAND ----------

### Data
data=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18),
(13 ,'Nidhi',60000,  17),
(14 ,'Priya',80000,  18),
(15 ,'Mohit',45000,  18),
(16 ,'Rajesh',90000, 10),
(17 ,'Raman',55000, 16),
(18 ,'Sam',65000,   17),
(18 ,'Sam',55000,   17),
(18 ,'Sam',65000,   17)
]

### schema ###
schema = ['id','Name','salary','mngr_id']
### Create Data Frame
duplicate_manager_df = spark.createDataFrame(data=data, schema = schema)
duplicate_manager_df.show()

# COMMAND ----------

manager_df.union(manager_df1).count()

# COMMAND ----------

manager_df.unionAll(manager_df1).count()
### for dataframe union and union all are same but not for Spark SQL ###

# COMMAND ----------

duplicate_manager_df.union(manager_df1).count()

# COMMAND ----------

duplicate_manager_df.unionAll(manager_df1).count()

# COMMAND ----------

manager_df1.createOrReplaceTempView("manager_df_tbl")
duplicate_manager_df.createOrReplaceTempView("duplicate_manager_df_tbl")

# COMMAND ----------

### spark Sql
### here you can see union is working in spark sql

spark.sql("""
select * from manager_df_tbl
union
select * from duplicate_manager_df_tbl           
""").count()

# COMMAND ----------

spark.sql("""
select * from manager_df_tbl
union all
select * from duplicate_manager_df_tbl           
""").count()

# COMMAND ----------

### Another Data
wrong_column_data=[(19 ,50000, 18,'Sohan'),
(20 ,75000,  17,'Sima')]
### creating Schema
wrong_schema2=['id','salary','mngr_id','Name']
wrong_manager_df= spark.createDataFrame(data=wrong_column_data, schema=wrong_schema2)


# COMMAND ----------

manager_df1.union(wrong_manager_df).show()
### here you can see how it place data if the column names are not same as other tables column name

# COMMAND ----------

manager_df1.show()
wrong_manager_df.show()

# COMMAND ----------

# DBTITLE 1,UnionByNAme
### to solve the above we use union by name
manager_df1.unionByName(wrong_manager_df).show()

# COMMAND ----------

### Note the columns should be equal is both the tables getting union else error 
