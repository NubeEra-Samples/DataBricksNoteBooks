# Databricks notebook source
#Create Database
################
db="deltadb"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
spark.sql(f"USE {db}")

# COMMAND ----------

#Configure Delta Database
#########################
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
spark.sql("SET spark.databricks.delta.defaults.autoOptimize.optimizeWrite=true")

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Databricks delta Dataset

# COMMAND ----------

readFormat="delta"
#dbutils.fs.ls("/databricks-datasets/learning-spark-v2/people/people-10m.delta")
loadPath="/databricks-datasets/learning-spark-v2/people/people-10m.delta"

people=spark.read \
.format(readFormat) \
.load(loadPath)

display(people)
#test

# COMMAND ----------

# MAGIC %md
# MAGIC # Write out DataFrame as Databricks Delta data

# COMMAND ----------

writeFormat="delta"
writeMode="overwrite"
partitionBy="gender"
savePath="/tmp/delta/p10m"

people.write \
 .format(writeFormat) \
 .partitionBy(partitionBy) \
 .mode(writeMode) \
 .save(savePath)

# COMMAND ----------

peopleDelta=spark.read.format(readFormat).load(savePath)
display(peopleDelta)

# COMMAND ----------

spark.sql(f"USE {db}")

# COMMAND ----------

tableName=f"{db}.people10m"
display(spark.sql(f"DROP TABLE IF EXISTS {tableName}"))
strSQLQuery=f"CREATE TABLE {tableName} USING DELTA LOCATION '{savePath}' "
#print(strSQLQuery)
display(spark.sql(strSQLQuery))

# COMMAND ----------

display(spark.table(tableName).select('id','salary').orderBy('salary',asceding=False))

# COMMAND ----------

df=spark.table(tableName)
display(df.select("gender").orderBy("gender",ascending=False).groupBy("gender").count())

# COMMAND ----------

display(spark.sql("SHOW PARTITIONS "+tableName))

# COMMAND ----------

display(spark.sql("OPTIMIZE "+tableName))

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY "+tableName))

# COMMAND ----------

display(spark.sql("DESCRIBE DETAIL "+tableName))

# COMMAND ----------

display(spark.sql("DESCRIBE FORMATTED "+tableName))

# COMMAND ----------

spark.sql("DROP TABLE "+tableName)
dbutils.fs.rm(savePath,True)

# COMMAND ----------


