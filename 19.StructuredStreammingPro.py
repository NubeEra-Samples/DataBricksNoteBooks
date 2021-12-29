# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/structured-streaming/events/

# COMMAND ----------

# MAGIC %fs head /databricks-datasets/structured-streaming/events/file-0.json

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

#Prepare Data+Schema
inputPath="/databricks-datasets/structured-streaming/events/"
jsonSchema = StructType([ 
    StructField("time", TimestampType(), True), 
    StructField("action", StringType(), True) 
])

#Prepare StreamingDF
streamingInputDF=(
    spark
    .readStream
    .schema(jsonSchema)
    .option("maxFilesPerTrigger",1)
    .json(inputPath)
)

#Perform some Operations on streamingInputDF
streamingCountsDF=(
    streamingInputDF
    .groupBy(
        streamingInputDF.action,
        window(streamingInputDF.time,"1 hour")
    )
    .count()
)

#Check streamingCountsDF Streaming or not
streamingCountsDF.isStreaming

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions","2")

query=(
    streamingCountsDF
    .writeStream
    .format("memory")      #memory= store in-memory table
    .queryName("counts")   #counts= name of the in-memory table
    .outputMode("complete")#complete= all the counts should be in the table
    .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select action,date_format(window.end,"MMM-dd HH:mm") as time,count from counts order by time, action

# COMMAND ----------


