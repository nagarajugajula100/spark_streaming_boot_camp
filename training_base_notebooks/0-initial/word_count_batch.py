# Databricks notebook source
from pyspark.sql.functions import explode, split, lower,trim

# COMMAND ----------

# Load the Data 
# /FileStore/tables/boot_camp/datasets/text/

# COMMAND ----------

base_data_dir = "/FileStore/tables/boot_camp"

# COMMAND ----------

# Data Cleanup

# COMMAND ----------

spark.sql("drop table if exists word_count_table")
dbutils.fs.rm("/user/hive/warehouse/word_count_table", True)

dbutils.fs.rm(f"{base_data_dir}/chekpoint", True)
dbutils.fs.rm(f"{base_data_dir}/data/text", True)

dbutils.fs.mkdirs(f"{base_data_dir}/data/text")

# COMMAND ----------

itr = 1

# COMMAND ----------

# Load the data to required location

# COMMAND ----------

dbutils.fs.cp(f"{base_data_dir}/datasets/text/text_data_{itr}.txt", f"{base_data_dir}/data/text/")

# COMMAND ----------

lines = (spark.read
                    .format("text")
                    .option("lineSep", ".")
                    .load(f"{base_data_dir}/data/text")
)
lines_df = lines.select(explode(split(lines.value, " ")).alias("word"))

# COMMAND ----------

# Data Clearning 

# COMMAND ----------

formatted_df = (lines_df.select(lower(trim(lines_df.word)).alias("word"))
                        .where("word is not null")
                        .where("word rlike '[a-z]'")
)

# COMMAND ----------

# Aggregation

# COMMAND ----------

formatted_group_df= formatted_df.groupBy("word").count()

# COMMAND ----------

# Saving teh Data 

# COMMAND ----------

(formatted_group_df.write
                    .format("delta")
                    .mode("overwrite")
                    .saveAsTable("word_count_table")
                    )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from word_count_table;
