# Databricks notebook source
# MAGIC %md
# MAGIC # START

# COMMAND ----------

from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import io
from datetime import date

today = date.today()

storage_account_key = dbutils.secrets.get(scope = "kotak-sakti-scope-111", key='raefadls-kunci-rahsia')

spark.conf.set("fs.azure.account.key.raefadls.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

schema = StructType([
    StructField("title", StringType(), True),
    StructField("url", StringType(), True),
    StructField("author_name", StringType(), True),
    StructField("word_count", LongType(), True),
    StructField("created_local_time", StringType(), True),
    StructField("body", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("sentiment", StringType(), True)]
           )

# COMMAND ----------

spdf_articles = (spark
.read
.option("header", "True")
.parquet('abfss://silver-new@raefadls.dfs.core.windows.net/combined/*hmetro_utama.parquet/*',schema=schema)
 )



# COMMAND ----------

spdf_articles = spdf_articles.select(*spdf_articles.columns, col('created_local_time').cast("string").alias('created_local_time_casted')).drop('created_local_time')

# COMMAND ----------

spdf_articles.limit(5).toPandas()

print(f"Shape: {(spdf_articles.count(), len(spdf_articles.columns))}")

# COMMAND ----------

spdf_articles_dedupped = spdf_articles.dropDuplicates(subset=['title'])

# COMMAND ----------

snowflake_pw = dbutils.secrets.get(scope = "kotak-sakti-scope-111", key='snowflake-raef-pw')

# COMMAND ----------

options = {
    'sfURL' : 'https://od18624.southeast-asia.azure.snowflakecomputing.com',
    'sfUser' : 'raef78',
    'sfPassword' : snowflake_pw,
    'sfDatabase' : 'FINAL_PROJECT',
    'sfSchema' : 'SCRAPED_NEWS',
    'sfWarehouse': 'COMPUTE_WH'
}

# COMMAND ----------

spdf_articles_dedupped.write.format("snowflake").options(**options).mode("overwrite").option('dbtable','HMETRO_UTAMA').save()

# COMMAND ----------

# MAGIC %md
# MAGIC # END
