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

spdf_articles = (spark
.read
.option("header", "True")
.option("inferSchema", "true")
.parquet('abfss://bronze-new@raefadls.dfs.core.windows.net/articles/*hmetro_utama.parquet')
 )

# COMMAND ----------

print(f"Shape: {(spdf_articles.count(), len(spdf_articles.columns))}")

# COMMAND ----------

spdf_inferences = (spark
.read
.option("header", "True")
.option("inferSchema", "true")
.parquet('abfss://bronze-new@raefadls.dfs.core.windows.net/inferred/*hmetro_utama.parquet')
 )

# COMMAND ----------

print(spdf_inferences.count(), len(spdf_inferences.columns))
spdf_inferences.printSchema()

# COMMAND ----------

spdf_joined = spdf_articles.join(spdf_inferences, spdf_articles.title ==  spdf_inferences.title,"left").drop(spdf_inferences['title'])
spdf_joined.limit(5).toPandas()

# COMMAND ----------

print(f"Shape before remove duplicates: {(spdf_joined.count(), len(spdf_joined.columns))}")
spdf_dropped_duplicates = spdf_joined.dropDuplicates(subset=['title', 'url'])
print(f"Shape after remove duplicates: {(spdf_dropped_duplicates.count(), len(spdf_dropped_duplicates.columns))}")

# COMMAND ----------

spdf_dropped_columns = spdf_dropped_duplicates.drop('created_epoch', 'created_utc_time')
print(f"Shape after dropping columns: {(spdf_dropped_columns.count(), len(spdf_dropped_columns.columns))}")

# COMMAND ----------

spdf_dropped_columns.limit(5).toPandas()

# COMMAND ----------

spdf_clean = spdf_dropped_columns.withColumn('sentiment', when(lower('sentiment')=='positive', 'positive')
                                             .when(lower('sentiment')=='negative', 'negative')
                                             .otherwise('unknown'))

# COMMAND ----------

spdf_clean = spdf_clean.withColumn('topic', lower(col('topic')))

# COMMAND ----------

spdf_clean.select("sentiment").distinct().show()

# COMMAND ----------

spdf_clean.write.mode("append").partitionBy("sentiment").parquet(f"abfss://silver-new@raefadls.dfs.core.windows.net/combined/{today}_hmetro_utama.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC # END

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md References
# MAGIC - https://stackoverflow.com/questions/78165804/pyspark-how-to-read-multiple-csv-files-with-different-column-positions-most-eff
# MAGIC
# MAGIC
