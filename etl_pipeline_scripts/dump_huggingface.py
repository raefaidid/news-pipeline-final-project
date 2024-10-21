# Databricks notebook source
# MAGIC %md
# MAGIC # START

# COMMAND ----------

# MAGIC %pip install huggingface_hub==0.25.2

# COMMAND ----------

from huggingface_hub import HfApi, create_repo
from datetime import date
import pandas as pd
import adlfs


# COMMAND ----------

today = date.today()
storage_account_key = dbutils.secrets.get(scope = "kotak-sakti-scope-111", key='raefadls-kunci-rahsia')
storage_options={'account_key': storage_account_key}

hf_write_token_lulz  = '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'


# COMMAND ----------

lpdf_articles = pd.read_parquet(f'abfs://bronze-new@raefadls.dfs.core.windows.net/articles/{today}_hmetro_utama.parquet', storage_options=storage_options)

lpdf_articles.to_parquet(f'/Workspace/Users/raef.aidid@gmail.com/Databricks-Learning/news_pipeline/temp/{today}_articles.parquet')

create_repo(f"articles_{today}", repo_type="dataset", token = hf_write_token_lulz)

api = HfApi()
api.upload_file(
    path_or_fileobj=f'/Workspace/Users/raef.aidid@gmail.com/Databricks-Learning/news_pipeline/temp/{today}_articles.parquet',
    path_in_repo=f"{today}_articles.parquet",
    repo_id=f"raefdd/articles_{today}",
    repo_type="dataset",
    token = hf_write_token_lulz
)

# COMMAND ----------

lpdf_articles = pd.read_parquet(f'abfs://bronze-new@raefadls.dfs.core.windows.net/inferred/{today}_hmetro_utama.parquet', storage_options=storage_options)

lpdf_articles.to_parquet(f'/Workspace/Users/raef.aidid@gmail.com/Databricks-Learning/news_pipeline/temp/{today}_inferences.parquet')

create_repo(f"article_inferences_{today}", repo_type="dataset", token = hf_write_token_lulz)

api = HfApi()
api.upload_file(
    path_or_fileobj=f'/Workspace/Users/raef.aidid@gmail.com/Databricks-Learning/news_pipeline/temp/{today}_inferences.parquet',
    path_in_repo=f"{today}_inferences.parquet",
    repo_id=f"raefdd/article_inferences_{today}",
    repo_type="dataset",
    token = hf_write_token_lulz
)

# COMMAND ----------

# MAGIC %md
# MAGIC # END

# COMMAND ----------


# %sh

# pwd

# COMMAND ----------



# COMMAND ----------


