# Databricks notebook source
# MAGIC %md
# MAGIC # START

# COMMAND ----------

from openai import OpenAI
from datetime import date
import pandas as pd
import json



# COMMAND ----------

today = date.today()
storage_account_key = dbutils.secrets.get(scope = "kotak-sakti-scope-111", key='raefadls-kunci-rahsia')
storage_options={'account_key': storage_account_key}
lpdf = pd.read_parquet(f'abfs://bronze-new@raefadls.dfs.core.windows.net/articles/{today}_hmetro_utama.parquet', storage_options=storage_options)
lpdf

# COMMAND ----------

lst_title = lpdf.title.to_list()
lst_body = lpdf.body.to_list()
lst_tuple_title_body = list(zip(lst_title, lst_body))

# COMMAND ----------

def prompt_article_format(text):

    USER_PROMPT = """
    I will share with you an article in Bahasa Melayu. The articles are from a Malaysian news website. I want you to create a an article summary, extract a topic, and determine the sentiment.\
    Follow the steps below as guideline to execute this task.

    1. Read through the whole article.
    2. Determine the main idea and supporting texts. It may be facts or opinions.
    3. Summarize the article.
    4. Infer a topic from the article. A topic should be only a word or two. For example badminton, crypto, artificial intelligence and many more.
    5. Determine the sentiment of the  article. It should either be positive, unknown or negative only.

    The article is delimeted in triple backticks. \
    ```{}```
    Respond in JSON format with the keys being summary, topic, sentiment.\
    Your output must always be in valid JSON format so that I can parse it using Python.
    Do not give me a markdown reponse.
    """

    openai_api_key = dbutils.secrets.get(scope = "kotak-sakti-scope-111", key='openai-api-raef')
    client = OpenAI(api_key=openai_api_key)
    completion = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "user", "content": USER_PROMPT.format(text)}
        ]
    )
    return completion.choices[0].message.content

# COMMAND ----------

def parse_json_markdown(json_string: str) -> dict:
    # Remove the triple backticks if present
    json_string = json_string.strip()
    start_index = json_string.find("```json")
    end_index = json_string.find("```", start_index + len("```json"))

    if start_index != -1 and end_index != -1:
        extracted_content = json_string[start_index + len("```json"):end_index].strip()
        
        # Parse the JSON string into a Python dictionary
        parsed = json.loads(extracted_content)
    elif start_index != -1 and end_index == -1 and json_string.endswith("``"):
        end_index = json_string.find("``", start_index + len("```json"))
        extracted_content = json_string[start_index + len("```json"):end_index].strip()
        
        # Parse the JSON string into a Python dictionary
        parsed = json.loads(extracted_content)
    elif json_string.startswith("{"):
        # Parse the JSON string into a Python dictionary
        parsed = json.loads(json_string)
    else:
        raise Exception("Could not find JSON block in the output.")

    return parsed

# COMMAND ----------

dataset = []
for item in lst_tuple_title_body:
    dict_test = {}
    response = prompt_article_format(item[1])
    json_repsonse = parse_json_markdown(response)
    dict_test['title'] = item[0]
    dict_test['summary'] = json_repsonse['summary']
    dict_test['topic'] = json_repsonse['topic']
    dict_test['sentiment'] = json_repsonse['sentiment']
    dataset.append(dict_test)

lpdf_infer = pd.DataFrame(dataset)

# COMMAND ----------

lpdf_infer.to_parquet(f'abfs://bronze-new@raefadls.dfs.core.windows.net/inferred/{today}_hmetro_utama.parquet', storage_options=storage_options)

# COMMAND ----------

# MAGIC %md
# MAGIC # END

# COMMAND ----------

# test  = prompt_article_format(lst_tuple_title_body[0][1])
# parse_json_markdown(test)

# COMMAND ----------

# prompt_article_format(lst_tuple_title_body[0][1])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ```json
# MAGIC
# MAGIC {
# MAGIC "topic" : "something",
# MAGIC "sentiment" : "
# MAGIC }
