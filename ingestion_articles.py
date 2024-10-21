from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
import requests
from bs4 import BeautifulSoup
import html
import json
import re
from datetime import date
import adlfs

storage_account_key = dbutils.secrets.get(scope = "kotak-sakti-scope-111", key='raefadls-kunci-rahsia')
spark.conf.set("fs.azure.account.key.raefadls.dfs.core.windows.net", storage_account_key)


def get_body(url):
    article_url = f"https://www.hmetro.com.my/{url}"
    response = requests.get(article_url)
    soup = BeautifulSoup(response.text, "html.parser")
    body = soup.find_all("article-component")
    string = re.search(r"\{.*\}", str(body)).group(0)
    string_parsed_html = html.unescape(string)
    json_string = json.loads(string_parsed_html)
    return json_string["body"]


def extract_webpages():
    lst_webpages = []
    for i in range(10):
        response = requests.get(f"https://www.hmetro.com.my/api/topics/169?page={i}")
        # time.sleep(2)
        data = response.json()
        lst_webpages.extend(data)
    return lst_webpages


def get_dataset(lst_webpages: list) -> pd.DataFrame:
    dataset = []
    for index, item in enumerate(lst_webpages):

        observations = {}
        observations['title'] = item['title']
        observations['created_epoch'] = item['created']
        observations['url'] = item['url']
        try:
            observations['author_name'] = item['field_article_author']["name"]
        except:
            observations['author_name'] = None
        observations['word_count'] = item['word_count']

        dataset.append(observations)
    df = pd.DataFrame(dataset)
    df = df.assign(created_utc_time = pd.to_datetime(df['created_epoch'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S'))
    df = df.assign(created_local_time = pd.to_datetime(df['created_utc_time'],format='%Y-%m-%d %H:%M:%S') + pd.Timedelta(hours=8, minutes=0, seconds=0)).astype({'created_local_time':'object'})
    return df


def transform_pipeline(df):
    df["body"] = df["url"].apply(get_body)
    df = df.assign(
        body=df.body.str.replace(
            r"<\/?[a-zA-Z][^>]*>|(©.*Bhd)|(^[\w\d@.]*.my)", "", regex=True
        ).str.strip()
    )
    return df


def load_data(df):
    today = date.today()
    # df = spark.createDataFrame(df)
    #TODO: change write mode to append
    storage_account_key = dbutils.secrets.get(scope = "kotak-sakti-scope-111", key='raefadls-kunci-rahsia')
    storage_options={'account_key': storage_account_key}
    df.to_parquet(f'abfs://bronze-new@raefadls.dfs.core.windows.net/articles/{today}_hmetro_utama.parquet', storage_options=storage_options)
    # df.write.mode("overwrite").parquet(f"abfss://bronze@raefadls.dfs.core.windows.net/{today}_hmetro_utama.parquet")


def main():
    webpages = extract_webpages()
    df = get_dataset(webpages)
    df = transform_pipeline(df)
    load_data(df)


if __name__ == "__main__":
    main()


