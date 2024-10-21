import streamlit as st
import time
import pandas as pd
import numpy as np
import math
import datetime
from image_loader import render_image

session = st.connection('snowflake').session()

MARKDOWN_INTRO = """
## Introduction
This dashboards provides the output and metrics of articles scraped from a paritcular news website under the *Utama* category.
There is summarization, sentiment and topics inferred by an LLM based on the articles under the summarization tab. 
The data will be refreshed on a daily basis.
"""

# Change the query to point to your table
def get_data(_session):
    query = """
    select * EXCLUDE (created_local_time_casted), created_local_time_casted::TIMESTAMP created_local_time  
    from FINAL_PROJECT.SCRAPED_NEWS.HMETRO_UTAMA
    order by created_local_time desc
    """
    data = _session.sql(query).collect()
    return data


def stream_data(summary):
    for word in summary.split(" "):
        yield word + " "
        time.sleep(0.05)


# st.set_page_config(page_title="Bug report", layout="centered")

st.title('📰 :blue[LLM Article Summarization]')

df = pd.DataFrame(get_data(session))


tab1, tab2, tab3 = st.tabs(['About',' Summarization', 'Pipeline'])

with tab1:
    
    st.markdown(MARKDOWN_INTRO)


    st.subheader('Metrics')
    col1,col2 = st.columns(2,gap="medium" )

    
    with col1:
        
        st.metric(label = 'Number of articles scraped', value=df.shape[0])

        st.metric(label = 'Average articles per day', value=math.ceil(df
                                                               .groupby(df.CREATED_LOCAL_TIME.dt.date)
                                                               .agg({'TITLE':'nunique'})
                                                               .mean()))
    
        
    with col2:
        st.metric(label = 'Number of topics inferred', value=df.TOPIC.nunique())

        st.metric(label = 'Average word count per article', value=math.ceil(df
                                                                   .groupby(df.CREATED_LOCAL_TIME.dt.date)
                                                                   .agg({'WORD_COUNT':'mean'})
                                                                   .mean()))
        

with tab2:
    st.markdown('#### Filters')
    col1,col2 = st.columns(2,gap="small" )
    with col1:
        min_date = st.date_input(label='Select a date',
                                  value=datetime.date.today() - datetime.timedelta(days = 1),
                                  min_value=df.CREATED_LOCAL_TIME.dt.date.min(),
                                  max_value=df.CREATED_LOCAL_TIME.dt.date.max(),
                                 format="YYYY-MM-DD")
    with col2:
        topic_selection = st.selectbox(label='Select a topic', options = set(df.set_index('CREATED_LOCAL_TIME').loc[str(min_date)].TOPIC.to_list()), index=None)


    with st.container(border=True):
        if topic_selection is not None:
            article_selection = st.selectbox(label='Select an article',options=df.set_index('CREATED_LOCAL_TIME').loc[(str(min_date))].query(f'TOPIC == "{topic_selection}"').TITLE, index=None)
        else:
            article_selection = st.selectbox(label='Select an article',options=df.set_index('CREATED_LOCAL_TIME').loc[(str(min_date))].TITLE,index=None)
        summary = df.query(f'TITLE == "{article_selection}"').SUMMARY
        sentiment = df.query(f'TITLE == "{article_selection}"').SENTIMENT
        # st.write(summary.iloc[0])
        if article_selection is not None:
            if sentiment.iloc[0] == 'uknown':
                st.write_stream(stream_data(summary.iloc[0] + f'\n\n :blue[Sentiment]: :gray[{sentiment.iloc[0]}]'))
            elif sentiment.iloc[0] == 'positive':
                st.write_stream(stream_data(summary.iloc[0] + f'\n\n :blue[Sentiment]: :green[{sentiment.iloc[0]}]👍🏼'))
            elif sentiment.iloc[0] == 'negative':
                st.write_stream(stream_data(summary.iloc[0] + f'\n\n :blue[Sentiment]: :red[{sentiment.iloc[0]}]👎🏼'))
            else:
                st.write_stream(stream_data(summary.iloc[0] + f'\n\n :blue[Sentiment]: :gray[{sentiment.iloc[0]}]'))


with tab3:
    st.markdown("#### 1. Find a useful API endpoint")
    render_image('Screenshot 2024-10-19 at 2.48.57 AM.png')
    st.markdown("#### 2. Inspect the JSON response")
    render_image('Screenshot 2024-10-19 at 2.47.07 AM.png')
    st.markdown("#### 3. Retrieve article by parsing the HTML document")
    render_image('Screenshot 2024-10-19 at 2.48.00 AM.png')
    st.markdown("#### 4. Create the scripts for the ETL pipeline")
    render_image('Screenshot 2024-10-19 at 2.49.24 AM.png')
    st.markdown("#### 5. Dump scraped data on Hugging Face as backup")
    render_image('Screenshot 2024-10-19 at 2.51.32 AM.png')
    st.markdown("#### 6. Do prompt engineering to getsuitable LLM inferences")
    render_image('Screenshot 2024-10-19 at 3.25.16 AM.png')
    st.markdown("#### 7. Process data using PySpark")
    render_image('Screenshot 2024-10-19 at 2.50.40 AM.png')
    st.markdown("#### 8. Load data to Snowflake")
    render_image('Screenshot 2024-10-19 at 2.52.49 AM.png')
    st.markdown("#### 9. Orchestrate the pipeline using Databricks Workflow")
    render_image('Screenshot 2024-10-19 at 2.46.17 AM.png')
    st.markdown("#### 10. Develop this Streamlit WebApp in Snowflake")
    render_image('Screenshot 2024-10-19 at 2.51.54 AM.png')
    st.markdown("#### 11. Manage roles to allow public access to this web app")
    render_image('Screenshot 2024-10-19 at 3.22.32 AM.png')
    