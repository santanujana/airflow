import os
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/root/airflow/tasks/airflow-315004-07174ebe0941.json"

bq_client = bigquery.Client()

query = """
	INSERT INTO `airflow-315004.milkyway.hackernews_stories` 

    SELECT id, `by`, score, time_ts, title, url
    FROM `bigquery-public-data.hacker_news.stories`
    WHERE EXTRACT(DATE FROM time_ts) = '2015-10-12';
"""

job = bq_client.query(query)
job.result()
