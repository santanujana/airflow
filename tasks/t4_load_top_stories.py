import os
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/root/airflow/tasks/airflow-315004-07174ebe0941.json"

bq_client = bigquery.Client()

query = """
	INSERT INTO `airflow-315004.milkyway.top_stories`

    SELECT id, `by`, score, EXTRACT(DATE FROM time_ts) as dt, title, url
    FROM `milkyway.hackernews_stories` 
    ORDER BY score DESC 
    LIMIT 10;
"""

job = bq_client.query(query)
job.result()
