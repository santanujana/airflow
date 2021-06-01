import os
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/root/airflow/tasks/airflow-315004-07174ebe0941.json"

bq_client = bigquery.Client()

query = """
	TRUNCATE TABLE `airflow-315004.milkyway.top_stories`;
"""

job = bq_client.query(query)
job.result()
