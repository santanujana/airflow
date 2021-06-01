from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Santanu',
}

dag = DAG(
    dag_id='hacker_news_santanu',
    default_args=args,
    schedule_interval='0 * * * *',
    start_date=days_ago(0)
)

task1 = BashOperator(
    task_id="t1_truncate_stories",
    bash_command='python /root/airflow/tasks/t1_truncate_stories.py',
    dag=dag
)

task2 = BashOperator(
    task_id="t2_truncate_top_stories",
    bash_command='python /root/airflow/tasks/t2_truncate_top_stories.py',
    dag=dag
)

task3 = BashOperator(
    task_id="t3_fetch_stories",
    bash_command='python /root/airflow/tasks/t3_fetch_stories.py',
    dag=dag
)

task4 = BashOperator(
    task_id="t4_load_top_stories",
    bash_command='python /root/airflow/tasks/t4_load_top_stories.py',
    dag=dag
)

task1 >> task2
task2 >> task3
task3 >> task4
