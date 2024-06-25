import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from base64 import b64encode

AIR_ID = "#{GCP_PROJECT_ID}#".split("-")[1]

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}
dag = DAG(
    dag_id='destroy_pubsub_composer',
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60))

start_dag = DummyOperator(task_id="start_dag", dag=dag)

destroy = PubSubPublishMessageOperator(
    dag=dag,
    task_id="destroy_task",
    project_id="#{GCP_PROJECT_ID}#",
    topic="App_"+AIR_ID+"_#{TOPIC_NAME}#",
    messages=[{'data': b'EXTERMINATE!'}]
)

end_dag = DummyOperator(task_id="end_dag", dag=dag)

start_dag >> destroy >> end_dag