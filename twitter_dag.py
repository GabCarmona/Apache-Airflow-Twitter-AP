import sys
sys.path.append("/home/gabriel/Documents/airflowalura")

from os.path import join
from airflow.models import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from operators.twitter_operator import TwitterOperator


with DAG('TwitterDAG', start_date = days_ago(6), schedule_interval="@daily") as dag:
        
        query = 'data science'

        to = TwitterOperator(file_path = join("datalake/twitter_datascience", 
                                              "extract_date={{ ds }}", 
                                              "datascience_{{ ds_nodash }}.json"), 
                                              query=query, 
                                              start_time= "{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
                                              end_time= "{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
                                              task_id = "twitter_query")