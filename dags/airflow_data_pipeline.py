import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd

def csv_json():
    df = pd.read_csv("fakedata.csv")
    for deatils, records in df.iterrows():
        print(records['name'])
    df.to_json('convertcsv.json', orient='records')


#Build DAGS
args={
        'owner':'limoo',
        'start_date':dt.datetime(2023,2,5),
        'retries':1,
        'retry_delay':dt.timedelta(minutes=5),
    }

with DAG('csvjsonDAG',
        default_args = args,
        schedule_interval=timedelta(minutes=5),
        # '0 * * * *',
        ) as dag:
        starting_job = BashOperator(task_id = 'starting', bash_command='echo "Reading CSV file"')
        csvtojson = PythonOperator(task_id = 'convertCSVtoJSON', python_callable=csv_json)
        

        starting_job >> csvtojson



