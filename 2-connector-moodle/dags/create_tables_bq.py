# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
import os
from google.cloud import storage
import json


#Dataflow/BigQuery config file
bucket_name = os.environ.get("LOD_GCS_STAGING")
bucket_name = bucket_name[5:]
storage_client = storage.Client()
source_bucket = storage_client.bucket(bucket_name)
blob = source_bucket.get_blob(f"Files/config_files/config.json")
read_output = blob.download_as_text()
clean_data = json.loads(read_output)
project_id_bq = clean_data['project_id_bq']
dir_schm = clean_data['dir_schm']
ret_time = int(clean_data['retention_data'])
nm_dtst = clean_data['dataset_name']
default_dag_args = {}

params_list = {
    "dir_schm": dir_schm,
    "ret_time": ret_time,
    "nm_dtst": nm_dtst,
    "prj_id": project_id_bq,
    }

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

with DAG('create_table_bq',
         schedule_interval=None,
         start_date=yesterday,
         template_searchpath=['/home/airflow/gcs/data'],
         catchup=False) as dag:
    Bash = BashOperator(
        task_id="create_tables",
        retries=0,
        bash_command="create_tables_bq.sh",
        params=params_list,
        dag=dag
    )
    dummy_task_start = DummyOperator(task_id='start', retries=3)
    dummy_task_end = DummyOperator(task_id='end', retries=3)

    dummy_task_start >> Bash >> dummy_task_end
