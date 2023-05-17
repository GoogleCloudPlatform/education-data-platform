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
from google.cloud import storage
import json
import os


#API config File
bucket_name = os.environ.get("DRP_GCS")
bucket_name = bucket_name[5:]
storage_client = storage.Client()
source_bucket = storage_client.bucket(bucket_name)
blob = source_bucket.get_blob(f"config/config.json")
read_output = blob.download_as_text()
clean_data = json.loads(read_output)
bucket_api = clean_data['bucket']
api_path = clean_data['api_path']
path_file_api = 'gs://{0}/{1}'.format(bucket_api, api_path)
project_id_bq = clean_data['project_id_bq']
bq_dataset = clean_data['dataset_name']
ret_time = int(clean_data['retention_data'])
location = clean_data['location']

default_dag_args = {}

params_list = {
    "prj_id": project_id_bq,
    "dtst_nm": bq_dataset,
    "dir_orig": path_file_api,
    "ret_time": ret_time,
    "location": location
    }

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

with DAG('bq_load',
         schedule_interval='30 2 * * *',
         start_date=yesterday,
         template_searchpath=['/home/airflow/gcs/data'],
         catchup=False) as dag:
    Bash = BashOperator(
        task_id="load_api_files_bq",
        retries=0,
        bash_command="bq_load.sh",
        params=params_list,
        dag=dag
    )

    dummy_task_start = DummyOperator(task_id='start', retries=0)
    dummy_task_end = DummyOperator(task_id='end', retries=0)

    dummy_task_start >> Bash >> dummy_task_end
