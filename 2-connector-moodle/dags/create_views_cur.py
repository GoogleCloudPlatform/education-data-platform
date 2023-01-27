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


#Dataflow/BigQuery config file
bucket_name = os.environ.get("LOD_GCS_STAGING")
bucket_name = bucket_name[5:]
storage_client = storage.Client()
source_bucket = storage_client.bucket(bucket_name)
blob = source_bucket.get_blob(f"Files/config_files/config.json")
read_output = blob.download_as_text()
clean_data = json.loads(read_output)
dir_vw_cur = clean_data['dir_views_cur']
project_id_bq = clean_data['project_id_bq']
nm_dtst = clean_data['dataset_name']
proj_id_bq_cur = clean_data['prj_id_bq_cur']
dts_name_cur = clean_data['dts_nm_cur']

default_dag_args = {}

params_list = {
    "dir_views": dir_vw_cur,
    "prj_id_ld": project_id_bq,
    "dts_nm_ld": nm_dtst,
    "prj_id_cr": proj_id_bq_cur,
    "dts_nm_cr": dts_name_cur
    }

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

with DAG('create_views_cur',
         schedule_interval=None,
         start_date=yesterday,
         template_searchpath=['/home/airflow/gcs/data'],
         catchup=False) as dag:
    Bash = BashOperator(
        task_id="create_views",
        retries=0,
        bash_command="create_views_cur.sh",
        params=params_list,
        dag=dag
    )
    dummy_task_start = DummyOperator(task_id='start', retries=3)
    dummy_task_end = DummyOperator(task_id='end', retries=3)

    dummy_task_start >> Bash >> dummy_task_end
