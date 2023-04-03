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
from airflow.providers.google.cloud.operators.bigquery import BigQueryUpdateTableSchemaOperator
import os
from google.cloud import storage
import json
import logging


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Starting update Bigquery tables with polices...")

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

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

with DAG(
        dag_id="update_table_bq",
        schedule_interval=None,
        start_date=yesterday,
        max_active_tasks=5,
        template_searchpath=['/home/airflow/gcs/data'],
        catchup=False,
) as dag:
    for table in clean_data['tables_with_polices']:

        schema_table_location = "Files/config_files/mdl_schemas/schema." + table + ".json"
        schema_table = source_bucket.get_blob(schema_table_location)
        read_output_schema_table_txt = schema_table.download_as_text()
        read_output_schema_table = json.loads(read_output_schema_table_txt)

        BigQueryUpdateTableSchemaOperator(
            task_id="update_table_"+table,
            project_id=project_id_bq,
            dataset_id=nm_dtst,
            table_id=table,
            include_policy_tags=True,
            schema_fields_updates=read_output_schema_table
        )