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

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import json
import google.auth.transport.requests
import google.oauth2.id_token
from google.cloud import storage
from airflow.providers.google.common.utils import id_token_credentials as id_token_credential_utils
from google.auth.transport.requests import AuthorizedSession
import os
from airflow.exceptions import AirflowFailException


def invoke_cloud_function(service_url):
  url = service_url
  print('****** url:', url)

  request = google.auth.transport.requests.Request()
  print('****** request:', request)  

  id_token_credentials = id_token_credential_utils.get_default_id_token_credentials(url, request=request)
  print('****** id_token_credentials:', id_token_credentials)

  try:
    resp = AuthorizedSession(id_token_credentials).request("GET", url=url)
    print('****** resp:', resp)
    if resp.status_code == 200:
      return resp
    else:
      raise AirflowFailException("API Job failed. Aborting the Pipeline")

  except Exception as e:
    print('****** Exception:', e)  


def main():
    bucket_name = os.environ.get("DRP_GCS")
    bucket_name = bucket_name[5:]

    storage_client = storage.Client()
    source_bucket = storage_client.bucket(bucket_name)
    blob = source_bucket.get_blob(f"config/config.json")
    read_output = blob.download_as_text()
    clean_data = json.loads(read_output)

    response = invoke_cloud_function(clean_data['api_uri'])
    print('***** response invoke_cloud_function', response)

    if not response.text == 'End of process!':
      raise AirflowFailException("API Job failed. Aborting the Pipeline")


with DAG('API_pipeline', schedule_interval='@daily', start_date=datetime(2022, 12, 25), catchup=False) as dag:

   dummy_task_start = DummyOperator(task_id='start', retries=0)
   python_task = PythonOperator(task_id='call_API', retries=0, python_callable=main)
   dummy_task_end = DummyOperator(task_id='end', retries=0)

dummy_task_start >> python_task >> dummy_task_end