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
import datetime
import google.auth.transport.requests
import google.oauth2.id_token
from google.cloud import storage
from airflow.providers.google.common.utils import id_token_credentials as id_token_credential_utils
from google.auth.transport.requests import AuthorizedSession
import os
from airflow.exceptions import AirflowFailException

def invoke_cloud_function(api_uri, type, blob, url):
    print('****** api_uri:', api_uri)

    request = google.auth.transport.requests.Request()
    print('****** request:', request)

    id_token_credentials = id_token_credential_utils.get_default_id_token_credentials(api_uri, request=request)
    print('****** id_token_credentials:', id_token_credentials)

    try:
        headers = {"Content-Type": "application/json"}
        body = {"url":url, "type":type, "blob":blob}
        resp = AuthorizedSession(id_token_credentials).post(url=api_uri, json=body, headers=headers)
        print('****** resp:', resp)
        if resp.status_code == 200:
            return resp
        else:
            raise AirflowFailException("API Job failed. Aborting the Pipeline")

    except Exception as e:
        print('****** Exception:', e)


def run(api_uri, type, blob, url, **kwargs):

    response = invoke_cloud_function(api_uri, type, blob, url)
    print('***** response invoke_cloud_function', response)

    if not response.text == 'End of process!':
        raise AirflowFailException("API Job failed. Aborting the Pipeline")

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Starting the API Pipeline...")

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

bucket_name = os.environ.get("DRP_GCS")
bucket_name = bucket_name[5:]

storage_client = storage.Client()
source_bucket = storage_client.bucket(bucket_name)
blob = source_bucket.get_blob(f"config/config.json")
read_output = blob.download_as_text()
clean_data = json.loads(read_output)

with DAG(
        dag_id="API_pipeline",
        schedule_interval='@daily',
        start_date=yesterday,
        catchup=False,
) as dag:
    for endpoint in clean_data['endpoints']:
        PythonOperator(
            task_id=endpoint['blob'].replace("/load", ""),
            python_callable=run,
            provide_context=True,
            retries=0,
            op_kwargs={"api_uri":clean_data['api_uri'], "type":endpoint['type'], "blob":endpoint['blob'], "url":endpoint['url']},
        )
        pass
