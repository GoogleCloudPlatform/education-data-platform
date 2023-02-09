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

import requests
import json
import csv
import hashlib
import os
import time

from google.cloud import storage

bucket_name = os.environ['BUCKET_ID']

def main(request):

    request_json = request.get_json()
    url = request_json['url']
    type_file = request_json['type']
    blob_from_config = request_json['blob']

    storage_client = storage.Client()
    source_bucket = storage_client.bucket(bucket_name)

    blob = source_bucket.get_blob(f"config/config.json")

    read_output = blob.download_as_text()
    clean_data = json.loads(read_output)
    api_path = clean_data['api_path']

    if type_file == 'json':
        try:
            response = requests.get(url)

            if response.status_code == 200:

                source_bucket_output = storage_client.bucket(clean_data['bucket'])

                hash = hashlib.md5(str(time.time()).encode())
                filename = f"file_{hash.hexdigest()}.json"

                blob_output = source_bucket_output.blob(f"{api_path}/{blob_from_config}/{filename}")
                blob_output.upload_from_string(
                    data=json.dumps(response.json()),
                    content_type='application/json'
                )
                print('****** File Created:', blob_output)

        except Exception as e:
            print('****** Exception JSON:', e)


    elif type_file == 'csv':

        try:
            response = requests.get(url)

            if response.status_code == 200:

                source_bucket_output = storage_client.bucket(clean_data['bucket'])

                hash = hashlib.md5(str(time.time()).encode())
                filename = f"file_{hash.hexdigest()}.csv"
                blob_output = source_bucket_output.blob(f"{api_path}/{blob_from_config}/{filename}")
                blob_output.upload_from_string(
                    data=csv.writer(response.text),
                    content_type='application/CSV'
                )

                print('****** File Created:', blob_output)

        except Exception as e:
            print('****** Exception CSV:', e)


    return 'End of process!'