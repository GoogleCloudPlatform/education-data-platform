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
import hashlib
import time
import logging
import json
from google.cloud import storage
from googleapiclient.discovery import build
import datetime
from airflow.exceptions import AirflowFailException
import os


def call_api_dataflow(project_id, region, table_name, url_template, driver_jar, dvrclsname, conn_url, query_sql,
                     output_table,
                     bq_temp_dir,
                     conn_user, conn_pass, bq_sa_email):
   try:
       # Set up the job_id
       id_job = hashlib.sha1(str(time.time()).encode("utf-8")).hexdigest()[:4]

       # Set up the Jobname
       job_name = 'job-{0}-{1}'.format(table_name, id_job)

       # Set up the parameters
       parameters = {
           'driverJars': driver_jar,
           'driverClassName': dvrclsname,
           'connectionURL': conn_url,
           'query': query_sql,
           'outputTable': output_table,
           'bigQueryLoadingTemporaryDirectory': bq_temp_dir,
           'username': conn_user,
           'password': conn_pass,
           'connectionProperties': 'tinyInt1isBit=false'
       }

       environment = {
           'serviceAccountEmail': bq_sa_email,
           'network': 'default',
           'subnetwork': 'https://www.googleapis.com/compute/v1/projects/{0}/regions/{1}/subnetworks/default'.format(
               project_id, region),
           'tempLocation': bq_temp_dir
       }

       dataflow = build('dataflow', 'v1b3', cache_discovery=False)
       request = dataflow.projects().locations().templates().launch(
           projectId=project_id,
           location=region,
           gcsPath=url_template,
           body={
               'jobName': job_name,
               'parameters': parameters,
               'environment': environment
           }
       )

       response = request.execute()

       df_job_id = response.get('job').get('id')

       return df_job_id

   except:
       df_job_id = 'ERR'
       return df_job_id

def call_api_status_dataflow(project_id, job_id, region):
   try:

       dataflow = build('dataflow', 'v1b3', cache_discovery=False)
       request = dataflow.projects().locations().jobs().get(
           projectId=project_id,
           location=region,
           jobId=job_id,
           view='JOB_VIEW_SUMMARY'
       )

       response = request.execute()

       status_job = response.get('currentState')

       return status_job

   except:
       status_job = 'ERR'
       request.close()
       return status_job

def read_file(bucket_name, file_name):
   try:
       storage_client = storage.Client()
       bucket = storage_client.bucket(bucket_name)
       blob = bucket.blob(file_name)

       with blob.open("r") as f:
           file_content = json.loads(f.read())

       return file_content

   except:
       file_content = "ERR"
       return file_content

def run():

   bucket_name = os.environ.get("LOD_GCS_STAGING")
   bucket_name = bucket_name[5:]
   config_path = "Files/config_files/config.json"
   sql_query_path = "Files/config_files/sql_query.json"
   config_file = read_file(bucket_name, config_path)

   project_id = config_file.get('project_id')
   region = config_file.get('region')
   project_id_bq = config_file.get('project_id_bq')
   url_template = config_file.get('url_template')
   url_template1 = url_template.format(region)

   driver_jar = config_file.get('driver_jar')
   driver_jar = 'gs://{0}/{1}'.format(bucket_name, driver_jar)
   dvrclsname = config_file.get('driverclsname')
   conn_url = config_file.get('conn_url')
   conn_user = config_file.get('conn_user')
   conn_pass = config_file.get('conn_pass')
   bq_temp_dir = config_file.get('bq_temp_dir')
   bq_temp_dir1 = 'gs://{0}/{1}/'.format(bucket_name, bq_temp_dir)
   bq_sa_email = config_file.get('bq_sa_email')
   max_df_instance = int(config_file.get("max_df_instance"))
   bq_dataset = config_file.get('dataset_name')

   df_jobid = []

   if config_file != 'ERR':

       num_df = 1

       for i in config_file['tables']:
           table_name = i

           # read the sql_query file to check if there is aspecific query
           query_file = read_file(bucket_name, sql_query_path)

           query_sql = query_file.get(table_name)

           if query_sql == None:
               query_sql = query_file.get("default")
               query_sql = query_sql.format(table_name)

           output_table = '{0}:{1}.{2}'.format(project_id_bq, bq_dataset, table_name)

           logging.info('Table: {}'.format(i))
           logging.info('Query: {}'.format(query_sql))
           logging.info('Output: {}'.format(output_table))

           if num_df <= max_df_instance:
               num_df += 1
               logging.info('Create Dataflow pipeline for table {}'.format(i))
               job_id = call_api_dataflow(project_id, region, table_name, url_template1, driver_jar, dvrclsname,
                                          conn_url,
                                          query_sql, output_table,
                                          bq_temp_dir1, conn_user, conn_pass, bq_sa_email)

               if job_id != 'ERR':
                   df_jobid.append(job_id)
                   logging.info('Job ID: {}'.format(job_id))
                   logging.info('----------------------------------')
                   time.sleep(3)
               elif job_id == 'ERR':
                   logging.info('Error to run Dataflow')
                   break

           elif num_df > max_df_instance:
               while len(df_jobid) + 1 > max_df_instance:
                   logging.info('Waiting to check status of Dataflow Jobs!')
                   logging.info('List of running Jobs: {}'.format(df_jobid))
                   time.sleep(15)
                   for job in df_jobid:
                       logging.info('Ckeck status of Job ID: {}'.format(job))
                       status_job = call_api_status_dataflow(project_id, job, region)
                       logging.info('Status Job Id: {} - {}'.format(job, status_job))

                       if (status_job != 'JOB_STATE_RUNNING'):

                           if (status_job == 'JOB_STATE_FAILED'):
                               raise AirflowFailException("Job failed. Aborting the Pipeline")

                           else:
                               logging.info('Removing the job id of the list - Job ID: {}'.format(job))
                               df_jobid.remove(job)
                               num_df = len(df_jobid) + 1

                       time.sleep(1)

                   logging.info('num_df: ' + str(num_df))

               # Call the DF Job for the table that was not executed yet, before exit the elif
               job_id = call_api_dataflow(project_id, region, table_name, url_template1, driver_jar, dvrclsname,
                                          conn_url,
                                          query_sql, output_table,
                                          bq_temp_dir1, conn_user, conn_pass, bq_sa_email)

               if job_id != 'ERR':
                   num_df += 1
                   df_jobid.append(job_id)
                   logging.info('Job ID: {}'.format(job_id))
                   logging.info('----------------------------------')
               elif job_id == 'ERR':
                   logging.info('Error to run Dataflow')
                   break

       while len(df_jobid) > 0:
           logging.info('Waiting to check status of remaing Dataflow Jobs!!')
           logging.info('List of running Jobs: {}'.format(df_jobid))
           time.sleep(15)
           for job in df_jobid:
               logging.info('Ckeck status of Job ID: {}'.format(job))
               status_job = call_api_status_dataflow(project_id, job, region)
               logging.info('Status Job Id: {} - {}'.format(job, status_job))

               if (status_job != 'JOB_STATE_RUNNING'):

                   if (status_job == 'JOB_STATE_FAILED'):
                       raise AirflowFailException("Job failed. Aborting the Pipeline")

                   else:
                       logging.info('Removing the job id of the list - Job ID: {}'.format(job))
                       df_jobid.remove(job)

               time.sleep(1)


if __name__ == '__main__':
   logging.getLogger().setLevel(logging.INFO)
   logging.info("Starting the moodle Pipeline...")
   run()

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

with DAG('moodle_pipeline', schedule_interval='@daily', start_date=yesterday, catchup=False) as dag:

   dummy_task_start = DummyOperator(task_id='start', retries=0)
   python_task = PythonOperator(task_id='execute', retries=0, python_callable=run)
   dummy_task_end = DummyOperator(task_id='end', retries=0)

dummy_task_start >> python_task >> dummy_task_end