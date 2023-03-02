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
from airflow.operators.bash_operator import BashOperator

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

# Gets the state of the specified Cloud Dataflow job.
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

# Launch a Dataflow template.
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

def run(config_file,table_name,**kwargs):

   sql_query_path = "Files/config_files/sql_query.json"
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

   if config_file != 'ERR':

        # read the sql_query file to check if there is aspecific query
        query_file = read_file(bucket_name, sql_query_path)

        query_sql = query_file.get(table_name)

        if query_sql == None:
            query_sql = query_file.get("default")
            query_sql = query_sql.format(table_name)

        output_table = '{0}:{1}.{2}'.format(project_id_bq, bq_dataset, table_name)

        logging.info('Table: {}'.format(table_name))
        logging.info('Query: {}'.format(query_sql))
        logging.info('Output: {}'.format(output_table))

        # Call the DF Job for the table that was not executed yet, before exit the elif
        job_id = call_api_dataflow(project_id, region, table_name, url_template1, driver_jar, dvrclsname,
                                          conn_url,
                                          query_sql, output_table,
                                          bq_temp_dir1, conn_user, conn_pass, bq_sa_email)

        executing_states = ['JOB_STATE_PENDING', 'JOB_STATE_RUNNING', 'JOB_STATE_CANCELLING']

        # final states do not change further
        final_states = ['JOB_STATE_DONE', 'JOB_STATE_FAILED', 'JOB_STATE_CANCELLED']

        # Check states of Dataflow Job
        while True:
            logging.info('Ckeck status of Job ID: {}'.format(job_id))
            status_job = call_api_status_dataflow(project_id, job_id, region)
            logging.info('Status Job Id: {} - {}'.format(job_id, status_job))
            if status_job in executing_states:
                pass
            elif status_job in final_states:
                if status_job == "JOB_STATE_FAILED" or status_job == "JOB_STATE_CANCELLED":
                    raise AirflowFailException("Job failed")
                break
            time.sleep(10)



if __name__ == '__main__':
   logging.getLogger().setLevel(logging.INFO)
   logging.info("Starting the moodle Pipeline...")

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

bucket_name = os.environ.get("LOD_GCS_STAGING")
bucket_name = bucket_name[5:]
config_path = "Files/config_files/config.json"
config_file = read_file(bucket_name, config_path)
max_tasks = int(config_file.get('max_df_instance'))

with DAG(
    dag_id="moodle_pipeline",
    schedule_interval='@daily',
    start_date=yesterday,
    max_active_tasks=max_tasks,
    catchup=False,
) as dag:
    for table in config_file['tables']:

        PythonOperator(
            task_id=table,
            python_callable=run,
            provide_context=True,
            retries=0,
            op_kwargs={"config_file":config_file,"table_name":table},
        )
        pass