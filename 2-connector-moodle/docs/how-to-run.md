
# How to run Moodle Connector 

Before running the pipelines to ingest data from Moodle, make sure both Cloud Build Triggers configured on [previous](../README.md/#2.-Configure-Moodle-Connector-variables) steps run successfully. After the deployment conclusion you will have all the resources needed to run the next steps.

## Cloud Composer

The Cloud Composer will be used as the orchestration solution for data ingestion pipelines from moodle database.

In the orchestration project ({your-prefix}-orc) you find a Cloud Composer instance running a managed version of Apache Airflow. Once the artifacts for Moodle Connector were deployed you are going to find the following Airflow [DAGs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html) in this project:

- moodle_pipeline
- create_table_bq
- create_views_cur

### moodle_pipeline

This DAG consists of gathering data from Moodle database (MySQL) and ingest it in BigQuery and is deployed with a default scheduling of running this pipeline once a day. You can customize this scheduling in Airflow according to your needs.

The following GCP Cloud resources are used in this pipeline:

- Cloud Storage
- Cloud Dataflow
- Cloud Composer

This is going to use Dataflow Jobs to ingest the list of tables you configured in the variable "tables" of config.json file. The pipeline will establish the connection with the Moodle Database, run a SELECT on the informed table and write the data in BigQuery landing zone.

The Mooddle Pipeline has a dynamic max tables load (Jobs DataFlow), to avoid overload the CPU quota form Cloud DataFlow machines and preventing errors. As a Load Jobs finish, new Jobs (loads from other tables) are initialized, until all tables listed in the config file are processed.

### create_table_bq

This DAG create all tables of Moodle 4.0 in BigQuery.

This process execution use the configurations listed on Dataflow/BigQuery config File to create the dataset and tables - project_id_qa, dataset_name and retention_data

To create the tables, the process uses the table's schema in Json format (same pattern/structure as in bigquery).

This framework already contains all table schemas of moodle version 4 (469 tables) and are available in the filemdl_schemas.tar.gz

The schema files must have the following name pattern:

```
schema.{moodle-table-name}.json
```

The structure of the mdl_schemas.tar.gz file must be:

```
mdl_schemas
   | schema.tabela1.json
   | Schema.tabela2.json
   | Schema.tabela3.json
  ...
```  

This DAG must be executed manualy before the execution of moodle_pipeline DAG, to create the datase/tables used by it.

### create_views_cur

This DAG is responsible for creating the Bigquery Views in the Data Warehouse project, Curated Layer.

This process execution use the configurations listed on Dataflow/BigQuery config File to create the Dataset - if necessary - and the views – project_id_bq, dataset_name, dir_views_cur, prj_id_bq_cur and dts_nm_cur

It is necessary to create a .sql file with the select of each view, separately.

In the View select, when informing the project_id.dataset_name.table_name, the following pattern below must be used, so that the process can create the view correctly.

Exemple:

```sql
select * from [prj_id_lnd].[dts_nm_lnd].mdl_course
```

It is important to know that the name of the view will be created according to the name of the .sql file

If the file name is v_mdl_course.sql, the view name will be v_mdl_course

The view(s) will be created in the project/dataset according to the prj_id_bq_cur and dts_nm_cur parameters.

The views must be in a file named mdl_views_cur.tar.gz

The structure of the mdl_views_cur.tar.gz file must be:

```
mdl_views_cur
  | v_mdl_courses.sql
  | v_mdl_grades.sql
  | v_mdl_users.sql
  ...
```

The file mdl_views_cur.tar.gz must be available in the bucket/folder where the Dataflow/BigQuery configuration files are located.
