# Introduction

The propose of this project is to serve as an example of a Composer Customer for Composer DAG. This ia sample DAG that can be used by projects to execute Big Queries. The scope also includes configuration files and BQ Queries that are used in the code.

## Cloud Composer Architecture

Cloud Composer is a managed Apache Airflow service that helps you create, schedule, monitor and manage workflows. Cloud Composer automation helps you create Airflow environments quickly and use Airflow-native tools, such as the powerful Airflow web interface and command line tools, so you can focus on your workflows and not your infrastructure.

You can learn more on the architecture of Cloud composer https://cloud.google.com/composer/docs/concepts/overview#architecture  

## The Key Components of the Architecture:

Cloud Storage ( DAGs, logs)

Cloud Storage provides the storage bucket for staging DAGs, plugins, data dependencies, and logs. To deploy workflows (DAGs), you copy your files to the bucket for your environment. Cloud Composer takes care of synchronizing the DAGs among workers, schedulers, and the web server. With Cloud Storage you can store your workflow artifacts in the data/ and logs/ folders without worrying about size limitations and retain full access control of your data.

GKE Cluster ( Airflow workers are scheduled)

Cloud Composer deploys core components—such as Airflow scheduler, worker nodes, and CeleryExecutor—in a GKE.

Apache Airflow directed acyclic graph (DAG)

DAGS runs in a Cloud Composer environment. An Airflow DAG is defined in a Python file and is composed of the following components: A DAG definition, operators, and operator relationships. Once the python script is loaded into the "dags" folder in Cloud Storage it is automatically picked up by Composer and is visible on airflow web.

# AIA Project Configurations

A Composer customer will have a processing project where Composer environment is created. The Composer will execute queries against the dataLake Project. The service account of the customer will have access to the DataLake Project on required datasets.The Processing Project will be differ as per the envionment sandbox, Non-Production , Production. A customer can have multiple Processing Projects.

Sample DAG

The sample DAG is used to execute a set of queries stored on Cloud Storage component of Composer.The query file are sequenced as they need to be ordered for execution.

Sample DAG has below :

## Project Configurations

projectlist.conf : The file contains Customer details including AIRId , AIA CustomerId, Processing Project Id , DataLake Project Id, Logging Table Name, Snow Topic Name, Snow Project Name. AIRId & AIA Customer Id are used as labels to the BigQueries that are passed when executing BigQuery to enable better tracability. Hence all these parameters are mandatory to be provided in project configuration. The file also contains dataproc_cluster_name and dataproc_bucket_name that will be passed to DataProcSparkOperator if you need to run spark job on dataproc through composer. The svcnow_topic_name and svcnow_project parameter is used to integrate SNOW ticket generation with DAG in case the DAG fails in the production. The Repository Structure of the same is /Composer/DAGS/conf_file The code looks for this file on the path "/home/airflow/gcs/dags/conf_files/projectlist.conf" in Composer. Hence, path & name of the file should not be changed.
They key names for air_id, customer_id, statcatchertable, environment, dataproc_cluster_name, dataproc_bucket_name, svcnow_project and svcnow_topic_name should not change as these key names have been used in the python modules of the DAG template.

## Python Code to Get Configurations

getconfig.py : This is a python script that reads projectlist.conf and reads all dml scripts stored in "/home/airflow/gcs/dags/<dagname>/dml" in Composer.The queries should have the order of execution in the name e.g. 01_executefirst ,02_executesecond. getconfig.py is present under common_modules folder and stored in "/home/airflow/gcs/dags/common_modules"

Python Code for logging and ingestion in partitioned tables of bigquery

jobexecmodule.py : This is a python script used for logging into statcatcher table and also for ingesting data into bigquery tables as per partition condition like insert overwrite or append.

## Python Code to Create Operators

samplecode_1.py : The Python scripts that calls getconfig.py, jobexecmodule.py and based on configured dml creates the task (PythonOperator , BigQUeryOperator , BigQueryCheckOperator , TriggerDAGrunOperator)


DML

The DML folder contains the ddl for temp table creations & data processing queries. The query file comtains the operator , task id , write disposition, destination_dataset_table. Task name in DAG that will be dynamically created when the code executes and hence give meaningful names.The sample code will dynamically pick up the name of the operator in the file and create tasks. Refer to the airflow link on Big Query operators https://airflow.apache.org/docs/stable/_api/airflow/contrib/operators/bigquery_operator/index.html  

job.properties

Contains parameters specific to the DAG

## Checks
•Use operators supported by google
•Review the Code Review Checklist
•pylint is used for static code analysis.
•Google offers support to Dry run the queries. This is configured in the Build pipeline.In order to update any variable during testing use sqlvar.json. In case there are no variables , store one entry with blank curly braces {}

# Logging

The statcatcher table creation is handled in the DAG template under common_modules folder in jobexecmodule.py
The statcatcher table is auto populated when you execute the DAG through getconfig. This is used for summary of DAG execution details. Note that the log table is is mandatory. The table structure of the log table is as :

dagid : is the qualifier to identify the dag by the name given in dagid execution_date: is the composer execution date startdttm_bqtimezone : is the start time of the bigquery timezone enddttm_bqtimezone: is the end time of the bigquery timezone status: states whether the DAG is success, failed or in-progress taskdetails :gives the task level information and status nextdag: gives the details of the next dag triggered through triggerdagrunoperator

# Operators supported by sample DAG

Bigqueryoperator: Executes BigQuery SQL queries in a specific BigQuery database. Refer the link for more details https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html  

pythonoperator: Operator will be python operator when you need to pass data from one operator to another. Refer the link for more details https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/python/index.html#module-airflow.operators.python  

Dmlbigqueryoperator: The dmlbigqueryoperator will be used for queries like update, delete or merge and there are no any derived values used in the query from any another operator Bigquerycheckoperator Performs the check against bigquery operator. Refer the link for more details https://airflow.apache.org/docs/apache-airflow/1.10.5/_api/airflow/contrib/operators/bigquery_operator/index.html  

Triggerdagrunoperator: To trigger other DAG and to set the dependency between the DAG’s. Refer the link for more details https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/trigger_dagrun/index.html#module-airflow.operators.trigger_dagrun  

<br/>

## DataProcSparkOperator
 To execute the spark jobs on dataproc. Example:

**Variable Name** | **Value** | **Variable Value Description**
--- | --- | ---|
operator|DataProcSparkOperator|operator should be DataProcSparkOperator to run spark job in dataproc cluster.
task_id|run_spark_job|task_id is the taskid for operator
arguments|gs://{dataproc_bucket_name}/code/,<br/> gs://{dataproc_bucket_name}/prop/job.properties|arguments are the program level arguments required for the spark job. This will be commma seperated values for multiple arguments
region|us-east1|region will be the region where the dataproc cluster is created
job_name|run_spark_job|job_name is the name by which dataproc job will be getting submitted appended by current timestamp
dataproc_spark_jars|gs://{dataproc_bucket_name}/jar/spark-bqdemo1-0.0.1-SNAPSHOT.jar,gs://{dataproc_bucket_name}/jar/spark-avro_2.11-2.4.4.jar|dataproc_spark_jars is the list of jars seperated by comma which is required for the spark job
main_class|com.spark.scala.getconftest2|main_class is the class name of the spark job to be run

<br/>

## MLEngineTrainingOperator
To execute machine learning jobs. Example:

**Variable Name** | **Value** | **Variable Value Description**
--- | --- | ---|
operator|MLEngineTrainingOperator|operator should be MLEngineTrainingOperator to run MLEngine training job.
task_id|train_model |task_id is the ID of task within a DAG to be executed
region|us-east1|region is the compute engine region where you want your job to run
training_python_module|trainer.task|training_python_module is the name of the main module in your package. The main module is the Python file you call to start the application.
job_id|'census_train_job'|job_id is the name to be used for the job (mixed-case letters, numbers, and underscores only, starting with a letter)
package_uris|gs://prd-65343-modelmgmt-ds-ml/census/package/census-deploy-package-0.1.tar.gz|package_uris is a packaged training application that is staged in GCS bucket to be used for model training

<br/>

## UploadModelOperator 
To upload models to model registry. Example:

**Variable Name** | **Value** | **Variable Value Description**
--- | --- | ---|
operator|vertexaiuploadmodeloperator|operator should be vertexaiuploadmodeloperator to upload models to model registry.
task_id|upload_model|task_id is the ID of task within a DAG to be executed
region|us-central1|region is the compute engine region where you want your job to run
project_processing|{project_id}|Project id of the processing project
model_artifact_uri|gs://{project_name}-vertexai/vertex/training-model-upload|Path to the directory containing the Model artifact and any of its supporting files.
model_serving_container_uri|us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.0-23:latest|The given variable value is an example of docker container image provided by Vertex AI that you run as pre-built container for serving predictions and explanations from trained model artifacts

<br/>

## DeleteModelOperator 
To delete models from model registry. Example:

**Variable Name** | **Value** | **Variable Value Description**
--- | --- | ---|
operator|vertexaideletemodeloperator|operator should be vertexaideletemodeloperator to delete model from model registry
task_id|delete_model|task_id is the ID of task within a DAG to be executed
region|us-central1|region is the compute engine region where you want your job to run
project_processing|{project_id}|project id of the processing project
refrence_task_id|upload_model|refrence_task_id is the ID of upload model task within a DAG to be executed **(should be exact same as the task_id for vertexaiuploadmodeloperator)**

<br/>

## ExportModelOperator 
To export models. Example:

**Variable Name** | **Value** | **Variable Value Description**
--- | --- | ---|
operator|vertexaiexportmodeloperator|operator should be vertexaiexportmodeloperator to export model from model registry
task_id|export_model|task_id is the ID of task within a DAG to be executed
region|us-central1|region is the compute engine region where you want your job to run
project_processing|{project_id}|project id of the processing project
refrence_task_id|upload_model|refrence_task_id is the ID of upload model task within a DAG to be executed **(should be exact same as the task_id for vertexaiuploadmodeloperator)**
export_format_id|custom-trained| the ID of the format in which the model must be exported
output_uri_prefix|gs://{project_name}-vertexai/vertexjob/azdo/prebuilt/census/model/|location where the .joblib file will be stored after exporting the model

<br/>

## CreateCustomPythonPackageTrainingJobOperator 
To train models written in python. Example:

**Variable Name** | **Value** | **Variable Value Description**
--- | --- | ---|
operator|vertexaipytrainingoperator|operator should be vertexaipytrainingoperator for training models using python
task_id|python_package_task|task_id is the ID of task within a DAG to be executed
staging_bucket|gs://{project_name}-vertexai/|Bucket to stage local model artifacts. Overrides staging_bucket set in aiplatform.init.
python_package_gcs_uri|gs://{project_name}-vertexai/vertexjob/azdo/prebuilt/census/vertex-census-pkg-0.1.tar.gz|python_package_gcs_uri is a packaged training application that is staged in GCS bucket to be used for model training
python_module_name|trainer.task|python_module_name is the name of the main module in your
container_uri|us-docker.pkg.dev/vertex-ai/training/scikit-learn-cpu.0-23:latest|The given variable value is an example of docker container image provided by Vertex AI that you run as pre-built container for serving predictions and explanations from trained model artifacts
replica_count|1| number of machine replicas the deployed model will be always deployed on
machine_type|n1-standard-4| The machine resources to be used for each node of the deployment.
region|us-central1|region is the compute engine region where you want your job to run
project_processing|{project_id}|project id of the processing project
base_output_dir|gs://{project_name}-vertexai/vertexjob/azdo/prebuilt/census/|GCS output directory of job. If not provided a timestamped directory in the staging directory will be used.
BUCKET_NAME|{project_name}-vertexai| name of the main gcs bucket where all vertex related file and folders are sotred
OUTPUT_DIR|vertexjob/azdo/prebuilt/census/model| location where .joblib file will be stored after training is completed.

<br/>

## CreateCustomContainerTrainingJobOperator 
To train models written in R. Example:

**Variable Name** | **Value** | **Variable Value Description**
--- | --- | ---|
operator|vertexairtrainingoperator|operator should be vertexairtrainingoperator for training models using R
task_id|custom_container_task|task_id is the ID of task within a DAG to be executed
staging_bucket|gs://{project_name}-vertexai/vertexjob/azdo/custom/regression/r-scripts/|Bucket to stage local model artifacts. Overrides staging_bucket set in aiplatform.init.
container_uri|gcr.io/{project_id}/customr:latest| location of the custom built docker image stored in the container registry
replica_count|1|number of machine replicas the deployed model will be always deployed on
machine_type|n1-standard-4| The machine resources to be used for each node of the deployment.
region|us-central1|region is the compute engine region where you want your job to run
project_processing|{project_id}|project id of the processing project
ENTRY_POINT|logistic_regression.R| main R script from where the execution starts
R_PACKAGE_PATH|gs://{project_name}-vertexai/vertexjob/azdo/custom/regression/r-scripts|location of the R scripts in gcs bucket to be used for model training in R

<br/>

# Snow Integration with sample DAG template

## publish.py 
This module is required for genertaing a SNOW ticket in case of DAG failure in production. In case of sandbox and nonproduction job failure no action will be taken.
svcnow_project and svcnow_topic_name are the mandatory parameters to be provided in the projectlist.conf file. The values of these keynames should remain same as specified in prd folder.
The values of svcnow_project and svcnow_topic_name can have dummy values in sandbox and nonprod as the snow tickets will be generated only if the environment is production.

## jobexecmodule.py 
This is a python script where the publish.py python module is being called if the task in the DAG fails.

## vertexaiexemodule.py
This is a python script mainly used for calling and executing vertex ai delete and export model operators. 

## projectlist.conf
projectlist.conf : The file contains Customer details including AIRId , AIA CustomerId, Processing Project Id , DataLake Project Id, Logging Table Name, Snow Topic Name, Snow Project Name.
They key names air_id, customer_id, svcnow_project and svcnow_topic_name are used for snow integration with DAG template.

## Important Note
1) The snow ticket will be raised against the queue configured in uservice against the AIRID.

2) Ensure that the application service account should have the publisher role to access to the topic App_65343_ss-aiadata-65343-prd-snowtrigger-ntf.

3) Ensure to comment the line in the module jobexecmodule.py where publish.py is getting called(as shown below) inorder to avoid triggering of snow tickets.

This could be needed during the
a-	Teams are troubleshooting issues in prod
b-	The jobs are in stabilize stage and have not been signed-off by Prod OPS
c-	Model management development stage

    if task_id == "failed_log":
        #publish.pubsubfunc(kwargs['air_id'], kwargs['customer_id'], failed_msg, kwargs['svcnow_project'], kwargs['svcnow_topic_name'])
        raise ValueError('Task failed')
    return 'success'

## Parallel Tasks Implementation

    To implement logic for parallel task, user should make changes in dag template files which includes – getconfig.py and gettasklist.py along with adding a new file named task_sequence.py.    

    A sample dag(65343_0001_sample_parallel_code_dag03) contains a python file which calls out the aforementioned files to implement the logic.

    Here is a link to the Reference_Guide : https://dev.azure.com/accenturecio05/AcnInsights_65343/_git/65343-aia-tf-v2-platform-examples-YAML?path=/Composer-Airflow2/DAGS/Reference_Guide
