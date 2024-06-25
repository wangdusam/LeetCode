"""this file is needed to get list of tasks. file is available under /DAGS/common_modules"""
import sys
import os
import uuid
from airflow.configuration import conf
#from airflow.utils.task_group import TaskGroup
#from common_modules.getsubtasklist import get_subtask
import json
# pylint: disable=E0602
# pylint: disable=E0401
# pylint: disable=C0103
# pylint: disable=W0106
# pylint: disable=C0115
# pylint: disable=R0914
# pylint: disable=R0124
# pylint: disable=W0122
# pylint: disable=E1101
# pylint: disable=R0903
# pylint: disable=R0912
dagfolder = conf.get('core', 'DAGS_FOLDER')
#importlibpath = os.path.abspath(os.path.dirname(__file__)) + "/import.lib"
importlibpath = dagfolder + "/common_modules/import.lib"
# import all libaray from conf_files/import.lib file
import_lib = open(importlibpath, "r")
exec(import_lib.read())
# get internal path of composer from `os` class
pth = os.path.abspath(os.getcwd())
sys.path.append(pth)
pth1 = os.path.dirname(os.path.abspath(__file__))
sys.path.append(pth1)
class SafeDict(dict):
    ''' Return key value pairs'''
    def __missing__(self, key):
        return '{' + key + '}'
class Var:
    ''' Initialize variables'''
    var = "dummy"
    task_id = ""
    sql = ""
    destination_dataset_table = ""
    write_disposition = ""
    bigquery_conn_id = ""
    use_legacy_sql = ""
    create_disposition = ""
    trigger_dag_id = ""
    trigger_rule = ""
    operator = ""
    pass_value = ""
    customer_id = ""
    arguments = ""
    environment = ""
    air_id = ""
    dataproc_cluster_name = ""
    dataproc_bucket_name = ""
    dataproc_jars = ""
def get_task(storage_name, rootdir, dagid, confprojectlistfile):
    '''Get Task Details'''
    tasklist = ''
    nextdag = ''
    nextdagid = 0
    data = getconfig.fngetdata(storage_name, rootdir, dagid, confprojectlistfile)
    # Place the below in getconfig to return the value
    data = dict(sorted(data.items(), key=operator.itemgetter(0)))
    logging.info(data)
    gscopr = []
    for id, detail in data.items():
        print("----")
        print(id)
        setattr(Var, 'task_id', ''),
        setattr(Var, 'sql', ''),
        setattr(Var, 'destination_dataset_table', 'None'),
        setattr(Var, 'write_disposition', 'WRITE_EMPTY'),
        setattr(Var, 'bigquery_conn_id', None),
        setattr(Var, 'use_legacy_sql', False),
        setattr(Var, 'create_disposition', 'CREATE_IF_NEEDED'),
        setattr(Var, 'trigger_dag_id', ''),
        setattr(Var, 'trigger_rule', 'all_success')
        for key in detail:
            setattr(Var, key, detail[key])
        sqlstmt = Var.sql

        if Var.operator.lower() == 'triggerdagrunoperator':
            varnextdag = str(nextdagid) + ":{'id':'" + str(id) + "','taskid':'" \
                         + Var.task_id + "'," \
                                         "'trigger_dag_id':'" + Var.trigger_dag_id + "'}"
            nextdag = nextdag + "," + varnextdag if nextdag != '' else varnextdag
            nextdagid = nextdagid + 1
        if Var.task_id == "success_log" or Var.task_id == "failed_log":
            detail['tasklist'] = tasklist
            detail['nextdag'] = nextdag
        else:
            tasklist = tasklist + ',' + Var.task_id + '_' + str(id) \
                if tasklist != '' else Var.task_id + '_' + str(id)
            detail['tasklist'] = Var.task_id + '_' + str(id)
            detail['nextdag'] = ''
        if id == id:
            if Var.operator.lower() == 'dummyoperator':
                gscopr.append(DummyOperator(
                    task_id=Var.task_id + '_' + str(id),
                    trigger_rule=Var.trigger_rule
                ))
            #if Var.operator.lower() == 'taskgroup':
            #    gscopr.append(get_subtask(Var.storage_name, Var.rootdir, dagid, confprojectlistfile,
            #                              Var.task_id + '_' + str(id)))

            if Var.operator.lower() == 'dmlbigqueryoperator':
                gscopr.append(BigQueryInsertJobOperator(
                    task_id=Var.task_id + '_' + str(id),
                    configuration={
                        "query": {
                            "query": sqlstmt,
                            "useLegacySql": Var.use_legacy_sql
                        }},
                    trigger_rule=Var.trigger_rule
                ))
            elif Var.operator.lower() == 'custombqpythonoperator':
                gscopr.append(PythonOperator(
                    task_id=Var.task_id + '_' + str(id),
                    provide_context=True,
                    python_callable=jobexecmodule.JobExec,
                    trigger_rule=Var.trigger_rule,
                    op_kwargs=detail
                ))
            elif Var.operator.lower() == 'bigqueryvaluecheckoperator':
                gscopr.append(BigQueryValueCheckOperator(
                    task_id=Var.task_id + '_' + str(id),
                    pass_value=Var.pass_value,
                    sql=Var.sql,
                    use_legacy_sql=Var.use_legacy_sql
                ))
            elif Var.operator.lower() == 'bigqueryoperator':
                projectId = Var.destination_dataset_table.split(".")[0]
                datasetId = Var.destination_dataset_table.split(".")[1]
                tableId = Var.destination_dataset_table.split(".")[2]
                gscopr.append(BigQueryInsertJobOperator(
                    task_id=Var.task_id + '_' + str(id),
                    force_rerun=False,
                    configuration={
                        "query": {
                            "query": Var.sql,
                            "destinationTable": {
                                "projectId": projectId,
                                "datasetId": datasetId,
                                "tableId": tableId
                            },
                            "useLegacySql": Var.use_legacy_sql,
                            "writeDisposition": Var.write_disposition,
                            "createDisposition": Var.create_disposition
                        }}
                ))
            elif Var.operator.lower() == 'triggerdagrunoperator':
                gscopr.append(TriggerDagRunOperator(
                    task_id=Var.task_id + '_' + str(id),
                    trigger_dag_id=Var.trigger_dag_id
                ))
            
            elif Var.operator.lower() == 'dataprocsparkoperator':
                argument = Var.arguments + "," + Var.environment
                argument = argument.replace('{dataproc_bucket_name}',Var.dataproc_bucket_name)
                dataproc_jars = Var.dataproc_jars.replace('{dataproc_bucket_name}',Var.dataproc_bucket_name)
                SPARK_JOB = {
                    "reference": {"project_id": Var.project_processing},
                    "placement": {"cluster_name": Var.dataproc_cluster_name},
                    "spark_job": {
                            "jar_file_uris": dataproc_jars.split(","),
                            "main_class": detail['main_class'],
                            "args":argument.split(",")
                            
                        },
                    }  
                gscopr.append(DataprocSubmitJobOperator(
                    task_id=Var.task_id + '_' + str(id),
                    region=detail['region'],
                    job=SPARK_JOB,
                    project_id=Var.project_processing
                ))
            elif Var.operator.lower() == 'mlenginetrainingoperator':
                gscopr.append(MLEngineStartTrainingJobOperator(
                    task_id=Var.task_id + '_' + str(id),
                    project_id=Var.project_processing,
                    job_id=detail['job_id']+'_'+str(uuid.uuid4()),
                    region=Var.region,
                    scale_tier=Var.scale_tier,
                    package_uris=Var.package_uris,
                    training_python_module=detail['training_python_module'],
                    runtime_version=Var.runtime_version,
                    python_version=Var.python_version,
                    training_args=['--bucket_name', Var.BUCKET_NAME, '--output_dir', Var.OUTPUT_DIR]
                ))                
            elif Var.operator.lower() == 'mlenginetrainingoperator_r_lang':
                gscopr.append(MLEngineStartTrainingJobOperator(
                    task_id=Var.task_id + '_' + str(id),
                    project_id=Var.project_processing,
                    job_id=detail['job_id']+'_'+str(uuid.uuid4()),
                    region=Var.region,
                    training_args=[Var.training_args],
                    scale_tier=Var.scale_tier,
                    master_type=Var.master_type,
                    master_config=json.loads(Var.master_config.replace("'","\""))
                ))
            elif Var.operator.lower() == 'vertexaiuploadmodeloperator':
                gscopr.append(UploadModelOperator(
                    task_id=Var.task_id + '_' + str(id),
                    region=Var.region,
                    project_id=Var.project_processing,
                    model={
                        "display_name":'model-'+str(uuid.uuid4()),
                        "artifact_uri": Var.model_artifact_uri,
                        "container_spec":{
                            "image_uri": Var.model_serving_container_uri
                        }
                    }
                ))
            
            elif Var.operator.lower() == 'vertexaideletemodeloperator':
                gscopr.append(
                    PythonOperator(
                        task_id=Var.task_id + '_' + str(id),
                         provide_context=True,
                         op_kwargs={"details": detail,"data":data},
                         python_callable=vertexaiexemodule.vertexi_ai_model
                         )
                         
                )
            elif Var.operator.lower() == 'vertexaiexportmodeloperator':  
                gscopr.append(
                    PythonOperator(
                        task_id=Var.task_id + '_' + str(id),
                         provide_context=True,
                         op_kwargs={"details": detail,"data":data},
                         python_callable=vertexaiexemodule.vertexi_ai_model
                         )
                         
                )
            elif Var.operator.lower() == 'vertexaipytrainingoperator':
                ar_str = Var.arguments
                ar_str=ar_str[1:len(ar_str)-1]
                ar = ar_str.split(',')
                args=[]
                for i in range(len(ar)):
                    if i%2==0:
                        args.append('--'+ar[i])
                    else:
                        args.append(ar[i])
                gscopr.append(CreateCustomPythonPackageTrainingJobOperator(
                    task_id=Var.task_id + '_' + str(id),
                    staging_bucket=Var.staging_bucket,
                    service_account='sa-65343-big-data@'+Var.project_processing+'.iam.gserviceaccount.com',
                    display_name=Var.display_name+'-'+str(uuid.uuid4()),
                    model_display_name=Var.model_display_name+'-'+str(uuid.uuid4()),
                    python_package_gcs_uri=Var.python_package_gcs_uri,
                    python_module_name=Var.python_module_name,
                    container_uri=Var.container_uri,
                    model_serving_container_image_uri=Var.ms_container_image_uri,
                    replica_count=1,
                    machine_type=Var.machine_type,
                    region=Var.region,
                    project_id=Var.project_processing,
                    base_output_dir=Var.base_output_dir,
                    args=args,
                ))
            elif Var.operator.lower() == 'vertexaicustomcontainertrainingoperator':
                ar_str = Var.arguments
                ar_str=ar_str[1:len(ar_str)-1]
                ar = ar_str.split(',')
                args=[]
                for i in range(len(ar)):
                    if i%2!=0:
                        args.append('--'+ar[i-1]+'='+ar[i])
                gscopr.append(CreateCustomContainerTrainingJobOperator(
                    task_id=Var.task_id + '_' + str(id),
                    staging_bucket=Var.staging_bucket,
                    service_account=Var.service_account,
                    display_name=Var.display_name+'-'+str(uuid.uuid4()),
                    model_display_name=Var.model_display_name+'-'+str(uuid.uuid4()),
                    container_uri=Var.container_uri,
                    model_serving_container_image_uri=Var.ms_container_image_uri,
                    replica_count=1,
                    machine_type=Var.machine_type,
                    region=Var.region,
                    project_id=Var.project_processing,
                    base_output_dir=Var.base_output_dir,
                    args=args,
                ))
            

    return gscopr