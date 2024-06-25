'''jobexecmodule.py is needed for running DAG job template'''
import ast
import logging
import os
from airflow.configuration import conf
# import airflow classes for initializing class
from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.models import DagRun
from common_modules import publish
from google.cloud import bigquery
# pylint: disable=E0602
# pylint: disable=E0401
# pylint: disable=C0103
# pylint: disable=W0106
# pylint: disable=C0114
# pylint: disable=C0115
# pylint: disable=C0116
# pylint: disable=R0914
# pylint: disable=R0124
# pylint: disable=R0915
# pylint: disable=W0612
# pylint: disable=W0122

dagfolder = conf.get('core', 'DAGS_FOLDER')
importlibpath = dagfolder + "/common_modules/import.lib"

# import all libaray from conf_files/import.lib file
import_lib = open(importlibpath, "r")
exec(import_lib.read())


def JobExec(**kwargs):
    airid = ''
    topicnm = ''
    getpartition = "SELECT '1970-01-01'"
    taskdetail = ''
    nextdag = ''
    detail = {}
    client = bigquery.Client()


    execution_date = kwargs['execution_date']
    sql = kwargs['sql']
    destination_dataset_table = kwargs['destination_dataset_table']
    write_disposition = kwargs['write_disposition']
    getpartitions = kwargs['getpartitions']
    task_id = kwargs['task_id']
    nextdag = kwargs['nextdag']
    #statcatchertable = kwargs['statcatchertable']
    dictnextdag = {}
    logging.info(getpartitions)

    if task_id == "start_log":
        create_query = "create table if not exists `{statcatchertable}`(id INT64 , dagid STRING, execution_date TIMESTAMP," \
        "startdttm_bqtimezone TIMESTAMP ,enddttm_bqtimezone TIMESTAMP ,status STRING ,taskdetails STRING ," \
        "nextdag STRING ) PARTITION BY DATE(execution_date) " \
        "CLUSTER BY dagid, status,id OPTIONS (partition_expiration_days=360)".format(statcatchertable=kwargs['statcatchertable'])

        big_query_create_statcatcher = BigQueryInsertJobOperator(
            task_id="create_statcatcher_table",
            force_rerun=False,
            configuration={
                "query": {
                    "query": create_query,
                    "useLegacySql": False
                }})

        big_query_create_statcatcher.execute(context=kwargs)


    if task_id == "success_log" or task_id == "failed_log":
        exec_dttm = kwargs['execution_date']
        tasklist = kwargs['tasklist']
        inextdag = "{" + nextdag + "}"

        dictnextdag = ast.literal_eval(inextdag)

        for id, dagdetail in dictnextdag.items():
            nextdag_id = dagdetail['trigger_dag_id']
            dag_runs = DagRun.find(dag_id=nextdag_id)
            for dag_run in dag_runs:
                dictnextdag[id]['execution_date'] = str(dag_run.execution_date) \
                    if task_id == "success_log" else 'Not Executed'

        for task in tasklist.split(','):
            ti = get_task_instance(kwargs['dagid'], task, exec_dttm)
            state = ti.current_state()
            task_js = "{task_id:" + task + ",status:" + state + "}"
            taskdetail = taskdetail + "," + task_js if taskdetail != '' else task_js
            logging.info(task + ":" + state)

            if state == "failed":
                failed_msg = "{The DAG " + kwargs['dagid'] + " failed for the task " + task + " for AIAID " + kwargs['customer_id'] + "}"


    query_job = client.query(getpartitions)
    rows = query_job.result()

    partitions = [[]]
    partitions = [[row[0]] for row in rows]
    logging.info(partitions)
    print(partitions)

    for partition in partitions:

        partition_filter = partition[0]
        partition_val = partition_filter.replace("-", "") \
            if isinstance(partition_filter, int) is False else partition_filter
        logging.info(partition_val)

        isql = sql.format(partition_filter=partition_filter)
        sqlstr = isql.replace('<taskdetails>',
                              taskdetail).replace('<execution_date>', str(execution_date)).\
            replace('<nextdag>', str(dictnextdag))
        sql_destination_dataset_table = destination_dataset_table.format(
            partition_val=partition_val)

        logging.info("--->" + sqlstr)

        if 'select' in sqlstr.strip().partition(' ')[0].lower():
            projectId = sql_destination_dataset_table.split(".")[0]
            datasetId = sql_destination_dataset_table.split(".")[1]
            tableId = sql_destination_dataset_table.split(".")[2]
            logging.info("tableId--->" + tableId)

            big_query_reader = BigQueryInsertJobOperator(
                task_id="ingest_table",
                force_rerun=False,
                configuration={
                    "query": {
                        "query": sqlstr,
                        "destinationTable": {
                            "projectId": projectId,
                            "datasetId": datasetId,
                            "tableId": tableId
                        },
                        "useLegacySql": False,
                        "writeDisposition": write_disposition
                    }})

        else:
            big_query_reader = BigQueryInsertJobOperator(
                task_id="ingest_table",
                force_rerun=False,
                configuration={
                    "query": {
                        "query": sqlstr,
                        "useLegacySql": False
                    }})

        big_query_reader.execute(context=kwargs)

    if task_id == "failed_log":
        publish.pubsubfunc(kwargs['air_id'], kwargs['customer_id'], failed_msg, kwargs['svcnow_project'], kwargs['svcnow_topic_name'])
        raise ValueError('Task failed')
    return 'success'