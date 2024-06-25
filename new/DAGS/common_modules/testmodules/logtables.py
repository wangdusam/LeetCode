"""Module for creating log tables"""
import os
import re
import datetime
from random import randint
from google.cloud import bigquery


# pylint: disable=W0612
# pylint: disable=W0611
# pylint: disable=C0103


# TODO(developer): Construct a BigQuery client object.
client = bigquery.Client()
job_config = bigquery.QueryJobConfig()

testcases_script = """
CREATE TABLE IF NOT EXISTS `{testcases}`
(
projectName STRING,
phase STRING,
testcaseid STRING,
sql STRING,
description STRING,
trends STRING,
columns STRING,
filteron STRING,
value STRING,
sample_rows STRING,
info STRING,
expected_output STRING,
actual_output string,
type string,
tested_on timestamp,
sessionid int64
)
partition by date(tested_on)
cluster by trends
OPTIONS (partition_expiration_days=90)
"""
testcase_sessionlog_script = """
CREATE TABLE IF NOT EXISTS `{testcase_sessionlog}` 
(
sessionid int64,
query string,
hashvalue string,
temp_projectId string,
temp_datasetId string,
temp_tableId string,
createdttm timestamp
)
partition by date(createdttm)
cluster by hashvalue
OPTIONS (partition_expiration_days=2)
"""



def fn_createtestcasetables(testcases):
    '''create test cases table'''
    createtestcasetables = testcases_script.format(testcases=testcases)
    query_job = client.query(createtestcasetables, job_config=job_config)
    data = query_job.result()

    return createtestcasetables

def fn_createtestcase_sessionlog(testcase_sessionlog):
    '''create testcase_sessionlog table'''
    createtestcase_sessionlog = testcase_sessionlog_script.format(testcase_sessionlog=testcase_sessionlog)
    query_job = client.query(createtestcase_sessionlog, job_config=job_config)
    data = query_job.result()
    return createtestcase_sessionlog
