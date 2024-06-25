""" This module is used to run queries present in ts file and stores session log in testcase_sessionlog table """
import os
from random import randint
import re
import datetime
from google.cloud import bigquery


# pylint: disable=C0103
# pylint: disable=R0914



def replacefun(rawvalue, replacevalue):
    loc = {}
    exec("s=\"" + rawvalue + "\"" + replacevalue, globals(), loc)
    value = loc["s"]
    return value

# TODO(developer): Construct a BigQuery client object.
client = bigquery.Client()
job_config = bigquery.QueryJobConfig()

checkvalue_sessiontable = """
SELECT  temp_projectId, temp_datasetId,temp_tableId
FROM `{testcase_sessionlog}` 
WHERE createdttm > TIMESTAMP_SUB(current_timestamp, INTERVAL 23 hour)
AND hashvalue = '{hashval}'
order by createdttm desc
limit 1
"""

insert_sessiontable = """
insert into `{testcase_sessionlog}`
select {sessionid},\"\"\"{sql}\"\"\",'{hashval}','{temp_projectId}','{temp_datasetId}',
'{temp_tableId}',current_timestamp
"""


def fn_getqueryvalues(directory, replacevalue, testcase_sessionlog, newrecordind=False):
    '''This module is used to run queries present in ts file and stores session log in testcase_sessionlog table'''
    ## create log tables path - dags/common_modules/testmodules

    output = dict({})
    hashvalue = ''
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".ts"):

                temp_projectId = ""
                temp_datasetId = ""
                temp_tableId = ""

                sqlfile = os.path.join(root, file)
                f = open(sqlfile, "r")
                data = (f.read()).replace('\n', ' ')
                print(file.rsplit(".ts")[0])
                sessionid = randint(0, 10000000000)
                print(sessionid)
                sql = re.sub(r'[^a-zA-Z0-9 ,.\-_`]{}', r'', data).strip()
                # print(project_name)
                sql = replacefun(sql, replacevalue)
                #sql = sql.replace("{project_name}", project_name)
                print(sql)
                hashvalue = hash(sql)

                if newrecordind is False:
                    get_tempval = client.query(
                        checkvalue_sessiontable.format(hashval=hashvalue,
                                                       testcase_sessionlog=testcase_sessionlog)
                        , job_config=job_config)
                    temp_result = get_tempval.result()
                    for row in temp_result:
                        temp_projectId = row.temp_projectId
                        temp_datasetId = row.temp_datasetId
                        temp_tableId = row.temp_tableId

                print(temp_tableId)
                if (temp_tableId is ""):
                    query_job = client.query(sql, job_config=job_config)

                    # Make an API request.
                    data = query_job.result()
                    tempdetail = {}

                    tempdetail = query_job._properties["configuration"]["query"]["destinationTable"]

                    temp_projectId = tempdetail['projectId']
                    temp_datasetId = tempdetail['datasetId']
                    temp_tableId = tempdetail['tableId']
                    insertscript = insert_sessiontable.format(sessionid=sessionid, sql=sql,
                                                              hashval=hashvalue, temp_projectId=temp_projectId,
                                                              temp_datasetId=temp_datasetId, temp_tableId=temp_tableId,
                                                              testcase_sessionlog=testcase_sessionlog
                                                              )
                    query_job = client.query(insertscript, job_config=job_config)
                    data = query_job.result()

                filename = file.split(".")[0]
                output[filename] = {}
                output[filename]['sql'] = str(sql)
                output[filename]["hashvalue"] = hashvalue
                output[filename]["temp_projectId"] = temp_projectId
                output[filename]["temp_datasetId"] = temp_datasetId
                output[filename]["temp_tableId"] = temp_tableId
                output[filename]["sessionid"] = sessionid

    return output
