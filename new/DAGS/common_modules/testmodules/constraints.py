"""This modules checks if the the values are not null, unique and unique with not null(primary key)"""
from google.cloud import bigquery


# pylint: disable=R0913
# pylint: disable=R0914
# pylint: disable=C0103


# TODO(developer): Construct a BigQuery client object.
client = bigquery.Client()
job_config = bigquery.QueryJobConfig()

# sql for getting unique value
ukscript = """

    SELECT {columns},COUNT(1) as val
    FROM `{temp_projectId}.{temp_datasetId}.{temp_tableId}`
    {ukfilter}
    GROUP BY {columns}
    HAVING COUNT(1) > 1
"""
# sql script to get not null value
notnullscript = """
    SELECT {columns},COUNT(1) as val
    FROM `{temp_projectId}.{temp_datasetId}.{temp_tableId}`
    WHERE CONCAT( {columns}) IS NULL {nnfilter}
    GROUP BY {columns}
"""

uniquesql = """
INSERT INTO `{testcases_table}`
WITH ctc AS
(
SELECT count(1) value, 
CONCAT( \"{{\\\"Not_Matching_Values\\\":[\" ,STRING_AGG(TO_JSON_STRING( (inqry) ) limit 100),\"]}}\") as sample_rows
FROM (
    {sqlscript} ) inqry

)

SELECT '{temp_projectId}' AS projectName,'{phase}' as phase,'{id_prefix}' as testcaseid,\"\"\"{sql}\"\"\" as sql,
'{description}' as description,'{rule}' AS trends,'{columns}' as columns,
"{filter_condition}" as filter,CAST(value AS STRING) AS value,sample_rows,'Passed' as info,
'{expected_message}' as exp_msg,
'{pass_message}' as actual_msg,'' AS type,current_timestamp,{sessionid} as sessionid
FROM CTC
WHERE value = 0
UNION ALL
SELECT '{temp_projectId}' AS projectName,'{phase}' as phase,'{id_prefix}' as testcaseid,\"\"\"{sql}\"\"\" as sql,
'{description}' as description,'{rule}' AS  trends,'{columns}' as columns,
"{filter_condition}" as filter,CAST(value AS STRING) AS value,sample_rows,'Failed' as info,
'{expected_message}' as exp_msg,
'{failure_message}' as actual_msg, '{type}' AS type, current_timestamp,{sessionid} as sessionid
FROM CTC
WHERE value != 0

"""


def fn_constraints(sql_filenames, phase, testcases_table, tabledetails, details, key):
    '''This modules checks if the the values are not null, unique and unique with not null(primary key)'''
    filter_condition = ""
    description = ""
    sqlscript = ""
    sqlfile = sql_filenames
    temp_projectId = tabledetails[sqlfile]["temp_projectId"]
    temp_datasetId = tabledetails[sqlfile]["temp_datasetId"]
    temp_tableId = tabledetails[sqlfile]["temp_tableId"]
    sessionid = tabledetails[sqlfile]["sessionid"]
    sql = tabledetails[sqlfile]["sql"]
    counter = int(1)
    id_prefix = details["id_prefix"]
    columns = details["columns"]
    pass_message = details["pass_message"]
    failure_message = details["failure_message"]
    expected_message = details["expected_message"]
    type = details["type"]
    if "filter_condition" in details:
        filter_condition = details["filter_condition"]

    if "description" in details:
        description = details["description"]

    ukfilter = "WHERE CONCAT ({columns}) IS NULL AND (" + filter_condition + ")" \
        if key == "primarykey" and filter_condition != "" \
        else " WHERE CONCAT ({columns}) IS NOT NULL " if key == "primarykey" \
        else " WHERE (" + filter_condition + ")" if filter_condition != "" \
        else ""

    nnfilter = "AND (" + filter_condition + ")" if filter_condition != "" else ""

    sqltmt = ukscript.replace("{ukfilter}", ukfilter) + " UNION ALL " + \
             notnullscript.replace("{nnfilter}", nnfilter) \
        if key == "primarykey" \
        else ukscript.replace("{ukfilter}", ukfilter) if key == "uniquekey" \
        else notnullscript.replace("{nnfilter}", nnfilter) if key == "notnull" else ""
    # print(columns)
    for column in columns:
        sqlscript = ""
        sqlscript = sqltmt.format(temp_projectId=temp_projectId,
                                  temp_datasetId=temp_datasetId,
                                  temp_tableId=temp_tableId, columns=column)

        id_prefix = details["id_prefix"] + str(counter)
        sqlstmt = uniquesql.format(temp_projectId=temp_projectId, temp_datasetId=temp_datasetId,
                                   temp_tableId=temp_tableId, columns=column, pass_message=pass_message,
                                   failure_message=failure_message, type=type, sql=sql,
                                   expected_message=expected_message, phase=phase, id_prefix=id_prefix,
                                   sessionid=sessionid, sqlscript=sqlscript, rule=key,
                                   filter_condition=filter_condition,
                                   description=description, testcases_table=testcases_table)

        # print(sqlscript)
        uniquecol = client.query(sqlstmt, job_config=job_config)
        temp_result = uniquecol.result()
        counter = counter + 1
