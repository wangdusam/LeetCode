""" This module is used for aggregrations """
from google.cloud import bigquery


# pylint: disable=C0103
# pylint: disable=R0913
# pylint: disable=R0914
# pylint: disable=W0612
# pylint: disable=R0903


# TODO(developer): Construct a BigQuery client object.
client = bigquery.Client()
job_config = bigquery.QueryJobConfig()

aggrscript = """

    {select_cols}
    FROM `{temp_projectId}.{temp_datasetId}.{temp_tableId}`
    {filter_condition}
    {group_by_cols}
    {having_condition}
"""

uniquesql = """
INSERT INTO `{testcases_table}`
WITH ctc AS
(
SELECT MAX(val) value, 
CONCAT( \"{{\\\"Matching_Values\\\":[\" ,STRING_AGG(TO_JSON_STRING( (inqry) ) limit 100),\"]}}\") as sample_rows
FROM (
    {sqlscript} ) inqry

)

SELECT '{temp_projectId}' AS projectName,'{phase}' as phase,'{id_prefix}' as testcaseid,\"\"\"{sql}\"\"\" as sql, 
'{description}' as description, '{rule}' AS trends,'{columns}' as columns,"{filter_condition}" as filter, 
CAST(value AS STRING) AS value, sample_rows,'Passed' as info,'{expected_message}' as exp_msg,
'{pass_message}' as actual_msg,'' AS type,current_timestamp,{sessionid} as sessionid
FROM CTC
WHERE value != 0
UNION ALL
SELECT '{temp_projectId}' AS projectName,'{phase}' as phase,'{id_prefix}' as testcaseid,\"\"\"{sql}\"\"\" as sql, 
'{description}' as description, '{rule}' AS  trends,'{columns}' as columns,"{filter_condition}" as filter, 
CAST(value AS STRING) AS value, sample_rows,'Failed' as info,'{expected_message}' as exp_msg,
'{failure_message}' as actual_msg, '{type}' AS type, current_timestamp,{sessionid} as sessionid
FROM CTC
WHERE value = 0

"""


class Var:
    id_prefix = expected_message = failure_message = ""
    compare_operator = group_by = compare_value = ""
    columns = pass_message = type = ""
    filter_condition = compare_operator = ""
    description = ""
    temp_projectId = temp_datasetId = temp_tableId = ""
    sessionid = sql = ""


def fn_aggregate(sql_filenames, phase, testcases_table, tabledetails, details, key):
    ''' functions for performing aggregrations'''
    counter = int(1)

    # print(details)

    vt = Var()
    df_vars = [attr for attr in dir(vt) if not callable(getattr(vt, attr)) and not attr.startswith("__")]
    for df_var in df_vars:
        setattr(Var, df_var, "")

    for key_val in details:
        setattr(Var, key_val, details[key_val])

    for t_key_val in tabledetails[sql_filenames]:
        setattr(Var, t_key_val, tabledetails[sql_filenames][t_key_val])

    select_cols = "SELECT " + Var.group_by + ",{key}({column}) as val" \
        if Var.group_by != "" \
        else "SELECT {key}({column}) as val"

    group_by_cols = "GROUP BY " + Var.group_by \
        if Var.group_by != "" \
        else ""

    filter_condition = "WHERE (" + Var.filter_condition + ")" \
        if Var.filter_condition != "" else ""

    having_condition = "HAVING {key}({column}) " + Var.compare_operator + " " \
                       + Var.compare_value \
        if Var.compare_value != "" else ""

    sqltmt = aggrscript.replace("{select_cols}", select_cols) \
        .replace("{group_by_cols}", group_by_cols) \
        .replace("{filter_condition}", filter_condition) \
        .replace("{having_condition}", having_condition)

    # print (sqltmt)

    for column in Var.columns:
        sqlscript = ""
        sqlscript = sqltmt.format(temp_projectId=Var.temp_projectId,
                                  temp_datasetId=Var.temp_datasetId,
                                  temp_tableId=Var.temp_tableId, column=column, key=key)

        id_prefix = Var.id_prefix + str(counter)
        sqlstmt = uniquesql.format(temp_projectId=Var.temp_projectId, temp_datasetId=Var.temp_datasetId,
                                   temp_tableId=Var.temp_tableId, columns=column, pass_message=Var.pass_message,
                                   failure_message=Var.failure_message, type=Var.type, sql=Var.sql,
                                   expected_message=Var.expected_message, phase=phase, id_prefix=id_prefix,
                                   sessionid=Var.sessionid, sqlscript=sqlscript, rule=key,
                                   filter_condition=Var.filter_condition,
                                   description=Var.description, testcases_table=testcases_table
                                   )

        # print(sqltmt)

        # print(sqlstmt)
        uniquecol = client.query(sqlstmt, job_config=job_config)
        temp_result = uniquecol.result()
        counter = counter + 1
