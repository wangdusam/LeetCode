"""This module is required for pattern matching"""
import os
import sys
from google.cloud import bigquery
from pattern_regex import fn_getregex

pth = os.path.abspath(os.getcwd())
sys.path.append(pth)
pth1 = os.path.dirname(os.path.abspath(__file__))
sys.path.append(pth1)


# pylint: disable=C0103
# pylint: disable=R0913
# pylint: disable=R0914
# pylint: disable=E1101
# pylint: disable=W0612


# TODO(developer): Construct a BigQuery client object.
client = bigquery.Client()
job_config = bigquery.QueryJobConfig()

aggrscript = """

    {select_cols}
    FROM `{temp_projectId}.{temp_datasetId}.{temp_tableId}`
    {filter_condition}
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


class Var:
    id_prefix = expected_message = failure_message = ""
    compare_operator = group_by = compare_value = ""
    columns = pass_message = type = ""
    filter_condition = compare_operator = ""
    description = case_type = string_type = ""
    string_fix_size = True
    temp_projectId = temp_datasetId = temp_tableId = ""
    sessionid = sql = ""


def fn_pattern(sql_filenames, phase, testcases_table, tabledetails, details, key):
    '''function for pattern matching'''
    for pattern_detail in details:
        # print(pattern_detail)
        pattern_details = dict({})
        pattern_details = dict(pattern_detail)
        counter = int(1)

        # print(details)

        vt = Var()
        df_vars = [attr for attr in dir(vt) if not callable(getattr(vt, attr)) and not attr.startswith("__")]
        # print (df_vars)
        for df_var in df_vars:
            setattr(Var, df_var, "")

        for t_key_val in tabledetails[sql_filenames]:
            setattr(Var, t_key_val, tabledetails[sql_filenames][t_key_val])

        for key_val in pattern_details:
            setattr(Var, key_val, pattern_details[key_val])

        string_fix_size = False if Var.string_fix_size == "" else Var.string_fix_size

        select_cols = "SELECT {column} as val"
        filter_condition = fn_getregex(Var.pattern_type, Var.case_type, Var.string_type, string_fix_size)
        # print(Var.pattern_type)
        filter_condition = "WHERE REGEXP_CONTAINS({column}, r\"" + \
                           filter_condition + "\") = False " + \
                           (" AND (" + Var.filter_condition + ")"
                                if Var.filter_condition != "" else "")

        sqltmt = aggrscript.replace("{select_cols}", select_cols) \
            .replace("{filter_condition}", filter_condition)

        for column in Var.columns:
            sqlscript = ""
            sqlscript = sqltmt.replace("{temp_projectId}",Var.temp_projectId)\
                    .replace("{temp_datasetId}",Var.temp_datasetId)\
                    .replace("{temp_tableId}",Var.temp_tableId).replace("{column}",column)\
                    .replace( "{key}",key)

            id_prefix = Var.id_prefix + str(counter)
            sqlstmt = uniquesql.format(temp_projectId=Var.temp_projectId, temp_datasetId=Var.temp_datasetId,
                                       temp_tableId=Var.temp_tableId, columns=column, pass_message=Var.pass_message,
                                       failure_message=Var.failure_message, type=Var.type, sql=Var.sql,
                                       expected_message=Var.expected_message, phase=phase, id_prefix=id_prefix,
                                       sessionid=Var.sessionid, sqlscript=sqlscript,
                                       rule=(key + " - " + Var.pattern_type)
                                       , filter_condition=Var.filter_condition,
                                       description=Var.description, testcases_table=testcases_table
                                       )

            # print(sqlstmt)
            uniquecol = client.query(sqlstmt, job_config=job_config)
            temp_result = uniquecol.result()
            counter = counter + 1
