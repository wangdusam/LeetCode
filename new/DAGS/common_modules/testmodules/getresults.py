""" This results is to get results  as per column """
from google.cloud import bigquery


# pylint: disable=C0103
# pylint: disable=R0903
# pylint: disable=R0913
# pylint: disable=R0914


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
SELECT COUNT(1) value, CONCAT( \"{{\\\"Matching_Values\\\":[\" ,STRING_AGG(TO_JSON_STRING( (inqry) ) limit 100),\"]}}\") as sample_rows
FROM (
    {sqlscript} ) inqry

)

SELECT '{temp_projectId}' AS projectName,'{phase}' as phase,'{id_prefix}' as testcaseid,\"\"\"{sql}\"\"\" as sql, 
"{description}" as description, '{rule}' AS trends,'{columns}' as columns,"{filter_condition}" as filter, 
CAST(value AS STRING) AS value, sample_rows,'{info}' as info,"{expected_message}" as exp_msg,
"{pass_message}" as actual_msg,'' AS type,current_timestamp,{sessionid} as sessionid
FROM CTC
WHERE value != 0 
UNION ALL
SELECT '{temp_projectId}' AS projectName,'{phase}' as phase,'{id_prefix}' as testcaseid,\"\"\"{sql}\"\"\" as sql,
"{description}" as description,'{rule}' AS trends,'{columns}' as columns,
"{filter_condition}" as filter,CAST(value AS STRING) AS value,sample_rows,'{opp_info}' as info,"{expected_message}" as exp_msg,
"{pass_message}" as actual_msg,'' AS type,current_timestamp,{sessionid} as sessionid
FROM CTC
WHERE value = 0
"""


class Var:
    id_prefix = expected_message = failure_message = ""
    compare_columns = match_type = ""
    columns = pass_message = type = ""
    filter_condition = description = ""
    temp_projectId = temp_datasetId = temp_tableId = ""
    sessionid = sql = info = ""

def fn_select(sql_filenames, phase, testcases_table, tabledetails, details, key):
    ''' Get results as per column '''

    for pattern_detail in details:
        #print(pattern_detail)
        pattern_details = dict({})
        pattern_details = dict(pattern_detail)
        counter = int(1)
        where = ""
        msg_compare_columns = ""
        vt = Var()
        df_vars = [attr for attr in dir(vt) if not callable(getattr(vt, attr)) and not attr.startswith("__")]
        for df_var in df_vars:
            setattr(Var, df_var, "")

        for t_key_val in tabledetails[sql_filenames]:
            setattr(Var, t_key_val, tabledetails[sql_filenames][t_key_val])

        for key_val in pattern_details:
            setattr(Var, key_val, pattern_details[key_val])

        select_cols = "SELECT {columns} "

        print(Var.info)
        Var.info
        Var.info = "Failed" if "fail" in Var.info.lower()  else "Passed"

        opp_info = "Failed" if Var.info == "Passed" else "Passed"


        if Var.compare_columns != "":
            Var.compare_operator = "=" if Var.compare_operator == ""\
                                    else Var.compare_operator

            opr = "OR" if Var.compare_operator == "!=" else "AND"

            msg_compare_columns = " ( compared on columns -  " + Var.compare_columns + \
            ") with operator " + Var.compare_operator

            compare_columns = Var.compare_columns.split(",")
            for val in range(len(compare_columns)):
                if val != 0:
                    where = (where +
                        (" " + opr + " " if where != "" else "") +
                        ("( FORMAT('%t', " + compare_columns[val-1]
                        + ") "+ Var.compare_operator
                        +" FORMAT('%t', " + compare_columns[val] + ") )"))

            where = " WHERE (" + where + ")" if where != "" else ""

        filter_condition = ((" WHERE " if where == "" else where + " AND ")
                            + "(" + Var.filter_condition + ")"
                            if Var.filter_condition != "" else where + "")

        sqltmt = aggrscript.replace("{select_cols}", select_cols)\
                    .replace("{filter_condition}", filter_condition)

        for column in Var.columns:
            sqlscript = ""
            sqlscript = sqltmt.format(temp_projectId=Var.temp_projectId,
                        temp_datasetId=Var.temp_datasetId,
                        temp_tableId =Var.temp_tableId, columns=column, key=key)

            id_prefix = Var.id_prefix + str(counter)

            print(opp_info)

            sqlstmt = uniquesql.format(temp_projectId=Var.temp_projectId, temp_datasetId=Var.temp_datasetId,\
            temp_tableId=Var.temp_tableId, columns=column + msg_compare_columns, pass_message=Var.pass_message,\
            failure_message=Var.failure_message, type=Var.type, sql=Var.sql,\
            expected_message=Var.expected_message, phase=phase, id_prefix=id_prefix,\
            sessionid=Var.sessionid, sqlscript=sqlscript, rule=key , filter_condition=Var.filter_condition,\
            description=Var.description, testcases_table=testcases_table, info=Var.info,\
            opp_info=opp_info
            )

            uniquecol = client.query(sqlstmt, job_config=job_config)
            temp_result = uniquecol.result()
            counter = counter + 1
