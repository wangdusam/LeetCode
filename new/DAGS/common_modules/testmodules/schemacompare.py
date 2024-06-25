"""this file is needed to compare schema of the table present in csv with the bigquery table schema"""
import csv
import collections
import sys
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import BadRequest


# pylint: disable=C0301
# pylint: disable=R0914
# pylint: disable=C0103
# pylint: disable=W0612
# pylint: disable=R0912
# pylint: disable=R0915
# pylint: disable=W0611



def fn_test_extract_schema(project, dataset_id, table_id, csvfilepath, testcases_table):
    '''Function to extract and compare schema'''
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    tablenm = project + "." + dataset_id + "." + table_id
    var = ''
    fail_msg = ''
    sqlscript1 = ''

    # Check if table exists
    try:
        client.get_table(tablenm)  # Make an API request.
        print("Table {} exists.".format(tablenm))
        # Read CSV File for Schema Comparison
        with open(csvfilepath, 'r') as csv_file:
            names = csv.reader(csv_file)
            next(names)
            table_map = {}
            for name in names:
                table_map[name[0]] = name[1]

        # Read Bigquery Table Schema
        dataset_ref = client.dataset(dataset_id, project=project)
        table_ref = dataset_ref.table(table_id)
        table = client.get_table(table_ref)  # API Request

        result = ["['{0}', '{1}']".format(schema.name, schema.field_type) for schema in table.schema]
        table_map_com = {}
        for row2 in table.schema:
            table_map_com[row2.name] = row2.field_type

        def lower_dict(d):
            new_dict = dict((k2.lower(), v2.lower()) for k2, v2 in d.items())
            return new_dict

        a = {'Foo': "Hello", 'Bar': "World"}
        print(lower_dict(a))

        table_map_com1 = lower_dict(table_map_com)
        table_map1 = lower_dict(table_map)
        #    print(list(table_map_com.items()))
        #    print(list(table_map.items()))

        for k1, v1 in table_map1.items():
            if k1 not in table_map_com1.keys():
                msg1 = "Additional column in csv"
                var = var + "select '{0}', '{1}' union all ".format(k1, msg1)
            else:
                msg = "Schema is matching"

        for k, v in table_map_com1.items():
            if k in table_map1.keys():
                if table_map1[k] != v:
                    # datatype mismatch
                    message = "Table name : " + table_id + " Field: " + k + " Type: " + v + " is different with " + csvfilepath + " The Type in csv is " + \
                              table_map1[k]
                    print(message)
                    msg2 = "Data Type mismatch. Field is {0} type in CSV and {1} type in Table".format(table_map1[k], v)
                    var = var + "select '{0}', '{1}' union all ".format(k, msg2)
            #               print(var)
            else:
                message = "Table name : " + table_id + " Field: " + k + " does not exists in " + csvfilepath
                #           print(message)
                msg3 = "Column not found in CSV"
                var = var + "select '{0}', '{1}' union all ".format(k, msg3)
        #            print(var)

        if var == '':
            sqlscript1 = "select {0}, '{1}' limit 0".format(0, '')
        else:
            sqlscript1 = (var[::-1].replace('union all'[::-1], ''[::-1], 1))[::-1]
            fail_msg = 'Schema Not Matching'
            print(sqlscript1)

    except NotFound:
        print("Table {} is not found.".format(tablenm))
        sqlscript1 = "select {0}, '{1}'".format(0, 'Table not found')
        fail_msg = 'Table does not exist'
        print(fail_msg)
    except BadRequest as e:
        print('ERROR: {}'.format(str(e)))
        raise ValueError('Schema Comparison Failed')

    insert_sessiontable = """
    INSERT INTO `{testcases_table}`
    WITH ctc AS
    (
    SELECT COUNT(1) value, CONCAT( \"{{\\\"Not_Matching_Values\\\":[\" ,STRING_AGG(TO_JSON_STRING( (inqry) )),\"]}}\") as sample_rows
    FROM (
        {sqlscript} ) inqry

    )

    SELECT '{temp_projectId}' AS projectName,
    'schema_compare' as phase,'schema_compare' as testcaseid,'{temp_tableId}' as sql, 
    '{temp_tableId}' as description, 'schema_compare' AS trends,'' as columns,"" as filter, 
    CAST(value AS STRING) AS value, sample_rows,'Failed' as info,"Schema should match" as exp_msg,
    '{failure_msg}' as actual_msg,'critical' AS type,current_timestamp,1 as sessionid
    FROM CTC
    WHERE value != 0 
    UNION ALL
    SELECT '{temp_projectId}' AS projectName,'schema_compare' as phase,'schema_compare' as testcaseid,'{temp_tableId}' as sql,
    '{temp_tableId}' as description,'schema_compare' AS trends,'' as columns,
    "" as filter, CAST(value AS STRING) AS value,sample_rows, 'Passed' as info,"Schema should match" as exp_msg,
    "Schema Matching" as actual_msg,'' AS type,current_timestamp,1 as sessionid
    FROM CTC
    WHERE value = 0"""

    sqlstmt = insert_sessiontable.format(temp_projectId=project, temp_tableId=(dataset_id + "." + table_id),
                                         sqlscript=sqlscript1, failure_msg=fail_msg, testcases_table=testcases_table)
    #    print(sqlstmt)

    query_job = client.query(sqlstmt, job_config=job_config)
    temp_result = query_job.result()

