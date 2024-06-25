"""Read json file for testcases"""
import ast
import sys
import os
import time
from aggregate import fn_aggregate
from constraints import fn_constraints
from getresults import fn_select
from gettempvalues import fn_getqueryvalues
from pattern import fn_pattern
from logtables import fn_createtestcasetables
from logtables import fn_createtestcase_sessionlog
from schemacompare import fn_test_extract_schema

# pylint: disable=C0103
# pylint: disable=C0301
# pylint: disable=W0612
# pylint: disable=R0912
# pylint: disable=R0914
# pylint: disable=R0915


pth = os.path.abspath(os.getcwd())
sys.path.append(pth)
pth1 = os.path.dirname(os.path.abspath(__file__))
sys.path.append(pth1)


def fn_runtestcases(directory, confprojectlistfile):
    ''' Run appropriate test cases by reading json file  '''
    filelist = []
    replacevalue = ""
    testcase_sessionlog = ""
    testcases_table = ""
    freshrun = False
    jobpropefile = directory + "/job.properties"

    if os.path.isfile(jobpropefile) == True:
        filelist.append(jobpropefile)

    filelist.append(confprojectlistfile)
    for file in filelist:
        file = open(file, "r")
        projects = file.read()
        for project in projects.splitlines():
            key = project[0:project.find("=")]
            value = project[project.find("=") + 1:len(project)]
            if "project_" == key[0:8]:
                replacevalue = replacevalue + ".replace('{" + key + "}','" + value + "')"
            if key == "testcase_sessionlog":
                testcase_sessionlog = value
            if key == "testcases_table":
                testcases_table = value
            if key == "freshrun":
                freshrun = value.capitalize()
            if key == "project_bigdata":
                project_bigdata = value

    fn_createtestcasetables(testcases_table)
    fn_createtestcase_sessionlog(testcase_sessionlog)

    tabledetails = fn_getqueryvalues(directory, replacevalue, testcase_sessionlog, freshrun)
    filter_condition = ""

    dist = {}
    csv_err_list = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".json"):
                jsonfile = os.path.join(root, file)
                # print(jsonfile)
                f = open(jsonfile, "r")
                dist = ast.literal_eval(f.read())

                if str(dist.get('compare')) != "True":
                    for filenm in list(dist.keys()):
                        sql_filenames = filenm
                        print(sql_filenames)
                        phase = dist[filenm]["phase"]
                        nestkey = dist[filenm]["assertions"]
                        nest_key_list = list(nestkey.keys())
                        for key in nest_key_list:
                            details = nestkey[key]
                            # print(details)
                            if key in ["uniquekey", "primarykey", "notnull"]:
                                fn_constraints(sql_filenames, phase, testcases_table, tabledetails, details, key)

                            if key in ["min", "max", "avg", "count"]:
                                fn_aggregate(sql_filenames, phase, testcases_table, tabledetails, details, key)

                            if key in ["pattern"]:
                                fn_pattern(sql_filenames, phase, testcases_table, tabledetails, details, key)
                            if key in ["select"]:
                                fn_select(sql_filenames, phase, testcases_table, tabledetails, details, key)
                            time.sleep(2)
            if file.endswith(".csv"):
                #csv_err_list = []
                csvfilepath = os.path.join(root, file)
                #print('csv filepath: ' + csvfilepath)
                #print(project_bigdata)
                if (len(file[:-4].split(".")) == 2):
                    dataset_id = file[:-4].split(".")[0]
                    tablenm = file[:-4].split(".")[1]
                    print('dataset_id : ' + dataset_id)
                    print('tablenm : ' + tablenm)
                    print('csvfilepath : ' + csvfilepath)
                    fn_test_extract_schema(project_bigdata, dataset_id, tablenm, csvfilepath, testcases_table)
                else:
                    print('file : ' + file)
                    csv_err_list.append(file)
                    print('table ID is not in the form of <dataset_id.tablenm> for schema comparison: ' + file)
            time.sleep(2)
    if len(csv_err_list) > 0:
        raise ValueError('CSV File Name is not in the form of <dataset_id.tablenm> for schema comparison')
    else:
        return "success"
