"""getconfig.py is the helper function for sample DAG template"""
import glob
import os
import operator
from random import randint

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


def replacefun(rawvalue, replacevalue):
    '''replace function'''
    loc = {}
    exec("s=\"" + rawvalue + "\"" + replacevalue, globals(), loc)
    value = loc["s"]
    return value


class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}'


# prjdict["air_id"], prjdict["customer_id"]

def dict_addons(operator, task_id, destination_dataset_table, sql, dagid, air_id, customer_id, svcnow_project,
                svcnow_topic_name):
    thisdict = {'task_status': 'not_started'}
    thisdict["operator"] = operator
    thisdict["task_id"] = task_id
    thisdict["use_legacy_sql"] = False
    thisdict["write_disposition"] = "WRITE_APPEND"
    thisdict["getpartitions"] = "select 'a'"
    thisdict["destination_dataset_table"] = destination_dataset_table
    thisdict["sql"] = sql
    thisdict["dagid"] = dagid
    thisdict["air_id"] = air_id
    thisdict["customer_id"] = customer_id
    thisdict["svcnow_project"] = svcnow_project
    thisdict["svcnow_topic_name"] = svcnow_topic_name
    if task_id == 'failed_log':
        thisdict["trigger_rule"] = "one_failed"
    return thisdict


# Function for .properties files
def process_properties(element, replacevalue):
    '''read job.properties file'''
    data = element
    thisdict = {'task_status': 'not_started'}
    lst = data.splitlines()
    for st in lst:
        key = st[0:st.find("=")]
        rawvalue = st[st.find("=") + 1:len(st)]
        value = replacefun(rawvalue, replacevalue)
        thisdict[key] = value
    return thisdict


# Function for .sql files
def process_sql(element, dagid, replacevalue):
    '''read the sql query'''
    data = element
    script = ''
    value = ''
    thisdict = {'task_status': 'not_started'}
    sqlind = True

    if data.find("---sqlscript") >= 0:
        # getting key value pair for non sql data
        nosql = data[0:data.find("---sqlscript")]
        thisdict = process_properties(nosql, replacevalue)
        thisdict['dagid'] = dagid

        # getting key value pair for sql script
        rawsql = data[data.find("---sqlscript") + (len("---sqlscript")):len(data)]

        while sqlind is True:
            script = ''
            sqlind = True if rawsql.find("---sqlscript") > 0 else False

            if sqlind is True:
                sql = rawsql[0:rawsql.find("---sqlscript")]
                rawsql = rawsql[rawsql.find("---sqlscript") + len("---sqlscript"):len(rawsql)]
            else:
                sql = rawsql.replace("\\", "\\\\").replace("\"", "\\\"")
                #print('after replacing: ' +sql)

            for qry in sql.split():
                script = script + ' ' + qry

            key = script[0:script.find("=")].strip()
            rawvalue = script[script.find("=") + 1:len(script)]
            value = replacefun(rawvalue, replacevalue)
            thisdict[key] = value
    else:
        thisdict = process_properties(data, replacevalue)

    return thisdict


def fngetdata(storage_name, rootdir, dagid, confprojectlistfile,issubtask = False ):
    '''read projectlist.conf file'''
    maxseq = 0.0
    initseq = 0.0
    prjdict = {}
    output = dict({})
    # output[0] =''
    replacevalue = ''
    prefix = storage_name + "/" + rootdir + "/*"
    #print(prefix)
    filelist = glob.glob(prefix)
    statcatchertable = ''

    # Getting Project list
    file = open(confprojectlistfile, "r")
    projects = file.read()

    for project in projects.splitlines():

        key = project[0:project.find("=")]
        value = project[project.find("=") + 1:len(project)]
        if key.split('_')[0] == 'project':
            replacevalue = replacevalue + ".replace('{" + key + "}','" + value + "')"
        if 'statcatchertable=' in project:
            statcatchertable = project[project.find("=") + 1:len(project)]
        prjdict[key] = value

    statcatcherlogsid = randint(0, 10000000000)
    insertstmt = "insert into `{statcatchertable}` (id, dagid, execution_date, startdttm_bqtimezone, status, " \
                 "taskdetails) values ".format_map(SafeDict(statcatchertable=statcatchertable))
    insertlogsql = insertstmt + "( {statcatcherlogsid} , '{dagid}','<execution_date>',CURRENT_TIMESTAMP(), " \
                                "'inprogess', '<taskdetails>')"\
        .format_map(SafeDict(statcatcherlogsid=statcatcherlogsid, dagid=dagid))

    output[initseq] = prjdict
    # print("list-"+replacevalue)
    for filepath in filelist:
        if(os.path.isdir(filepath)==True):
            filenm = filepath[filepath.rindex("/") + 1:len(filepath)]
            seqval = filenm[0:filenm.index("_")]
            decind = False
            folder_sequence = seqval
            maxseq = int(float(folder_sequence)) if int(float(folder_sequence)) > maxseq else maxseq
            if seqval.find(".")>=0:
                decind = True
                startseq = float(seqval[:seqval.index(".")])
                output[startseq] = {'operator':'dummyoperator','task_status': 'not_started'}
                output[startseq]["task_id"]="parallel_start_task"
                output[startseq]["task_status"]="not_started"
                endseq = startseq + 0.99999
                output[endseq] = {'operator':'dummyoperator'}
                output[endseq]["task_id"]="parallel_end_task"
                output[endseq]["task_status"]="not_started"
                
                

            subfilelist = glob.glob(filepath+"/*")
            for subfilepath in subfilelist:  

                if subfilepath.lower().find(".sql") >= 0:            
                    filenm = subfilepath[subfilepath.rindex("/") + 1:len(subfilepath)]
                    seqval = filenm[0:filenm.index("_")]
                    strseqval = "." + str("%04d" % int(seqval)) if decind == False else str("%02d" % int(seqval))
                    sequence = float(folder_sequence+strseqval) 
                    file = open(subfilepath, "r")
                    data = file.read()
                    output[sequence] = process_sql(data, dagid, replacevalue)
                    output[sequence]["air_id"] = prjdict["air_id"]
                    output[sequence]["customer_id"] = prjdict["customer_id"]
                    
                    #print(sequence)

                print(maxseq)
        if filepath.lower().find(".properties") >= 0:
            file = open(filepath, "r")
            data = file.read()
            output[0].update(process_properties(data, replacevalue))
        if filepath.lower().find(".sql") >= 0:
            
            filenm = filepath[filepath.rindex("/") + 1:len(filepath)]
            seqval = filenm[0:filenm.index("_")]
            sequence = float(seqval)
            file = open(filepath, "r")
            data = file.read()
            output[sequence] = process_sql(data, dagid, replacevalue)
            output[sequence]["air_id"] = prjdict["air_id"]
            output[sequence]["customer_id"] = prjdict["customer_id"]
            if seqval.find(".")>=0:
                startseq = float(seqval[:seqval.index(".")])
                output[startseq] = {'operator':'dummyoperator','task_status': 'not_started'}
                output[startseq]["task_id"]="parallel_start_task"
                output[startseq]["task_status"]="not_started"
                endseq = startseq + 0.99999
                output[endseq] = {'operator':'dummyoperator'}
                output[endseq]["task_id"]="parallel_end_task"
                output[endseq]["task_status"]="not_started"
            maxseq = int(sequence if sequence > maxseq else maxseq)
            print(maxseq)
    output[initseq]['operator'] = 'custombqpythonoperator'
    output[initseq]["task_id"] = "start_log"
    output[initseq]["use_legacy_sql"] = False
    output[initseq]["write_disposition"] = "WRITE_APPEND"
    output[initseq]["destination_dataset_table"] = statcatchertable
    output[initseq]["sql"] = insertlogsql
    output[initseq]["getpartitions"] = "select 1"
    output[initseq]["dagid"] = dagid
    output[initseq]["air_id"] = prjdict["air_id"]
    output[initseq]["customer_id"] = prjdict["customer_id"]
    # print(output)
    if issubtask == False:
        maxstmt = "(select max(c.execution_date) from `{statcatchertable}` c where dagid= '{dagid}' " \
                "and status ='inprogess')"

        insertlog = "UPDATE  `{statcatchertable}` SET enddttm_bqtimezone = CURRENT_TIMESTAMP()," \
                    "status = 'success', taskdetails = '<taskdetails>',nextdag=\"<nextdag>\" WHERE dagid ='{dagid}' " \
                    "AND status = 'inprogess' AND execution_date =" + maxstmt
        insertlogsql = insertlog.format_map(SafeDict(statcatchertable=statcatchertable, dagid=dagid))
        seq = float(maxseq) + 1
        output[seq] = dict_addons("custombqpythonoperator", "success_log", statcatchertable, insertlogsql, dagid,
                                prjdict["air_id"], prjdict["customer_id"], prjdict["svcnow_project"],
                                prjdict["svcnow_topic_name"])

        insertlog = "UPDATE  `{statcatchertable}` SET enddttm_bqtimezone = CURRENT_TIMESTAMP(),status = 'failed', " \
                    "taskdetails = '<taskdetails>',nextdag=\"<nextdag>\" WHERE dagid ='{dagid}' " \
                    "AND status = 'inprogess' AND execution_date =" + maxstmt
        insertlogsql = insertlog.format_map(SafeDict(statcatchertable=statcatchertable, dagid=dagid))
        seq = float(maxseq) + 2
        output[seq] = dict_addons("custombqpythonoperator", "failed_log", statcatchertable, insertlogsql, dagid,
                                prjdict["air_id"], prjdict["customer_id"], prjdict["svcnow_project"], 
                                prjdict["svcnow_topic_name"])
        #print(statcatchertable)
    
    if issubtask == True:
        del output[0]

    return output
