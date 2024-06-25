'''Follwing Files are needed for the job 
# import.lib - contains list of libraries needed for running job. file is present under /DAGS/common_modules
# gettasklist.py - this file is needed to get list of tasks. file is available under /DAGS/common_modules
# getconfig.py & jobexecmodule.py are needed for running job. files are available under /DAGS/common_modules
# projectlist.conf - contains projest and logtable details. file under DAGS/conf_file/<environment> path
# publish.py - This module is required for genertaing a SNOW ticket in case of DAG failure in production. 
#In case of sandbox and nonproduction job failure no action will be taken.
#
# DAG properties -
# concurrency: the number of task instances allowed to run concurrently across all active runs of the DAG
# max_active_runs: maximum number of active runs for this DAG.
#                  The scheduler will not create new active DAG runs once this limit is hit
# catchup: set to true will avoid backfill else will start from start_date value'''

from airflow.operators.python import PythonOperator
from common_modules import getconfig
from airflow.configuration import conf
import logging
import time
from common_modules.gettasklist import get_task
from common_modules.task_sequence import task_seq


dagfolder = conf.get('core', 'DAGS_FOLDER')
confprojectlistfile = dagfolder + "/conf_files/projectlist.conf"
importlibpath = dagfolder + "/common_modules/import.lib"

import_lib = open(importlibpath, "r")
exec(import_lib.read())

# get internal path of composer from `os` class
pth = os.path.abspath(os.getcwd())
sys.path.append(pth)
pth1 = os.path.dirname(os.path.abspath(__file__))
sys.path.append(pth1)

rootdir = "dml"
storage_name = pth1

# ====>>> input dagid <<<=====
dagid = '196865_0076_hr_onetime'

# start date scheduling
yesterday = datetime.datetime.combine(datetime.datetime.today() - datetime.timedelta(1),
                                      datetime.datetime.min.time())

default_dag_args = {
    'start_date': yesterday, 'default_timezone': 'utc', 'concurrency': 1, 'retries': 0, 'retry_delay': datetime.timedelta(minutes=1)
}

gscopr = []
with models.DAG(
        dagid,
        catchup=False,
        max_active_runs=1,
        schedule_interval='*/10 * * * *',
        default_args=default_dag_args) as dag:

    gscopr = get_task(storage_name, rootdir, dagid, confprojectlistfile)
    task_seq(gscopr)
    # for i in range(0, len(gscopr)):
    #     if i not in [0]:
    #         gscopr[i - 1] >> gscopr[i]
