'''vertexaiexemodule.py is needed for running DAG job template'''
from multiprocessing import context
from airflow.providers.google.cloud.operators.vertex_ai.model_service import ExportModelOperator
from airflow.providers.google.cloud.operators.vertex_ai.model_service import DeleteModelOperator

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

def vertexi_ai_model(**kwargs):
    
    task_instance = kwargs['task_instance']
    print(kwargs['details']['project_processing'])
    print(kwargs['details']['region'])
    print(kwargs['details']['refrence_task_id'])
    operator = kwargs['details']['operator']
    data = kwargs['data']

    for id, detail in data.items():
        if detail['task_id'] == kwargs['details']['refrence_task_id']:
            task_id = detail['task_id'] + '_' + str(id)
    model_id = task_instance.xcom_pull(task_ids=task_id)['model'].split('/')[-1]


    if operator == 'vertexaideletemodeloperator':
        delete_model = DeleteModelOperator(
            task_id=task_id,
            project_id=kwargs['details']['project_processing'],
            region=kwargs['details']['region'],
            model_id=model_id) 
        delete_model.execute(context=kwargs)
    elif operator == 'vertexaiexportmodeloperator':
        export_model = ExportModelOperator(
            task_id=task_id,
            project_id=kwargs['details']['project_processing'],
            region=kwargs['details']['region'],
            model_id=model_id,
            output_config={
                "export_format_id": kwargs['details']['export_format_id'],
                    "artifact_destination":{
                            "output_uri_prefix": kwargs['details']['output_uri_prefix']
                        }})
        export_model.execute(context=kwargs)
        
    