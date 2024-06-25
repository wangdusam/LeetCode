'''This module is required for genertaing a SNOW ticket in case of DAG failure in production. In case of sandbox and nonproduction job failure no action will be taken.'''
import json
from base64 import b64encode
from google.cloud import pubsub_v1


def pubsubfunc(airid, aiaid, failed_msg, projectid, topicnm):
    '''Pubclish message to pubsub topic on dag failure if env is prd'''
    # Encoding the variables
    env_name = projectid.split("-")[0]
    if env_name.lower() in ['prd']:
        actionvar = b64encode(b'trigger_snow_ticket').decode('utf-8')
        short_desc = b64encode(failed_msg.encode('ascii')).decode('utf-8')

        airidvar = b64encode(airid.encode('ascii')).decode('utf-8')
        aiaidvar = b64encode(aiaid.encode('ascii')).decode('utf-8')

        publisher = pubsub_v1.PublisherClient()
        topic_name = 'projects/{project_id}/topics/{topic}'.format(project_id=projectid,
                                                                   topic=topicnm)
        json_string = '{"attributes": {"action": "' + actionvar + '", "short_description": "' + short_desc + '"} , "data": {"airid": "' + airidvar + '", "aiaid": "' + aiaidvar + '", "short_description": "' + short_desc + '"}}'
        msg = json_string.encode("utf-8")
        print(msg)
        publisher.publish(topic_name, msg)
