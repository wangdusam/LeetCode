import json
import base64
import requests
import logging
import string
import base64
import fileinput
import logging
from datetime import datetime
from io import BytesIO
from google.cloud import storage
from google.cloud import secretmanager

def access_secret_version(project_id, secret_id, version_id):
  """
  Access the payload for the given secret version if one exists. The version
  can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
  """
  # Create the Secret Manager client.
  client = secretmanager.SecretManagerServiceClient()

  # Build the resource name of the secret version.
  name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

  # Access the secret version.
  response = client.access_secret_version(request={"name": name})

  # Print the secret payload.
  #
  # WARNING: Do not print the secret in a production environment - this
  # snippet is showing how to access the secret material.
  payload = response.payload.data.decode("UTF-8")
  #print("Plaintext: {}".format(payload))
  return json.loads(payload)
  #print("Pass: {}".format(json_data["AZURE_PASS"]))

def crc32c(data):
    """
    Calculates the CRC32C checksum of the provided data.
    Args:
        data: the bytes over which the checksum should be calculated.
    Returns:
        An int representing the CRC32C checksum of the provided bytes.
    """
    import crcmod
    import six
    crc32c_fun = crcmod.predefined.mkPredefinedCrcFun('crc-32c')
    return crc32c_fun(six.ensure_binary(data))
    
###################
## below block to get the build definition using build name
###################

def get_def_id(azdevopsUser,azdevopsPass,azDefUrl,destroy_pipeline_name):
    azdevopsURL=azDefUrl
    params = {
        ('name', destroy_pipeline_name)
    }
    head = { 'Accept': 'application/json', 'Content-Type': 'application/json'}
    
    try:
        response = requests.get(azdevopsURL, headers=head, params=params, auth=(azdevopsUser, azdevopsPass))
        response.raise_for_status()
        jsonResponse = response.json()
        def_id=jsonResponse["value"][0]['id']
        print("INFO - Call finished succcessfully - ", response)
        return def_id
    except requests.exceptions.HTTPError as err:
        print("ERROR - Call Azure DevOps API for Build definition finished with issues....", response)
        print("ERROR - Status Code : ", response.status_code)
        print("ERROR - Reason :", response.reason)

def call_destroy_azure(event, context):
    print ('Invocating destroy stage')
    gcp_project_id = "#{GCP_PROJECT_ID}#"
    secret_id = "sm-{}-{}-{}".format(gcp_project_id,'#{GCP_SM_ENV_NAME}#','#{GCP_SM_NAME}#')
    secret = access_secret_version(gcp_project_id,secret_id,"latest")
    azdevopsUser = secret["AZURE_USER"]
    azdevopsPass = secret["AZURE_PASS"]
    az_uri = '#{SYSTEM_COLLECTIONURI}#'
    az_project = '#{SYSTEM_TEAMPROJECT}#'
    destroy_pipeline_name='#{TOOL_TF_PIPELINE_NAME}#'
    azDefUrl = az_uri + az_project +'/_apis/build/definitions'
    # Build definition
    dest_def_id = get_def_id(azdevopsUser,azdevopsPass,azDefUrl,destroy_pipeline_name)
    print("Dest_def_id is {}".format(dest_def_id))
    # CLOUDFUNCTION's BODY
    params = {
        ('api-version', '6.0-preview.1')
    }

    azdevopsURL = az_uri + az_project +'/_apis/pipelines/'+str(dest_def_id)+'/runs'
    data = '{"templateParameters": {"action": "destroy_composer"}, "variables": {}}'
    head = { 'Accept': 'application/json', 'Content-Type': 'application/json'}

    print("INFO - Calling Azure DevOps Release API....................")

    try:
        response = requests.post(azdevopsURL, headers=head, data=data, params=params, auth=(azdevopsUser, azdevopsPass))
        response.raise_for_status()
        print("INFO - Call finished succcessfully - ", response)
    except requests.exceptions.HTTPError as err:
        print("ERROR - Call Azure DevOps API finished with issues....", response)
        print("ERROR - Status Code : ", response.status_code)
        print("ERROR - Reason :", response.reason)