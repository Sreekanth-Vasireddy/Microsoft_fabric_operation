import json
import time
import struct
import sqlalchemy
import pyodbc
import pandas as pd
from pyspark.sql import functions as fn
from datetime import datetime
import sempy.fabric as fabric
from sempy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException

# Function to pad or truncate string
def pad_or_truncate_string(input_string, length, pad_char=' '):
    if len(input_string) > length:
        return input_string[:length]
    return input_string.ljust(length, pad_char)

# Function to display Fabric configuration
def display_fabric_config():
    tenant_id = spark.conf.get("trident.tenant.id")
    workspace_id = spark.conf.get("trident.workspace.id")
    lakehouse_id = spark.conf.get("trident.lakehouse.id")
    lakehouse_name = spark.conf.get("trident.lakehouse.name")
    
    print(f"Tenant ID: {tenant_id}")
    print(f"Workspace ID: {workspace_id}")
    print(f"Lakehouse ID: {lakehouse_id}")
    print(f"Lakehouse Name: {lakehouse_name}")
    return tenant_id, workspace_id, lakehouse_id, lakehouse_name
# Function to initialize Fabric client and get SQL endpoints
def get_sql_endpoints(workspace_id, lakehouse_id):
    client = fabric.FabricRestClient()
    sql_endpoints = client.get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}").json()['properties']['sqlEndpointProperties']
    print("SQL Endpoints Structure:", sql_endpoints)
    return client, sql_endpoints

# Function to refresh SQL endpoint
def refresh_sql_endpoint(client, sqlendpoint_id):
    uri = f"/v1.0/myorg/lhdatamarts/{sqlendpoint_id}"
    payload = {"commands":[{"$type":"MetadataRefreshExternalCommand"}]}
    
    # Call the REST API
    response = client.post(uri, json=payload)
    data = json.loads(response.text)
    batchId = data["batchId"]
    progressState = data["progressState"]
    statusuri = f"/v1.0/myorg/lhdatamarts/{sqlendpoint_id}/batches/{batchId}"
    
    output_messages = []
    
    while progressState == 'inProgress':
        time.sleep(1)
        statusresponsedata = client.get(statusuri).json()
        progressState = statusresponsedata["progressState"]
        message = f"Sync state for endpoint {sqlendpoint_id}: {progressState}"
        print(message)
        output_messages.append(message)
    
    if progressState == 'success':
        table_details = [
            {
                'tableName': table['tableName'],
                'warningMessages': table.get('warningMessages', []),
                'lastSuccessfulUpdate': table.get('lastSuccessfulUpdate', 'N/A'),
                'tableSyncState': table['tableSyncState'],
                'sqlSyncState': table['sqlSyncState']
            }
            for table in statusresponsedata['operationInformation'][0]['progressDetail']['tablesSyncStatus']
        ]
        output_messages.append("Extracted Table Details:")
        for detail in table_details:
            message = f"Table: {pad_or_truncate_string(detail['tableName'], 30)}   Last Update: {detail['lastSuccessfulUpdate']}  tableSyncState: {detail['tableSyncState']}   Warnings: {detail['warningMessages']}"
            print(message)
            output_messages.append(message)
    
    if progressState == 'failure':
        error_message = f"Error syncing endpoint {sqlendpoint_id}:"
        print(error_message)
        output_messages.append(error_message)
        output_messages.append(statusresponsedata)
    
    return output_messages

# Main function to execute the script
def refresh_lakehouse():
    tenant_id, workspace_id, lakehouse_id, lakehouse_name = display_fabric_config()
    client, sql_endpoints = get_sql_endpoints(workspace_id, lakehouse_id)
    sqlendpoint_id = sql_endpoints['id']
    output_messages = refresh_sql_endpoint(client, sqlendpoint_id)
    
    # Serialize the output messages to JSON and send back to the pipeline
    output_json = json.dumps({"messages": output_messages})
    notebook.exit(output_json)

# Execute the main function
refresh_lakehouse()
