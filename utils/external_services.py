import requests

def get_one_integration_erp_config(host, workspace_id, erp_instance_id):
    url = f"{host}/api/one-integration/internal/erp_query_config/"

    payload = {}
    headers = {
        'accept': '*/*',
        'x-workspace-id': workspace_id,
        'x-erp-instance-id': erp_instance_id,
        'x-user-id': 'DBT'
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    # print(response.json())
    return response.json()["result"]


def get_advance_ingestion_config(host, workspace_id, erp_type = "SAP"):
    url = f"{host}/api/advance/ingestion/internal/getErpConfig?erpType={erp_type}&workspaceId={workspace_id}"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    return response.json()

def get_erp_config_schema(request):
    if request.get("caller") == "ONE_INTEGRATION":
        return get_one_integration_erp_config(request.get("caller_host"), request["workspace_id"], request["erp_instance_id"])
    if request.get("caller") == "ADVANCE_INGESTION":
        return get_advance_ingestion_config(request.get("caller_host"), request["workspace_id"])
