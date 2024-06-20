import json
import requests
import os
from framework.utils.retryable_session import RetryableSession
from framework.utils.singleton import SingletonMeta


class  IngestionClient(metaclass = SingletonMeta):
    def __init__(self,workflow_request,user_id):
        self._base_url = "https://ingestionv2-sandbox-http.internal.cleartax.co"
        self._org_id = workflow_request.orgId
        self._workspace_id = workflow_request.workspaceId
        self._user_id = user_id
        self._auth_token = workflow_request.authToken
        self._pan_node_id = workflow_request.panNodeId

        
    def get_presigned_url(self,ingest_file_path,file_type):
        session = RetryableSession()
        mode = 'purchase'
        tenant = 'MaxItc'
        if file_type == 'SALES':
            mode = 'sales'
            tenant = 'GSTSALES'
            
        
        file_name = os.path.basename(ingest_file_path)
        url = f"{self._base_url}/api/ingestion/{mode}/public/activity/v1/{tenant}/url/pre-sign?file_name={file_name}"
        headers = self.get_presign_headers()
        try:
            response = session.get(url,headers=headers,timeout=45)
            response.raise_for_status()
            response_data = response.json()
            return response_data
        except requests.exceptions.HTTPError as http_err:
            raise Exception(f"HTTP error occurred: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(f"Connection error occurred: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            raise Exception(f"Timeout error occurred: {timeout_err}")
        # except requests.exceptions.RequestException as req_err:
        #     raise Exception(f"An error occurred: {req_err}")
        
    def upload_to_s3(self,upload_url,file_path):
        session = RetryableSession()
        headers = {
        'Content-Type': 'application/vnd.ms-excel'
        }
        payload = open(file_path, 'rb')
        
        try:
            response = session.put(upload_url,headers=headers,data=payload,timeout=45)
            response.raise_for_status()
            if response.status_code != 200 and response.status_code != 201:
                print("s3 upload failure: ", response.text)
            return None
        except requests.exceptions.HTTPError as http_err:
            raise Exception(f"HTTP error occurred: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(f"Connection error occurred: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            raise Exception(f"Timeout error occurred: {timeout_err}")
        except requests.exceptions.RequestException as req_err:
            raise Exception(f"An error occurred: {req_err}")
        
    def create_activity(self, s3_file_url,user_file_name,file_type,return_period_start,return_period_end):
        session = RetryableSession()
        mode = 'purchase'
        tenant = 'MaxItc'
        if file_type == 'SALES':
            mode = 'sales'
            tenant = 'GSTSALES'
            
        
        url = f"{self._base_url}/api/ingestion/{mode}/public/activity/v1/{tenant}/create"
        headers = self.get_create_activity_headers()
        
        payload = self.get_create_activity_payload(s3_file_url,user_file_name,file_type,return_period_start,return_period_end)
        
        try:
            response = session.post(url,headers=headers,data=payload,timeout=45)
            response.raise_for_status()
            response_data = response.json()
            return response_data
        except requests.exceptions.HTTPError as http_err:
            raise Exception(f"HTTP error occurred: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(f"Connection error occurred: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            raise Exception(f"Timeout error occurred: {timeout_err}")
        except requests.exceptions.RequestException as req_err:
            raise Exception(f"An error occurred: {req_err}")
            
    def get_activity_status(self,activity_id):
        session = RetryableSession()
        url = f"{self._base_url}/api/ingestion/purchase/public/activity/v1/MaxItc/activity/{activity_id}"
        headers = self.get_activity_status_headers()
        try:
            response = session.get(url,headers=headers,timeout=45)
            response.raise_for_status()
            response_data = response.json()
            return response_data
        except requests.exceptions.HTTPError as http_err:
            raise Exception(f"HTTP error occurred: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(f"Connection error occurred: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            raise Exception(f"Timeout error occurred: {timeout_err}")
        except requests.exceptions.RequestException as req_err:
            raise Exception(f"An error occurred: {req_err}")
    
    def get_presign_headers(self):
          return {
        'accept': 'application/json',
        'fileContentType': "application/vnd.ms-excel",
        'x-cleartax-action': 'action',
        'x-cleartax-orgunit': self._org_id,
        'x-cleartax-user': self._user_id
    }
          
    
    def get_create_activity_headers(self):
        return {
            'accept': 'application/json',
            'x-cleartax-orgunit': self._org_id,
            'x-cleartax-user': self._user_id,
            'Content-Type': 'application/json',
            "x-workspace-id":self._workspace_id,
            "x-cleartax-product":"GST",
            "x-cleartax-auth-token": self._auth_token
         } 
        
    def get_create_activity_payload(self,s3_file_url,user_file_name,file_type,return_period_start,return_period_end):
        mode = 'purchase'
        tenant = 'MaxItc'
        template_type = 'PURCHASE'
        custom_template_id = '60e5613ff71f4a7aeca4336b'
        if file_type == 'SALES':
            mode = 'sales'
            tenant = 'GSTSALES'
            template_type = 'SALES'
            custom_template_id = '618a5623836651c01c1498ad'
            
        file_content_type = 'application/vnd.ms-excel'
        
        return json.dumps({
                "customTemplateId": custom_template_id,
                "fileInfo": {
                    "mediaType": file_content_type,
                    "s3FileUrl": s3_file_url,
                    "userFileName": user_file_name
                },
                "userInputArgs": {
                    "templateType": template_type,
                    "multiBranch": True,
                    "multiGSTIN": True,
                    "createCustomTemplate": False,
                    "returnPeriodEnd": return_period_start, 
                    "returnPeriodStart": return_period_end,
                    "sourceBusinessHierarchyNodeId": self._org_id,
                    "sourceBusinessHierarchyNodeType": 'ORGANISATION',
                    "tenant": tenant,
                    "metadata": {}
                },
                "dryRun": False,
                "ingestionChannel": "ERP"
                })         
            
    def get_activity_status_headers(self):
        return {
                'accept': 'application/json, text/plain, */*',
                'x-clear-node-id': self._org_id,
                'x-clear-node-type': 'ORGANISATION',
                'x-cleartax-orgunit': self._org_id,
                'x-cleartax-product': 'GST',
                'x-cleartax-source': 'APPCLEAR',
                'x-cleartax-tenant': 'MaxItc',
                'x-cleartax-user': self._user_id,
                'x-workspace-id': self._workspace_id,
                'x-cleartax-auth-token': self._auth_token
            }