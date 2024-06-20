import json
import requests
from framework.utils.retryable_session import RetryableSession
from framework.utils.singleton import SingletonMeta

class APIReconWorkflowRequest:
    def __init__(self,request_dict):
        self.api_request = request_dict
        
    @property        
    def matchingRequestType(self):
        return self.api_request.matchingRequestType
    
    @property
    def panNodeId(self):
        return self.api_request.panNodeId
    
    @property
    def pullReturnPeriodStart(self):
        return self.api_request.pullReturnPeriodStart
    
    @property
    def pullReturnPeriodEnd(self):
        return self.api_request.pullReturnPeriodEnd
    
    @property
    def reconReturnPeriodStart(self):
        return self.api_request.reconReturnPeriodStart
    
    @property
    def reconReturnPeriodEnd(self):
        return self.api_request.reconReturnPeriodEnd
    
    @property
    def workspaceId(self):
        return self.api_request.workspaceId 
    
    @property
    def disableDataPosting(self):
        return False  
    

class CustomJSONEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, APIReconWorkflowRequest):
                # Serialize obj into a JSON-serializable format
                return obj.to_json()  # Example method to convert object to JSON
            return super().default(obj)     
    
    
class ReconIntegrationClient(metaclass=SingletonMeta):
    def __init__(self,workflow_request):
        self._base_url = "https://recon-integration-sandbox-http-server.internal.cleartax.co"
        self._headers = {
            'Content-Type': 'application/json',
            'x-clear-auth-token': workflow_request.authToken,
            'x-erp-instance-id': workflow_request.erpInstanceId
        }
        
    def trigger_recon_workflow(self,request):
        session = RetryableSession()
        url = f"{self._base_url}/api/recon-integration/internal/workflow/recon"
        payload = json.dumps(request)
        
        try:
            response = session.post(url,headers=self._headers,data=payload,timeout=60)
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

            
        
       
        
    def get_workflow_current_status(self,async_task_id):
        session = RetryableSession()
        url = f"{self._base_url}/api/recon-integration/internal/workflow/status?asyncTaskId={async_task_id}"
        
        try:
            response = session.get(url,headers=self._headers,timeout=45)
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
        
            
            
            
            