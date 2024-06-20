from framework.utils.singleton import SingletonMeta

class WorkflowRequest(metaclass = SingletonMeta):
    def __init__(self,request_dict):
        self._request = request_dict
        
    @property    
    def orgId(self):
        return self._request.get("orgId")  
    
    @property
    def workspaceId(self):
        return self._request.get("workspaceId")
    
    @property
    def authToken(self):
        return self._request.get("authToken")
    
    @property
    def panNodeId(self):
        return self._request.get("panNodeId")
    
    @property
    def prIngestFilePath(self):
        return self._request.get("prIngestFilePath")
    
    @property
    def salesIngestFilePath(self):
        return self._request.get("salesIngestFilePath")
    
    @property    
    def returnPeriodStart(self):
        return self._request.get("returnPeriodStart")
    
    @property
    def returnPeriodEnd(self):
        return self._request.get("returnPeriodEnd") 
    
    @property        
    def matchingRequestType(self):
        return self._request.get("matchingRequestType")
    
    @property
    def erpInstanceId(self):
        return self._request.get("erpInstanceId")
