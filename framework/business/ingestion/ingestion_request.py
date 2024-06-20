from framework.utils.singleton import SingletonMeta

class IngestionRequest(metaclass = SingletonMeta):
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
    def prReturnPeriodStart(self):
        return self._request.get("prReturnPeriodStart")
    @property
    def prReturnPeriodEnd(self):
        return self._request.get("prReturnPeriodEnd")
    @property
    def salesReturnPeriodStart(self):
        return self._request.get("salesReturnPeriodStart")
    @property
    def salesReturnPeriodEnd(self):
        return self._request.get("salesReturnPeriodEnd")